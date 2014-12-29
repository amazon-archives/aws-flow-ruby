#--
# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#++

module AWS
  module Flow



    # A generic Activity/Workflow worker class.
    class GenericWorker
      # Creates a new `GenericWorker`.
      # @param service
      #   The AWS service class to use.
      #
      # @param domain
      #   The Amazon SWF domain to use.
      #
      # @param task_list_to_poll
      #   The list of tasks to poll for this worker.
      #
      # @param args
      #   Arguments for the workflow worker.
      #
      # @param [WorkerOptions] block
      #   A set of {WorkerOptions} for the worker.
      #
      def initialize(service, domain, task_list_to_poll, *args, &block)
        @service = service
        @domain = domain
        @task_list = task_list_to_poll
        if args
          args.each { |klass_or_instance| add_implementation(klass_or_instance) }
        end
        @shutting_down = false
        %w{ TERM INT }.each do |signal|
          Signal.trap(signal) do
            if @shutting_down
              @executor.shutdown(0) if @executor
              Kernel.exit! 1
            else
              @shutting_down = true
              @shutdown_first_time_function.call if @shutdown_first_time_function
            end
          end
        end
      end

      # @api private
      def camel_case_to_snake_case(camel_case)
        camel_case.
          gsub(/(.)([A-Z])/,'\1_\2').
          downcase
      end

      def resolve_default_task_list(name)
        name == FlowConstants.use_worker_task_list ? @task_list : name
      end

    end

    module GenericTypeModule
      def hash
        [@name.to_sym, @version].hash
      end
      def eql?(other)
        @name.to_sym == other.name.to_sym && @version == other.version
      end
    end

    class GenericType
      include GenericTypeModule
      attr_accessor :name, :version, :options
      def initialize(name, version, options = {})
        @name = name
        @version = version
        @options = options
      end
    end

    [:ActivityType, :WorkflowType].each do |type|
      klass =  AWS::SimpleWorkflow.const_get(type)
      klass.class_eval { include GenericTypeModule }
    end



    # Represents a workflow type.
    class WorkflowType < GenericType; end


    # Represents an activity type.
    class ActivityType < GenericType; end

    # This worker class is intended for use by the workflow implementation. It
    # is configured with a task list and a workflow implementation. The worker
    # class polls for decision tasks in the specified task list. When a decision
    # task is received, it creates an instance of the workflow implementation
    # and calls the @ execute() decorated method to process the task.
    class WorkflowWorker < GenericWorker

      # The workflow type for this workflow worker.
      attr_accessor :workflow_type

      # Creates a new WorkflowWorker instance.
      #
      # @param service
      #   The service used with this workflow worker.
      #
      # @param [String] domain
      #   The Amazon SWF domain to operate on.
      #
      # @param [Array] task_list
      #   The default task list to put all of the decision requests.
      #
      # @param args
      #   The decisions to use.
      #
      def initialize(service, domain, task_list, *args, &block)
        @workflow_definition_map = {}
        @workflow_type_options = []
        @options = Utilities::interpret_block_for_options(WorkerOptions, block)

        @logger = @options.logger if @options
        @logger ||= Utilities::LogFactory.make_logger(self)
        @options.logger ||= @logger if @options

        super(service, domain, task_list, *args)
      end

      def set_workflow_implementation_types(workflow_implementation_types)
        workflow_implementation_types.each {|type| add_workflow_implementation(type)}
      end

      def add_implementation(workflow_class)
        add_workflow_implementation(workflow_class)
      end

      # Called by {#add_implementation}.
      # @api private
      def add_workflow_implementation(workflow_class)
        workflow_class.workflows.delete_if do |workflow_type|
          workflow_type.version.nil? || workflow_type.name.nil?
        end

        @workflow_definition_map.merge!(
          WorkflowDefinitionFactory.generate_definition_map(workflow_class)
        )

        workflow_class.workflows.each do |workflow_type|
          # TODO should probably do something like
          # GenericWorkflowWorker#registerWorkflowTypes
          options = workflow_type.options
          workflow_hash = options.get_options(
            [
              :default_task_start_to_close_timeout,
              :default_execution_start_to_close_timeout,
              :default_child_policy,
              :default_task_priority
            ], {
              :domain => @domain.name,
              :name => workflow_type.name,
              :version => workflow_type.version
            }
          )

          if options.default_task_list
            workflow_hash.merge!(
              :default_task_list => {:name => resolve_default_task_list(options.default_task_list)}
            )
          end
          @workflow_type_options << workflow_hash
        end
      end


      # Registers this workflow with Amazon SWF.
      def register
        @workflow_type_options.delete_if {|workflow_type_options| workflow_type_options[:version].nil?}
        @workflow_type_options.each do |workflow_type_options|
          begin
            @service.register_workflow_type(workflow_type_options)
          rescue AWS::SimpleWorkflow::Errors::TypeAlreadyExistsFault => e
            @logger.warn "#{e.class} while trying to register workflow #{e.message} with options #{workflow_type_options}"
            # Purposefully eaten up, the alternative is to check first, and who
            # wants to do two trips when one will do?
          end
        end
      end


      # Starts the workflow with a {WorkflowTaskPoller}.
      #
      # @param [true,false] should_register
      #   Indicates whether the workflow needs to be registered with Amazon SWF
      #   first. If {#register} was already called
      #   for this workflow worker, specify `false`.
      #
      def start(should_register = true)
        # TODO check to make sure that the correct properties are set
        # TODO Register the domain if not already registered
        # TODO register types to poll
        # TODO Set up throttler
        # TODO Set up a timeout on the throttler correctly,
        # TODO Make this a generic poller, go to the right kind correctly

        poller = WorkflowTaskPoller.new(
          @service,
          @domain,
          DecisionTaskHandler.new(@workflow_definition_map, @options),
          @task_list,
          @options
        )

        register if should_register
        @logger.debug "Starting an infinite loop to poll and process workflow tasks."
        loop do
          run_once(false, poller)
        end
      end

      # Starts the workflow and runs it once, with an optional
      # {WorkflowTaskPoller}.
      #
      # @param should_register (see #start)
      #
      # @param poller
      #   An optional {WorkflowTaskPoller} to use.
      #
      def run_once(should_register = false, poller = nil)
        register if should_register

        poller = WorkflowTaskPoller.new(
          @service,
          @domain,
          DecisionTaskHandler.new(@workflow_definition_map, @options),
          @task_list,
          @options
        ) if poller.nil?

        Kernel.exit if @shutting_down
        poller.poll_and_process_single_task
      end
    end


    # Used to implement an activity worker. You can use the `ActivityWorker`
    # class to conveniently poll a task list for activity tasks.
    #
    # You configure the activity worker with activity implementation objects.
    # This worker class then polls for activity tasks in the specified task
    # list. When an activity task is received, it looks up the appropriate
    # implementation that you provided, and calls the activity method to
    # process the task. Unlike the {WorkflowWorker}, which creates a new
    # instance for every decision task, the `ActivityWorker` simply uses the
    # object you provided.
    class ActivityWorker < GenericWorker

      # Creates a new `ActivityWorker` instance.
      #
      # @param service
      #   The Amazon SWF [Client](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Client.html) used to register
      #   this activity worker.
      #
      # @param [String] domain
      #   The Amazon SWF [Domain](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Domain.html) to operate on.
      #
      # @param [Array] task_list
      #   The default task list to put all of the activity requests.
      #
      # @param args
      #   The activities to use.
      #
      def initialize(service, domain, task_list, *args, &block)
        @activity_definition_map = {}
        @activity_type_options = []
        @options = Utilities::interpret_block_for_options(WorkerOptions, block)

        @logger = @options.logger if @options
        @logger ||= Utilities::LogFactory.make_logger(self)
        @options.logger ||= @logger if @options

        max_workers = @options.execution_workers if @options
        max_workers = 20 if (max_workers.nil? || max_workers.zero?)
        @executor = ForkingExecutor.new(
          :max_workers => max_workers,
          :logger => @logger
        )
        @shutdown_first_time_function = lambda do
          @executor.shutdown Float::INFINITY
          Kernel.exit
        end
        super(service, domain, task_list, *args)
      end

      # Adds an activity implementation to this `ActivityWorker`.
      #
      # @param [Activity] class_or_instance
      #   The {Activity} class or instance to add.
      #
      def add_implementation(class_or_instance)
        add_activities_implementation(class_or_instance)
      end


      # Registers the activity type.
      def register
        @activity_type_options.each do |activity_type_options|
          begin
            @service.register_activity_type(activity_type_options)
          rescue AWS::SimpleWorkflow::Errors::TypeAlreadyExistsFault => e
            @logger.warn "#{e.class} while trying to register activity #{e.message} with options #{activity_type_options}"
            previous_registration = @service.describe_activity_type(
              :domain => @domain.name,
              :activity_type => {
                :name => activity_type_options[:name],
                :version => activity_type_options[:version]
              }
            )
            default_options = activity_type_options.select { |key, val| key =~ /default/}
            previous_keys = previous_registration["configuration"].keys.map {|x| camel_case_to_snake_case(x).to_sym}

            previous_registration = Hash[previous_keys.zip(previous_registration["configuration"].values)]
            if previous_registration[:default_task_list]
              previous_registration[:default_task_list][:name] = previous_registration[:default_task_list].delete("name")
            end
            registration_difference =  default_options.sort.to_a - previous_registration.sort.to_a

            unless registration_difference.empty?
              raise "Activity [#{activity_type_options[:name]}]: There is a difference between the types you have registered previously and the types you are currently registering, but you haven't changed the version. These new changes will not be picked up. In particular, these options are different #{Hash[registration_difference]}"
            end
            # Purposefully eaten up, the alternative is to check first, and who
            # wants to do two trips when one will do?
          end
        end
      end

      # Adds an activity implementation to this `ActivityWorker`.
      #
      # @param [Activity] class_or_instance
      #   The {Activity} class or instance to add.
      #
      def add_activities_implementation(class_or_instance)
        klass = (class_or_instance.class == Class) ? class_or_instance : class_or_instance.class
        instance = (class_or_instance.class == Class) ? class_or_instance.new : class_or_instance
        klass.activities.each do |activity_type|

          # TODO this should assign to an activityImplementation, so that we can
          # call execute on it later
          @activity_definition_map[activity_type] = ActivityDefinition.new(
            instance,
            activity_type.name.split(".").last,
            nil,
            activity_type.options,
            activity_type.options.data_converter
          )
          options = activity_type.options
          option_hash = {
            :domain => @domain.name,
            :name => activity_type.name.to_s,
            :version => activity_type.version
          }

          option_hash.merge!(options.get_registration_options)

          if options.default_task_list
            option_hash.merge!(
              :default_task_list => {:name => resolve_default_task_list(options.default_task_list)}
            )
          end

          @activity_type_options << option_hash
        end
      end


      # Starts the activity that was added to the `ActivityWorker`.
      #
      # @param [true, false] should_register
      #   Set to `false` if the activity should not register itself (it is
      #   already registered).
      #
      def start(should_register = true)

        register if should_register
        poller = ActivityTaskPoller.new(
          @service,
          @domain,
          @task_list,
          @activity_definition_map,
          @executor,
          @options
        )

        @logger.debug "Starting an infinite loop to poll and process activity tasks."
        loop do
          run_once(false, poller)
        end
      end

      # Starts the activity that was added to the `ActivityWorker` and,
      # optionally, sets the {ActivityTaskPoller}.
      #
      # @param [true, false] should_register
      #   Set to `false` if the activity should not register itself (it is
      #   already registered).
      #
      # @param [ActivityTaskPoller] poller
      #   The {ActivityTaskPoller} to use. If this is not set, a default
      #   {ActivityTaskPoller} will be created.
      #
      def run_once(should_register = true, poller = nil)
        register if should_register
        poller = ActivityTaskPoller.new(
          @service,
          @domain,
          @task_list,
          @activity_definition_map,
          @executor,
          @options
        ) if poller.nil?

        Kernel.exit if @shutting_down
        poller.poll_and_process_single_task(@options.use_forking)
      end
    end

  end
end

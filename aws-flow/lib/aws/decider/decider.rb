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

    # A MethodPair groups a method name with an associated data converter.
    class MethodPair
      # The method name for the method pair.
      attr_accessor :method_name

      # The data converter for the method pair.
      attr_accessor :data_converter

      # Creates a new MethodPair instance.
      #
      # @param method_name
      #   The method name for the method pair.
      #
      # @param data_converter
      #   The data converter for the method pair.
      #
      def initialize(method_name, data_converter)
        @method_name = method_name
        @data_converter = data_converter
      end
    end

    # A generic workflow client implementation.
    class GenericWorkflowClient
      include Utilities::SelfMethods
      def completion_function(event, open_request)
        open_request.result = event.attributes[:result]
        open_request.completion_handle.complete
      end


      # The data converter for the generic workflow client.
      attr_accessor :data_converter

      # Creates a new generic workflow client.
      #
      # @param decision_helper
      #
      # @param workflow_context
      #
      def initialize(decision_helper, workflow_context)
        @decision_helper = decision_helper
        @workflow_context = workflow_context
      end

      # Handler for the ExternalWorkflowExecutionCancelRequested event.
      #
      # @param [Object] event The event instance.
      #
      def handle_external_workflow_execution_cancel_requested(event)
        # NOOP
      end

      # Handler for the ChildWorkflowExecutionCanceled event.
      #
      # @param [Object] event The event instance.
      #
      def handle_child_workflow_execution_canceled(event)
        handle_event(event,
                     {
                       :id_methods => [:workflow_execution, :workflow_id],
                       :consume_symbol => :handle_cancellation_event,
                       :decision_helper_scheduled => :scheduled_external_workflows,
                       :handle_open_request => lambda do |event, open_request|
                         cancellation_exception = CancellationException.new("Cancelled from a ChildWorkflowExecutionCancelled")
                         open_request.completion_handle.fail(cancellation_exception)
                       end
                     })
      end


      # Handler for the ChildWorkflowExecutionCompleted event.
      #
      # @param [Object] event The event instance.
      #

      def handle_child_workflow_execution_completed(event)
        handle_event(event,
                     {:id_methods => [:workflow_execution, :workflow_id],
                       :consume_symbol => :handle_completion_event,
                       :decision_helper_scheduled => :scheduled_external_workflows,
                       :handle_open_request => method(:completion_function)
                     })
      end

      # Handler for the ChildWorkflowExecutionFailed event.
      #
      # @param [Object] event The event instance.
      #
      def handle_child_workflow_execution_failed(event)
        handle_event(event,
                     {:id_methods => [:workflow_execution, :workflow_id],
                       :consume_symbol => :handle_completion_event,
                       :decision_helper_scheduled => :scheduled_external_workflows,
                       :handle_open_request => lambda do |event, open_request|
                         reason = event.attributes.reason
                         details = event.attributes.details
                         # workflow_id = @decision_helper.child_initiated_event_id_to_workflow_id[event.attributes.initiated_event_id]
                         # @decision_helper.scheduled_external_workflows[workflow_id]
                         failure = ChildWorkflowFailedException.new(event.id, event.attributes[:workflow_execution], event.attributes.workflow_type, reason, details )
                         open_request.completion_handle.fail(failure)
                       end
                     }
                     )
      end


      # Handler for the ChildWorkflowExecutionStarted event.
      #
      # @param [Object] event The event instance.
      #
      def handle_child_workflow_execution_started(event)
        handle_event(event,
                     {:id_methods => [:workflow_execution, :workflow_id],
                       :consume_symbol => :handle_started_event,
                       :decision_helper_scheduled => :scheduled_external_workflows,
                       :handle_open_request => lambda do |event, open_request|
                         open_request.run_id.set(event.attributes.workflow_execution.run_id)
                       end
                     })
      end

      # Handler for the ChildWorkflowExecutionTerminated event.
      #
      # @param [Object] event The event instance.
      #
      def handle_child_workflow_execution_terminated(event)
        handle_event(event,
                     {:id_methods => [:workflow_execution, :workflow_id],
                       :consume_symbol => :handle_completion_event,
                       :decision_helper_scheduled => :scheduled_external_workflows,
                       :handle_open_request => lambda do |event, open_request|
                         exception = ChildWorkflowTerminatedException.new(event.id, open_request.description, nil)
                         open_request.completion_handle.fail(exception)
                       end
                     })
      end

      # Handler for the ChildWorkflowExecutionTimedOut event.
      #
      # @param [Object] event The event instance.
      #
      def handle_child_workflow_execution_timed_out(event)
        handle_event(event,
                     {:id_methods => [:workflow_execution, :workflow_id],
                       :consume_symbol => :handle_completion_event,
                       :decision_helper_scheduled => :scheduled_external_workflows,
                       :handle_open_request => lambda do |event, open_request|
                         exception = ChildWorkflowTimedOutException.new(event.id, open_request.description, nil)
                         open_request.completion_handle.fail(exception)
                       end
                     })
      end

      # Handler for the ExternalWorkflowExecutionSignaled event.
      #
      # @param [Object] event The event instance.
      #
      def handle_external_workflow_execution_signaled(event)
        signal_id = @decision_helper.signal_initiated_event_to_signal_id[event.attributes[:initiated_event_id]]
        state_machine = @decision_helper[signal_id]
        state_machine.consume(:handle_completion_event)
        if state_machine.done?
          open_request = @decision_helper.scheduled_signals.delete(signal_id)
          open_request.result = nil
          open_request.completion_handle.complete
        end
      end

      # Handler for the SignalExternalWorkflowExecutionFailed event.
      #
      # @param [Object] event The event instance.
      #
      def handle_signal_external_workflow_execution_failed(event)
        handle_event(event, {
                       :id_methods => [:control],
                       :consume_symbol => :handle_completion_event,
                       :decision_helper_scheduled => :scheduled_signals,
                       :handle_open_request => lambda do |event, open_request|
                         workflow_execution = AWS::SimpleWorkflow::WorkflowExecution.new("",event.attributes.workflow_id, event.attributes.run_id)
                         failure = SignalExternalWorkflowException(event.id, workflow_execution, event.attributes.cause)
                         open_request.completion_handle.fail(failure)
                       end
                     })
      end

      # Handler for the StartExternalWorkflowExecutionFailed event.
      #
      # @param [Object] event The event instance.
      #
      def handle_start_child_workflow_execution_failed(event)
        handle_event(event, {
                     :id_methods => [:workflow_id],
                     :consume_symbol => :handle_initiation_failed_event,
                     :decision_helper_scheduled => :scheduled_external_workflows,
                     :handle_open_request => lambda do |event, open_request|
                       workflow_execution = AWS::SimpleWorkflow::WorkflowExecution.new("",event.attributes.workflow_id, event.attributes.run_id)
                       workflow_type = event.attributes.workflow_type
                       cause = event.attributes.cause
                       failure = StartChildWorkflowFailedException.new(event.id, workflow_execution, workflow_type, cause)
                       open_request.completion_handle.fail(failure)
                     end
                     })
      end
    end


    # Types and methods related to workflow execution. Extend this to implement a workflow decider.
    #
    # @!attribute version
    #   Sets or returns the Decider version.
    #
    # @!attribute options
    #   Sets or returns the {WorkflowOptions} for this decider.
    #
    module Workflows
      attr_accessor :version, :options
      extend Utilities::UpwardLookups
      @precursors = []
      def look_upwards(variable)
        precursors = self.ancestors.dup
        precursors.delete(self)
        results = precursors.map { |x| x.send(variable) if x.methods.map(&:to_sym).include? variable }.compact.flatten.uniq
      end
      property(:workflows, [])
      @workflows = []
      def self.extended(base)
        base.send :include, InstanceMethods
      end

      #  This method is for internal use only and may be changed or removed
      #   without prior notice.  Use {#workflows} instead. Set the entry point
      #   in the {#workflow} method when creating a new workflow.
      # @!visibility private
      def entry_point(input=nil)
        if input
          @entry_point = input
          workflow_type = WorkflowType.new(self.to_s + "." + input.to_s, nil, WorkflowOptions.new(:execution_method => input))
          self.workflows.each { |workflow| workflow.name = self.to_s + "." + input.to_s }
          self.workflows.each do |workflow|
            workflow.options = WorkflowOptions.new(:execution_method => input)
          end
          self.workflows = self.workflows << workflow_type
        end
        return @entry_point if @entry_point
        raise "You must set an entry point on the workflow definition"
      end

      #  This method is for internal use only and may be changed or removed
      # without prior notice.  Use {#workflows} instead.
      # Set the version in the {WorkflowOptions} passed in to the {#workflow} method.
      # @!visibility private
      def version(arg = nil)
        if arg
          self.workflows.each { |workflow| workflow.version = arg }
          self.workflows = self.workflows << WorkflowType.new(nil, arg, WorkflowOptions.new)
        end
        return @version
      end

      # Sets the activity client.
      #
      # @param name
      #   Sets the client name for the activity client.
      #
      # @param block
      #   A block of {ActivityOptions} for the activity client.
      #
      def activity_client(name, &block)
        options = Utilities::interpret_block_for_options(ActivityOptions, block)
        # TODO: Make sure this works for dynamic stuff
        begin
          activity_class = get_const(options.prefix_name)
        rescue Exception => e
          #pass
        end
        activity_options = {}
        if activity_class
          values = activity_class.activities.map{|x| [x.name.split(".").last.to_sym, x.options]}
          activity_options = Hash[*values.flatten]
        end
        #   define_method(name) do
        #     return @client if @client
        #     @client ||= activity_class.activity_client.new(@decision_helper, options)
        #     @client.decision_context = @decision_context
        #     @client
        #   end
        # else
        client_name = "@client_#{name}"

        define_method(name) do
          return instance_variable_get(client_name) if instance_variable_get(client_name)
          @decision_context ||= Fiber.current[:decision_context]
          @decision_helper ||= @decision_context.decision_helper
          @decision_helper.activity_options = activity_options
          instance_variable_set(client_name, GenericActivityClient.new(@decision_helper, options))
          instance_variable_get(client_name)
        end
        instance_variable_get(client_name)
      end


      # @!visibility private
      def _options; self.workflows.map(&:options); end

      # Defines a new workflow
      #
      # @param entry_point
      #   The entry point (method) that starts the workflow.
      #
      # @param block
      #   A block of {WorkflowOptions} for the workflow.
      #
      def workflow(entry_point, &block)
        options = Utilities::interpret_block_for_options(WorkflowOptionsWithDefaults, block)
        options.execution_method = entry_point
        workflow_name = options.prefix_name || self.to_s
        workflow_type = WorkflowType.new(workflow_name.to_s + "." + entry_point.to_s, options.version, options)
        self.workflows = self.workflows << workflow_type
      end

      # @return [MethodPair]
      #   A {MethodPair} object
      #
      def get_state_method(get_state_method = nil, options = {})
        data_converter = options[:data_converter]
        @get_state_method = MethodPair.new(get_state_method, data_converter) unless get_state_method.nil?
        @get_state_method
      end

      # Defines a signal for the workflow.
      #
      # @param method_name
      #   The signal method for the workflow.
      #
      # @param [SignalWorkflowOptions] options
      #   The {SignalWorkflowOptions} for this signal.
      #
      def signal(method_name , options = {})
        data_converter = options[:data_converter]
        signal_name = options[:signal_name]
        signal_name ||= method_name.to_s
        data_converter ||= FlowConstants.default_data_converter
        @signals ||= {}
        @signals[signal_name] = MethodPair.new(method_name, data_converter)
        @signals
      end


      # @return [Hash]
      #   A hash of string(SignalName) => MethodPair(method, signalConverter) objects
      def signals
        @signals
      end


      # Instance methods for {DecisionContext}
      module InstanceMethods

        # Returns the {DecisionContext} instance.
        # @return [DecisionContext]
        #   The {DecisionContext} instance.
        def decision_context
          FlowFiber.current[:decision_context]
        end


        # Returns the workflow ID.
        #
        # @return
        #   The workflow ID
        #
        def workflow_id
          self.decision_context.workflow_context.decision_task.workflow_execution.workflow_id
        end

        # Returns the decision helper for the decision context. This should be an instance of {DecisionHelper} or a
        # class derived from it.
        def run_id
          self.decision_context.workflow_context.decision_task.workflow_execution.run_id
        end

        def decision_helper
          FlowFiber.current[:decision_context].decision_helper
        end


        # Sets the activity client for this decision context.
        #
        # @param name
        #   The name of the activity client.
        #
        # @param block
        #   A block of {ActivityOptions} for the activity client.
        #
        def activity_client(name=nil, &block)
          options = Utilities::interpret_block_for_options(ActivityOptions, block)
          begin
            activity_class = get_const(options.prefix_name)
          rescue Exception => e
            #pass
          end
          activity_options = {}
          if activity_class
            values = activity_class.activities.map{|x| [x.name.split(".").last.to_sym, x.options]}
            activity_options = Hash[*values.flatten]
          end
          client = GenericActivityClient.new(self.decision_helper, options)
          self.class.send(:define_method, name) { client }  if ! name.nil?
          client
        end


        # Creates a timer on the workflow that executes the supplied block after a specified delay.
        #
        # @param delay_seconds
        #   The number of seconds to delay before executing the block.
        #
        # @param block
        #   The block to execute when the timer expires.
        #
        def create_timer(delay_seconds, &block)
          self.decision_context.workflow_clock.create_timer(delay_seconds, block)
        end

        # Creates an asynchronous timer on the workflow that executes the supplied block after a specified delay.
        #
        # @param (see #create_timer)
        #
        # @deprecated
        #   Use {#create_timer_async} instead.
        #
        # @!visibility private
        def async_create_timer(delay_seconds, &block)
          task { self.decision_context.workflow_clock.create_timer(delay_seconds, block) }
        end


        # Creates an asynchronous timer on the workflow that executes the supplied block after a specified delay.
        #
        # @param (see #create_timer)
        #
        def create_timer_async(delay_seconds, &block)
          task { self.decision_context.workflow_clock.create_timer(delay_seconds, block) }
        end


        # Restarts the workflow as a new workflow execution.
        #
        # @param args
        #   Arguments for this workflow execution, in JSON format.
        #
        # @param [ContinueAsNewOptions] block
        #   The {ContinueAsNewOptions} for this workflow execution.
        #
        def continue_as_new(*args, &block)
          continue_as_new_options = Utilities::interpret_block_for_options(ContinueAsNewOptions, block)
          @data_converter ||= YAMLDataConverter.new
          if ! args.empty?
            input = @data_converter.dump args
            continue_as_new_options.input = input
          end
          known_workflows = self.class.workflows
          # If there is only one workflow, we can unambiguously say that we should use that one

          if known_workflows.length == 1
            continue_as_new_options.precursors << known_workflows.first.options
          end
          # If we can find a name that matches, use that one
          if continue_as_new_options.execution_method
            matching_option = self.class.workflows.map(&:options).find {|x| x.execution_method == continue_as_new_options.execution_method }
            continue_as_new_options.precursors << matching_option unless matching_option.nil?
          end
          self.decision_context.workflow_context.continue_as_new_options = continue_as_new_options
        end
      end
    end

    # This method is for internal use only and may be changed or removed
    # without prior notice.
    #  Use {Workflows} instead.
    #
    # @!visibility private
    module Decider
      include Workflows

      def self.extended(base)
        base.send :include, InstanceMethods
      end
    end

  end
end

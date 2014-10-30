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

    # Contains data about an Amazon SWF domain.
    #
    # @!attribute name
    #   The domain name.
    #
    class MinimalDomain
      attr_accessor :name

      # Creates a new `MinimalDomain` instance.
      #
      # @param domain
      #   The [AWS::SimpleWorkflow::Domain](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Domain.html)
      #   that this instance will refer to.
      #
      def initialize(domain); @domain = domain; end
    end

    # Contains data about a workflow execution. This class represents a minimal set of data needed by the AWS Flow
    # Framework for Ruby, based on the
    # [AWS::SimpleWorkflow::WorkflowExecution](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/WorkflowExecution.html)
    # class.
    #
    # @!attribute domain
    #   The domain that the workflow is running in. See {MinimalWorkflowExecution#initialize} for details.
    #
    # @!attribute run_id
    #   The unique run indentifier for this workflow. See {MinimalWorkflowExecution#initialize} for details.
    #
    # @!attribute workflow_id
    #   The unique identifier for this workflow. See {MinimalWorkflowExecution#initialize} for details.
    #
    class MinimalWorkflowExecution
      attr_accessor :workflow_id, :run_id, :domain

      # Creates a new `MinimalWorkflowExecution` instance
      #
      # @param (Domain) domain
      #   The Amazon SWF domain that the workflow is running in; an instance of
      #   [AWS::SimpleWorkflow::Domain](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Domain.html).
      #
      # @param (String) workflow_id
      #   The unique workflow indentifier for this workflow; From
      #   [AWS::SimpleWorkflow::WorkflowExecution#workflow_id](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/WorkflowExecution.html#workflow_id-instance_method).
      #
      # @param (String) run_id
      #   The unique run indentifier for this workflow. From
      #   [AWS::SimpleWorkflow::WorkflowExecution#run_id](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/WorkflowExecution.html#run_id-instance_method).
      #
      def initialize(domain, workflow_id, run_id)
        @domain = domain
        @workflow_id = workflow_id
        @run_id = run_id
      end
    end

    # A future provided by a
    # [AWS::SimpleWorkflow::WorkflowExecution](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/WorkflowExecution.html).
    #
    # @!attribute _workflow_execution
    #   A {MinimalWorkflowExecution} instance that this future belongs to.
    #
    # @!attribute return_value
    #   The return value of the future.
    #
    class WorkflowFuture
      attr_accessor :_workflow_execution, :return_value


      # Creates a new workflow future.
      #
      # @param workflow_execution
      #   The {MinimalWorkflowExecution} to assign to this future.
      #
      def initialize(workflow_execution)
        @_workflow_execution = workflow_execution.dup
        @return_value = Future.new
      end

      # Determines whether the object is a flow future. The contract is that flow futures must have a `get` method.
      #
      # @return
      #   Always returns `true` for a {WorkflowFuture} object.
      def is_flow_future?
        true
      end

      # @api private
      def method_missing(method_name, *args, &block)
        @return_value.send(method_name, *args, &block)
      end

      # Gets the current value of the workflow execution.
      #
      # @return {MinimalWorkflowExecution}
      #   The workflow execution that this future belongs to.
      #
      def workflow_execution
        @_workflow_execution
      end
    end




    # @api private
    class NoInput
      def empty?; return true; end
    end


    # Represents a client for a workflow execution.
    #
    # @!attribute domain
    #   The Amazon SWF domain used for this workflow.
    #
    # @!attribute [Hash, WorkflowOptions] options
    #   Workflow options for this client.
    #
    class WorkflowClient < GenericClient
      attr_accessor :domain, :options

      # Creates a new {WorkflowClient}.
      #
      # @param service
      #   The Amazon SWF [Client](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Client.html) to use for
      #   creating this {WorkflowClient}.
      #
      # @param domain
      #   The Amazon SWF [Domain](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Domain.html) in which to
      #   start the workflow execution.
      #
      # @param workflow_class
      #
      # @param [Hash, WorkflowOptions] options
      #   Workflow options for this client.
      #
      def initialize(service, domain, workflow_class, options)
        @service = service
        @domain = domain
        @workflow_class = workflow_class
        @options = options
        @failure_map = {}
        super
      end


      # @api private
      def self.default_option_class; WorkflowOptions; end

      # Gets the events for this workflow client.
      #
      def events
        @execution.events if @execution
      end


      # Gets the current {DecisionContext} for the workflow client.
      #
      def get_decision_context
        @decision_context ||= FlowFiber.current[:decision_context]
        @decision_helper ||= @decision_context.decision_helper
      end


      # Begins executing this workflow.
      #
      # @param input
      #   Input to provide to the workflow.
      #
      # @param [Hash, StartWorkflowOptions] block
      #   A hash of {StartWorkflowOptions} to use for this workflow execution.
      #
      def start_execution(*input, &block)
        start_execution_method(nil, *input, &block)
      end

      def start_execution_method(method_name, *input, &block)
        if Utilities::is_external
          self.start_external_workflow(method_name, input, &block)
        else
          self.start_internal_workflow(method_name, input, &block)
        end
      end

      # Records a `WorkflowExecutionSignaled` event in the workflow execution history and creates a decision task for
      # the workflow execution. The event is recorded with the specified user-defined signal name and input (if
      # provided).
      #
      # @param signal_name
      #   The user-defined name of the signal.
      #
      # @param workflow_execution
      #   The workflow execution to signal.
      #
      # @param [Hash, SignalWorkflowOptions] block
      #   A block of {SignalWorkflowOptions} for the `WorkflowExecutionSignaled` event.
      #
      def signal_workflow_execution(signal_name = nil, workflow_execution = nil, &block)
        if Utilities::is_external
          self.signal_external_workflow(signal_name, workflow_execution, &block)
        else
          self.signal_internal_workflow(signal_name, workflow_execution, &block)
        end
      end


        # Called by {#signal_workflow_execution}.
        # @api private
        def signal_external_workflow(signal_name, workflow_execution, &block)
        options = Utilities::interpret_block_for_options(SignalWorkflowOptions, block)
        options.signal_name ||= signal_name
        if workflow_execution
          options.domain ||= workflow_execution.domain.name.to_s
          options.workflow_id ||= workflow_execution.workflow_id.to_s
        end
        @service.signal_workflow_execution(options.get_full_options)
      end

        # Called by {#signal_workflow_execution}.
        # @api private
      def signal_internal_workflow(signal_name, workflow_execution, &block)
        get_decision_context
        options = Utilities::interpret_block_for_options(SignalWorkflowOptions, block)
        # Unpack the workflow execution from the future
        workflow_execution = workflow_execution.workflow_execution if workflow_execution.respond_to? :workflow_execution
        options.signal_name ||= signal_name.to_s
        options.workflow_id ||= workflow_execution.workflow_id.get.to_s
        Utilities::merge_all_options(options)
        open_request = OpenRequestInfo.new
        decision_id = @decision_helper.get_next_id(:Signal)
        options.control ||= decision_id
        external_task do |external|
          external.initiate_task do |handle|
            @decision_helper[decision_id] = SignalDecisionStateMachine.new(decision_id, options)
            open_request.completion_handle = handle
            @decision_helper.scheduled_signals[decision_id] = open_request
          end
          external.cancellation_handler do |handle, cause|
            @decision_helper[decision_id].consume(:cancel)
            open_request = @decision_helper.scheduled_signal.delete(decision_id)
            raise "Signal #{decision_id} wasn't scheduled" unless open_request
            handle.complete
          end
        end
        return open_request.result
      end


      # Called by {#start_execution}.
      # @api private
      def start_internal_workflow(method_name, input = NoInput.new, &block)
        get_decision_context
        options = Utilities::interpret_block_for_options(StartWorkflowOptions, block)
        client_options = Utilities::client_options_from_method_name(method_name, @options)
        options = Utilities::merge_all_options(client_options, options)

        workflow_id_future, run_id_future = Future.new, Future.new
        minimal_domain = MinimalDomain.new(@domain.name.to_s)
        output = WorkflowFuture.new(AWS::Flow::MinimalWorkflowExecution.new(minimal_domain, workflow_id_future, run_id_future))
        new_options = StartWorkflowOptions.new(options)
        open_request = OpenRequestInfo.new
        workflow_id = new_options.workflow_id
        run_id = @decision_context.workflow_context.decision_task.workflow_execution.run_id
        workflow_id ||= @decision_helper.get_next_id(run_id.to_s + ":")
        workflow_id_future.set(workflow_id)
        error_handler do |t|
          t.begin do
            @data_converter = new_options.data_converter
            input = @data_converter.dump input unless input.empty?
            attributes = {}
            new_options.input ||= input unless input.empty?
            if @workflow_class != nil && new_options.execution_method.nil?
              new_options.execution_method = @workflow_class.entry_point
            end
            raise "Can't find an execution method for workflow #{@workflow_class}" if new_options.execution_method.nil?

            attributes[:options] = new_options
            attributes[:workflow_id] = workflow_id
            # TODO Use ChildWorkflowOptions
            attributes[:tag_list] = []

            external_task do |external|
              external.initiate_task do |handle|
                open_request.completion_handle = handle
                open_request.run_id = run_id_future
                open_request.description = output.workflow_execution
                @decision_helper.scheduled_external_workflows[workflow_id.to_s] = open_request
                @decision_helper[workflow_id.to_s] = ChildWorkflowDecisionStateMachine.new(workflow_id, attributes)
              end

              external.cancellation_handler do |handle, cause|
                state_machine = @decision_helper[workflow_id.to_s]
                if state_machine.current_state == :created
                  open_request = @decision_helper.scheduled_external_workflows.delete(workflow_id)
                  open_request.completion_handle.complete
                end
                state_machine.consume(:cancel)
              end
            end

            t.rescue(Exception) do |error|
              if error.is_a? ChildWorkflowFailedException
                details = @data_converter.load(error.details)
                error.details = details
                error.cause = details
              end
              @failure_map[workflow_id.to_s] = error
            end
            t.ensure do
              result = @data_converter.load open_request.result
              output.set(result)
              raise @failure_map[workflow_id.to_s] if @failure_map[workflow_id.to_s] && new_options.return_on_start
            end
          end
        end
        return output if new_options.return_on_start
        output.get
        this_failure = @failure_map[workflow_id.to_s]
        raise this_failure if this_failure
        return output.get
      end


      # Called by {#start_execution}.
      # @api private
      def start_external_workflow(method_name, input = NoInput.new, &block)
        options = Utilities::interpret_block_for_options(StartWorkflowOptions, block)
        client_options = Utilities::client_options_from_method_name(method_name, @options)
        options = Utilities::merge_all_options(client_options, options)

        @data_converter = options[:data_converter]
        # Basically, we want to avoid the special "NoInput, but allow stuff like nil in"
        if ! (input.class <= NoInput || input.empty?)
          options[:input] = @data_converter.dump input
        end
        if @workflow_class.nil?
          execution_method = @options.execution_method
          version = @options.version
        else
          workflow_type = method_name.nil? ? @workflow_class.workflows.first : @workflow_class.workflows.select { |x| x.options.execution_method.to_sym == method_name }.first
          execution_method = workflow_type.options.execution_method
          version = workflow_type.version
        end
        version = options[:version] ? options[:version] : version
        execution_method = options[:execution_method] ? options[:execution_method] : execution_method
        raise "Can't find an execution method for workflow #{workflow_class}" if execution_method.nil?
        # TODO A real workflowtype function
        workflow_name = @options.workflow_name || @options.prefix_name
        workflow_type_name = workflow_name.to_s + "." + execution_method.to_s

        task_list = options[:task_list]
        options[:task_list] = { :name => task_list } if options[:task_list]
        options[:workflow_id] ||= SecureRandom.uuid
        options[:domain] = @domain.name
        options[:workflow_type] = {
          :name => workflow_type_name.to_s,
          :version => version.to_s
        }
        [:prefix_name, :workflow_name, :version, :execution_method, :data_converter].each {|key| options.delete(key)}
        run_id = @service.start_workflow_execution(options)["runId"]
        this_workflow = @domain.workflow_executions.at(options[:workflow_id], run_id)
        this_workflow
      end

      def is_execution_method(method_name)
        (@workflow_class.workflows.map(&:options).map(&:execution_method).map(&:to_sym).include? method_name) || method_name == @workflow_class.entry_point
      end

      def method_missing(method_name, *args, &block)
        if is_execution_method(method_name)
          start_execution_method(method_name, *args, &block)
        else
          super(method_name, *args, &block)
        end
      end
      def request_cancel_workflow_execution(future)
        workflow_execution = future.workflow_execution
        run_id = workflow_execution.run_id.get
        workflow_id = workflow_execution.workflow_id.get
        state_machine = @decision_helper[workflow_id]
        state_machine.run_id = run_id
        state_machine.consume(:cancel)
      end
    end


    # Represents a workflow factory. Instances of `WorkflowFactory` are generated by {#workflow_factory}.
    class WorkflowFactory


      # Creates a new `WorkflowFactory` with the provided parameters. The construction parameters will be used for any
      # workflow clients generated by this workflow factory.
      #
      # @param service
      #   The service to use for workflow clients generated by this workflow factory
      #
      # @param domain
      #   The Amazon SWF [Domain](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Domain.html) to use for
      #   workflow clients generated by this workflow factory.
      #
      # @param block
      #   A block of {StartWorkflowOptions} to use for clients generated by this workflow factory.
      #
      def initialize(service, domain, block)
        @service = service
        @domain = domain
        @options = Utilities::interpret_block_for_options(StartWorkflowOptions, block)
        @workflow_class = get_const(@options.workflow_name) rescue nil
        if @workflow_class
          workflow_type = @workflow_class.workflows.delete_if {|wf_type| wf_type.version.nil? }.first
          @options.version = workflow_type.version
        end
      end


      # Get a {WorkflowClient} with the parameters used in the construction of this {WorkflowFactory}.
      #
      # @return [WorkflowClient]
      #   A workflow client created with the parameters used when creating the {WorkflowFactory}.
      #
      def get_client
        WorkflowClient.new(@service, @domain, @workflow_class, @options)
      end

    end

  end
end

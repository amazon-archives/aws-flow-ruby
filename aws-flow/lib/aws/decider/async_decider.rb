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

    # Represents a decision ID.
    class DecisionID

      # Creates a new decision ID.
      #
      # @param decision_target
      #   The decision target.
      #
      # @param string_id
      #   The string that identifies this decision.
      #
      def initialize(decision_target, string_id)
        @decision_target = decision_target
        @string_id = string_id
      end

      # Hash function to return an unique value for the decision ID.
      #
      # @return
      #   The calculated hash value for the decision ID.
      #
      def hash
        prime = 31
        result = 1
        result = result * prime + (@decision_target == nil ? 0 : @decision_target.hash)
        result = prime * result + (@string_id == nil ? 0 : @string_id.hash)
        result
      end

      # Is this decision ID the same as another?
      #
      # @param [Object] other
      #   The object to compare with.
      #
      # @return [true, false]
      #   Returns `true` if the object is the same as this decision ID; `false` otherwise.
      #
      def eql?(other)

      end
    end

    # @api private
    class DecisionWrapper
      #TODO Consider taking out the id, it's unclear if it is needed
      def initialize(id, decision, options = [])
        @decision = decision
        @id = id
      end

      # @api private
      def get_decision
        @decision
      end
      # @api private
      def consume(symbol)
        # quack like a state machine
      end
      # quack like a decision, too
      def keys
        return []
      end
    end


    # A decision helper for a workflow.
    #
    # @!attribute [Hash] activity_options
    #
    # @!attribute [Hash] activity_scheduling_event_id_to_activity_id
    #
    # @!attribute [Hash] decision_map
    #
    # @!attribute [Hash] scheduled_activities
    #
    # @!attribute [Hash] scheduled_external_workflows
    #
    # @!attribute [Hash] scheduled_signals
    #
    # @!attribute [Hash] scheduled_timers
    #
    # @!attribute [Hash] signal_initiated_event_to_signal_id
    #
    # @api private
    class DecisionHelper
      attr_accessor :decision_map, :activity_scheduling_event_id_to_activity_id, :scheduled_activities, :scheduled_timers, :activity_options, :scheduled_external_workflows, :scheduled_signals, :signal_initiated_event_to_signal_id, :child_initiated_event_id_to_workflow_id, :workflow_context_data
      class << self
        attr_reader :maximum_decisions_per_completion, :completion_events, :force_immediate_decision_timer
      end
      @force_immediate_decision_timer = "FORCE_IMMEDIATE_DECISION_TIMER"
      @maximum_decisions_per_completion = 100
      @completion_events = [:CancelWorkflowExecution, :CompleteWorkflowExecution, :FailWorkflowExecution, :ContinueAsNewWorkflowExecution]

      # Creates a new, empty {DecisionHelper} instance.
      def initialize
        @decision_map = {}
        @id = Hash.new {|hash, key| hash[key] = 0 }
        @scheduled_signals = {}
        @activity_scheduling_event_id_to_activity_id = {}
        @scheduled_activities = {}
        @scheduled_external_workflows = {}
        @scheduled_timers = {}
        @activity_options = {}
        @signal_initiated_event_to_signal_id = {}
        @child_initiated_event_id_to_workflow_id = {}
      end

      # @api private
      def get_next_id(decision_target)
        id = (@id[decision_target] += 1)
        "#{decision_target}#{id}"
      end

      # @api private
      def get_next_state_machine_which_will_schedule(list)
        return if list.empty?
        ele = list.shift
        ele = list.shift until (list.empty? || ele.get_decision != nil)
        ele
      end

      def is_completion_event(decision)
        DecisionHelper.completion_events.include? decision.get_decision[:decision_type].to_sym
      end

      # @api private
      def handle_decision_task_started_event
        # In order to ensure that the events we have already scheduled do not
        # make a decision, we will process only maximum_decisions_per_completion
        # here.
        count = 0
        decision_list = @decision_map.values
        decision_state_machine = get_next_state_machine_which_will_schedule(decision_list)
        until decision_state_machine.nil?
          next_decision_state_machine = get_next_state_machine_which_will_schedule(decision_list)
          count += 1
          if (count == DecisionHelper.maximum_decisions_per_completion &&
              next_decision_state_machine != nil &&
              ! is_completion_event(next_decision_state_machine))
            break
          end
          decision_state_machine.consume(:handle_decision_task_started_event)
          decision_state_machine = next_decision_state_machine
        end
        if (next_decision_state_machine != nil &&
            count < DecisionHelper.maximum_decisions_per_completion)
          next_decision_state_machine.consume(:handle_decision_task_started_event)
        end
      end

      # @api private
      def method_missing(method_name, *args)
        if [:[]=, :[]].include? method_name
          @decision_map.send(method_name, *args)
        end
      end

      # Returns the activity ID for a scheduled activity.
      #
      # @param [String] scheduled_id
      #   The scheduled activity ID.
      #
      # @api private
      def get_activity_id(scheduled_id)
        activity_scheduling_event_id_to_activity_id[scheduled_id]
      end
    end

    # Represents an asynchronous decider class.
    class AsyncDecider
      include Utilities::SelfMethods
      attr_accessor :task_token, :decision_helper

      # Creates a new asynchronous decider.
      def initialize(workflow_definition_factory, history_helper, decision_helper)
        @workflow_definition_factory = workflow_definition_factory
        @history_helper = history_helper
        @decision_helper = decision_helper
        @decision_task = history_helper.get_decision_task
        @workflow_clock = WorkflowClock.new(@decision_helper)

        @workflow_context = WorkflowContext.new(@decision_task, @workflow_clock)
        @activity_client = GenericActivityClient.new(@decision_helper, nil)
        @workflow_client = GenericWorkflowClient.new(@decision_helper, @workflow_context)
        @decision_context = DecisionContext.new(@activity_client, @workflow_client, @workflow_clock, @workflow_context, @decision_helper)
      end

      # @note *Beware, this getter will modify things*, as it creates decisions for the objects in the {AsyncDecider}
      #   that need decisions sent out.
      #
      # @api private
      def get_decisions
        result = @decision_helper.decision_map.values.map {|decision_object|
          decision_object.get_decision}.compact
        if result.length > DecisionHelper.maximum_decisions_per_completion
          result = result.slice(0, DecisionHelper.maximum_decisions_per_completion - 1)
          result << ({:decision_type => "StartTimer", :start_timer_decision_attributes => {
                         :timer_id => DecisionHelper.force_immediate_decision_timer,
                            :start_to_fire_timeout => "0"
                          }})
        end
        return result
      end

      # @api private
      def decide
        begin
          decide_impl
        rescue Exception => error
          raise error
        ensure
          begin
            @decision_helper.workflow_context_data = @definition.get_workflow_state
          rescue WorkflowException => error
            @decision_helper.workflow_context_data = error.details
          rescue Exception => error
            @decision_helper.workflow_context_data = error.message
            # Catch and do stuff
          ensure
            @workflow_definition_factory.delete_workflow_definition(@definition)
          end
        end
      end

      # @api private
      def decide_impl
        single_decision_event = @history_helper.get_single_decision_events
        while single_decision_event.length > 0
          @decision_helper.handle_decision_task_started_event
          [*single_decision_event].each do |event|
            last_non_replay_event_id = @history_helper.get_last_non_replay_event_id
            @workflow_clock.replaying = false if event.event_id >= last_non_replay_event_id
            @workflow_clock.replay_current_time_millis = @history_helper.get_replay_current_time_millis
            process_event(event)
            event_loop(event)
          end
          @task_token = @history_helper.get_decision_task.task_token
          complete_workflow if completed?
          single_decision_event = @history_helper.get_single_decision_events
        end
        if @unhandled_decision
          @unhandled_decision = false
          complete_workflow
        end
      end

      # Registers a {FailWorkflowExecution} decision.
      #
      # @param [DecisionID] decision_id
       #   The ID of the {DecisionTaskCompleted} event corresponding to the decision task that resulted in the decision
      #   failing in this execution. This information can be useful for tracing the sequence of events back from the
      #   failure.
      #
      # @param [Exception] failure
      #   The exception that is associated with the failed workflow.
      #
      # @see http://docs.aws.amazon.com/amazonswf/latest/apireference/API_FailWorkflowExecutionDecisionAttributes.html
      #   FailWorkflowExecutionDecisionAttributes
      #
      def make_fail_decision(decision_id, failure)
        decision_type = "FailWorkflowExecution"

        # Get the reason from the failure. Or get the message if a
        # CancellationException is initialized without a reason. Fall back to
        # a default string if nothing is provided
        reason = failure.reason || failure.message || "Workflow failure did not provide any reason."
        # Get the details from the failure. Or get the backtrace if a
        # CancellationException is initialized without a details. Fall back to
        # a default string if nothing is provided
        details = failure.details || failure.backtrace.to_s || "Workflow failure did not provide any details."

        fail_workflow_execution_decision_attributes = { reason: reason, details: details }
        decision = {:decision_type => decision_type, :fail_workflow_execution_decision_attributes => fail_workflow_execution_decision_attributes}
        CompleteWorkflowStateMachine.new(decision_id, decision)

      end

      def make_completion_decision(decision_id, decision)
        CompleteWorkflowStateMachine.new(decision_id, decision)
      end
      def make_cancel_decision(decision_id)
        CompleteWorkflowStateMachine.new(decision_id, {:decision_type => "CancelWorkflowExecution"})
      end
      # Continues this as a new workflow, using the provided decision and options.
      #
      # @param [DecisionID] decision_id
      #   The decision ID to use.
      #
      # @param [WorkflowOptions] continue_as_new_options
      #   The options to use for the new workflow.
      #
      def continue_as_new_workflow(decision_id, continue_as_new_options)
        result = {
          :decision_type => "ContinueAsNewWorkflowExecution",
        }

        task_list = continue_as_new_options.task_list ? {:task_list => {:name => continue_as_new_options.task_list}} : {}
        to_add = continue_as_new_options.get_options([:execution_start_to_close_timeout, :task_start_to_close_timeout, :task_priority, :child_policy, :tag_list, :workflow_type_version, :input], task_list)
        result[:continue_as_new_workflow_execution_decision_attributes] = to_add
        CompleteWorkflowStateMachine.new(decision_id, result)
      end

      # Registers a `CompleteWorkflowExecution` decision.
      #
      # @see http://docs.aws.amazon.com/amazonswf/latest/apireference/API_CompleteWorkflowExecutionDecisionAttributes.html
      #   CompleteWorkflowExecutionDecisionAttributes
      #
      def complete_workflow
        return unless @completed && ! @unhandled_decision
        decision_id = [:SELF, nil]
        if @failure
          @decision_helper[decision_id] = make_fail_decision(decision_id, @failure)
        elsif @cancel_requested
              @decision_helper[decision_id] = make_cancel_decision(decision_id)
        else

          if ! @workflow_context.continue_as_new_options.nil?
            @decision_helper[decision_id] = continue_as_new_workflow(decision_id, @workflow_context.continue_as_new_options)
          else
            if @result.nil?
              @decision_helper[decision_id] = make_completion_decision(decision_id, {
                                                                         :decision_type => "CompleteWorkflowExecution"})
            else
              @decision_helper[decision_id] = make_completion_decision(decision_id, {
                                                                         :decision_type => "CompleteWorkflowExecution",
                                                                         :complete_workflow_execution_decision_attributes => {:result => @result.get }})
            end
          end
        end
      end

      # Indicates whether the task completed.
      #
      # @return [true, false]
      #   Returns `true` if the task is completed; `false` otherwise.
      #
      def completed?
        @completed
      end

      # Handler for the `:ActivityTaskScheduled` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_activity_task_scheduled(event)
        activity_id = event.attributes[:activity_id]
        @decision_helper.activity_scheduling_event_id_to_activity_id[event.id] = activity_id
        @decision_helper[activity_id].consume(:handle_initiated_event)
        return @decision_helper[activity_id].done?
      end

      # Handler for the `:WorkflowExecutionStarted` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_workflow_execution_started(event)
        @workflow_async_scope = AsyncScope.new do
          FlowFiber.current[:decision_context] = @decision_context
          input = (event.attributes.keys.include? :input) ?  event.attributes[:input] : nil
          @definition = @workflow_definition_factory.get_workflow_definition(@decision_context)
          @result = @definition.execute(input)
        end
      end

      # Handler for the `:TimerFired` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_timer_fired(event)
        timer_id = event.attributes[:timer_id]
        return if timer_id == DecisionHelper.force_immediate_decision_timer
        @decision_helper[timer_id].consume(:handle_completion_event)
        if @decision_helper[timer_id].done?
          open_request = @decision_helper.scheduled_timers.delete(timer_id)
          return if open_request.nil?
          open_request.blocking_promise.set(nil)
          open_request.completion_handle.complete
        end
      end

      # Handler for the `:StartTimerFailed` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_start_timer_failed(event)
        timer_id = event.attributes.timer_id
        return if timer_id == DecisionHelper.force_immediate_decision_timer
        handle_event(event, {
                       :id_methods => [:timer_id],
                       :consume_symbol => :handle_completion_event,
                       :decision_helper_scheduled => :scheduled_timers,
                       :handle_open_request => lambda do |event, open_request|
                         exception = StartTimerFailedException(event.id, timer_id, nil, event.attributes.cause)
                         open_request.completion_handle.fail(exception)
                       end
                     })
        state_machine = @decision_helper[timer_id]


      end

      # Handler for the `:WorkflowExecutionCancelRequested` event.
      # @param [Object] event
      #   The event to process.
      def handle_workflow_execution_cancel_requested(event)
        @workflow_async_scope.cancel(CancellationException.new("Cancelled from a WorkflowExecutionCancelRequested"))
        @cancel_requested = true
      end

      # Handler for the `:ActivityTaskCancelRequested` event.
      # @param [Object] event
      #   The event to process.
      def handle_activity_task_cancel_requested(event)
        activity_id = event.attributes[:activity_id]
        @decision_helper[activity_id].consume(:handle_cancellation_initiated_event)
      end

      # Handler for the `:RequestCancelActivityTaskFailed` event.
      # @param [Object] event
      #   The event to process.
      def handle_request_cancel_activity_task_failed(event)
        handle_event(event, {
                       :id_methods => [:activity_id],
                       :consume_symbol => :handle_cancellation_failure_event
                     })
      end

      def handle_closing_failure
        @unhandled_decision = true
        @decision_helper[[:SELF, nil]].consume(:handle_initiation_failed_event)
      end

      # Handler for the `:CompleteWorkflowExecutionFailed` event.
      # @param [Object] event
      #   The event to process.
      def handle_complete_workflow_execution_failed(event)
        handle_closing_failure
      end

      # Handler for the `:FailWorkflowExecutionFailed` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_fail_workflow_execution_failed(event)
        handle_closing_failure
      end

      # Handler for the `:CancelWorkflowExecutionFailed` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_cancel_workflow_execution_failed(event)
        handle_closing_failure
      end

      # Handler for the `:ContinueAsNewWorkflowExecutionFailed` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_continue_as_new_workflow_execution_failed(event)
        handle_closing_failure
      end

      # Handler for the `:TimerStarted` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_timer_started(event)
        timer_id = event.attributes[:timer_id]
        return if timer_id == DecisionHelper.force_immediate_decision_timer
        @decision_helper[timer_id].consume(:handle_initiated_event)
        @decision_helper[timer_id].done?
      end

      # Handler for the `:TimerCanceled` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_timer_canceled(event)
        handle_event(event, {
                       :id_methods => [:timer_id],
                       :consume_symbol => :handle_cancellation_event,
                       :decision_helper_scheduled => :scheduled_timers,
                       :handle_open_request => lambda do |event, open_request|
                         if ! open_request.nil?
                           cancellation_exception = CancellationException.new("Cancelled from a Timer Cancelled event")
                           open_request.completion_handle.fail(cancellation_exception)
                         end
                       end
                     })
      end

      # Handler for the `:SignalExternalWorkflowExecutionInitiated` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_signal_external_workflow_execution_initiated(event)
        signal_id = event.attributes[:control]
        @decision_helper.signal_initiated_event_to_signal_id[event.id] = signal_id
        @decision_helper[signal_id].consume(:handle_initiated_event)
        @decision_helper[signal_id].done?
      end

      # Handler for the `:RequestCancelExternalWorkflowExecutionInitiated` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_request_cancel_external_workflow_execution_initiated(event)
        handle_event(event, {
                       :id_methods => [:workflow_id],
                       :consume_symbol => :handle_cancellation_initiated_event
                     })
      end

      # Handler for the `:RequestCancelExternalWorkflowExecutionFailed` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_request_cancel_external_workflow_execution_failed(event)
        handle_event(event, {
                       :id_methods => [:workflow_id],
                       :consume_symbol => :handle_cancellation_failure_event
                     })
      end

      # Handler for the `:StartChildWorkflowExecutionInitiated` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_start_child_workflow_execution_initiated(event)
        workflow_id = event.attributes[:workflow_id]
        @decision_helper.child_initiated_event_id_to_workflow_id[event.id] = workflow_id
        @decision_helper[workflow_id].consume(:handle_initiated_event)
        @decision_helper[workflow_id].done?
      end

      # Handler for the `:CancelTimerFailed` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_cancel_timer_failed(event)
        handle_event(event, {
                       :id_methods => [:timer_id],
                       :consume_symbol => :handle_cancellation_failure_event
                     })
      end

      # Handler for the `WorkflowExecutionSignaled` event.
      #
      # @param [Object] event
      #   The event to process.
      #
      def handle_workflow_execution_signaled(event)
        signal_name = event.attributes[:signal_name]
        input = event.attributes[:input] if event.attributes.keys.include? :input
        input ||= NoInput.new
        # TODO do stuff if we are @completed
        t = Task.new(nil) do
          @definition.signal_received(signal_name, input)
        end
        task_context = TaskContext.new(:parent => @workflow_async_scope.get_closest_containing_scope, :task => t)
        @workflow_async_scope.get_closest_containing_scope << t
      end

      # Processes decider events.
      #
      # @param [Object] event
      #   The event to process.
      #
      def process_event(event)
        event_type_symbol = event.event_type.to_sym
        # Mangle the name so that it is handle_ + the name of the event type in snakecase
        handle_event = "handle_" + event.event_type.gsub(/(.)([A-Z])/,'\1_\2').downcase
        noop_set = Set.new([:DecisionTaskScheduled, :DecisionTaskCompleted,
        :DecisionTaskStarted, :DecisionTaskTimedOut, :WorkflowExecutionTimedOut,
        :WorkflowExecutionTerminated, :MarkerRecorded,
        :WorkflowExecutionCompleted, :WorkflowExecutionFailed,
        :WorkflowExecutionCanceled, :WorkflowExecutionContinuedAsNew, :ActivityTaskStarted])

        return if noop_set.member? event_type_symbol

        self_set = Set.new([:TimerFired, :StartTimerFailed,
        :WorkflowExecutionCancel, :ActivityTaskScheduled,
        :WorkflowExecutionCancelRequested,
        :ActivityTaskCancelRequested, :RequestCancelActivityTaskFailed,
        :CompleteWorkflowExecutionFailed, :FailWorkflowExecutionFailed,
        :CancelWorkflowExecutionFailed, :ContinueAsNewWorkflowExecutionFailed,
        :TimerStarted, :TimerCanceled,
        :SignalExternalWorkflowExecutionInitiated,
        :RequestCancelExternalWorkflowExecutionInitiated,
        :RequestCancelExternalWorkflowExecutionFailed,
        :StartChildWorkflowExecutionInitiated, :CancelTimerFailed, :WorkflowExecutionStarted, :WorkflowExecutionSignaled])

        activity_client_set = Set.new([:ActivityTaskCompleted,
        :ActivityTaskCanceled, :ActivityTaskTimedOut,
        :ScheduleActivityTaskFailed, :ActivityTaskFailed])

        workflow_client_set =
        Set.new([:ExternalWorkflowExecutionCancelRequested,
        :ChildWorkflowExecutionCanceled, :ChildWorkflowExecutionCompleted,
        :ChildWorkflowExecutionFailed,
        :ChildWorkflowExecutionStarted, :ChildWorkflowExecutionTerminated,
        :ChildWorkflowExecutionTimedOut, :ExternalWorkflowExecutionSignaled,
        :SignalExternalWorkflowExecutionFailed,
        :StartChildWorkflowExecutionFailed])

        event_set_to_object_mapping = { self_set => self,
          activity_client_set => @activity_client,
          workflow_client_set => @workflow_client }
        thing_to_operate_on = event_set_to_object_mapping.map {|key, value|
          value if key.member? event_type_symbol }.compact.first
        thing_to_operate_on.send(handle_event, event)
          # DecisionTaskStarted is taken care of at TODO
      end

      # @api private
      def event_loop(event)
        return if @completed
        begin
          @completed = @workflow_async_scope.eventLoop
          #TODO Make this a cancellationException, set it up correctly?
        rescue Exception => e
          @failure = e unless @cancel_requested
          @completed = true
        end
      end

    end

  end
end

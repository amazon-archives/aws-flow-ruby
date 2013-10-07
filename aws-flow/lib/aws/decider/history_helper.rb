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

require 'set'

module AWS
  module Flow
    class HistoryHelper
      def initialize(decision_task_iterator)
        # TODO make sure we page through correctly
        @single_decision_events_iterator = SingleDecisionIterator.new(decision_task_iterator)
      end

      def get_single_decision_events
        @current_decision_data = @single_decision_events_iterator.next
        return @current_decision_data.decision_events
      end

      def get_replay_current_time_millis
        raise IllegalStateException if @current_decision_data.nil?
        @current_decision_data.replay_current_time_milliseconds
      end

      def get_last_non_replay_event_id
        result = get_decision_task.previous_started_event_id
        result ||= 0
      end

      def get_decision_task
        @single_decision_events_iterator.get_decision_task
      end
    end

    class EventsIterator
      attr_accessor :events, :decision_task, :decision_tasks

      def initialize(decision_tasks)
        @decision_tasks = decision_tasks
        if ! @decision_tasks.nil?
          @decision_task = decision_tasks
          @events = @decision_task.events.to_a
        end
      end
    end

    class SingleDecisionData
      attr_reader :decision_events, :replay_current_time_milliseconds, :workflow_context_data
      def initialize(decision_events, replay_current_time_milliseconds, workflow_context_data)
        @decision_events = decision_events
        @replay_current_time_milliseconds = replay_current_time_milliseconds
        @workflow_context_data = workflow_context_data
      end
    end

    class SingleDecisionIterator
      class << self
        attr_accessor :decision_events
      end
      @decision_events = Set.new([
                                  :ActivityTaskCancelRequested,
                                  :ActivityTaskScheduled,
                                  :CancelTimerFailed,
                                  :CancelWorkflowExecutionFailed,
                                  :CompleteWorkflowExecutionFailed,
                                  :ContinueAsNewWorkflowExecutionFailed,
                                  :FailWorkflowExecutionFailed,
                                  :MarkerRecorded,
                                  :RequestCancelActivityTaskFailed,
                                  :RequestCancelExternalWorkflowExecutionFailed,
                                  :RequestCancelExternalWorkflowExecutionInitiated,
                                  :ScheduleActivityTaskFailed,
                                  :SignalExternalWorkflowExecutionFailed,
                                  :SignalExternalWorkflowExecutionInitiated,
                                  :StartChildWorkflowExecutionFailed,
                                  :StartChildWorkflowExecutionInitiated,
                                  :StartTimerFailed,
                                  :TimerCanceled,
                                  :TimerStarted,
                                  :WorkflowExecutionCanceled,
                                  :WorkflowExecutionCompleted,
                                  :WorkflowExecutionContinuedAsNew,
                                  :WorkflowExecutionFailed
                                 ])

      def is_decision_event?(event)
        SingleDecisionIterator.decision_events.member? event
      end

      def get_decision_task
        @events.decision_task
      end

      def initialize(decision_tasks)
        @events = EventsIterator.new(decision_tasks)
        fill_next
        @current = @next
        fill_next
      end

      def next
        result = @current
        @current = @next
        fill_next
        return result
      end

      def reorder_events(start_to_completion, completion_to_start, last_decision_index)
        reordered = []
        reordered.concat(completion_to_start.slice(0, last_decision_index + 1)) if last_decision_index >= 0
        reordered.concat(start_to_completion)
        if completion_to_start.length > last_decision_index + 1
          reordered.concat(completion_to_start.slice((last_decision_index + 1)..-1))
        end
        return reordered.flatten
      end

      def fill_next
        decision_task_timed_out = false
        decision_start_to_completion_events, decision_completion_to_start_events = [], []
        next_replay_current_time_milliseconds = -1
        last_decision_index = -1
        while @events.events.length > 0
          event = @events.events.shift
          event_type = event.event_type.to_sym
          case event_type
          when :DecisionTaskCompleted
            #TODO get execution context
            #TODO updateWorkflowContextDataAndComponentVersions
            concurrent_to_decision = false
          when :DecisionTaskStarted
            next_replay_current_time_milliseconds = event.created_at
            if decision_task_timed_out
              @current.decision_events.concat(decision_start_to_completion_events)
              decision_start_to_completion_events = []
              decision_task_timed_out = false
            else
              break
            end
          when :DecisionTaskTimedOut
            decision_task_timed_out = true
          when :DecisionTaskScheduled
            # pass
          when :MarkerRecorded
            # pass
          else
            if concurrent_to_decision
              decision_start_to_completion_events << event
            else
              if is_decision_event? event_type
                last_decision_index = decision_completion_to_start_events.length
              end
              decision_completion_to_start_events << event
            end
          end
        end
        next_events = reorder_events(decision_start_to_completion_events, decision_completion_to_start_events, last_decision_index)
        @next = SingleDecisionData.new(next_events, next_replay_current_time_milliseconds, @workflow_context_data )
      end
    end
  end
end

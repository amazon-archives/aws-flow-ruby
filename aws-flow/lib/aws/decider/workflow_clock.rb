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


    # Represents a workflow clock that can create timers.
    class WorkflowClock

      # Gets or sets the replaying status of the workflow clock.
      attr_accessor :replaying

      # Gets or sets the current time in milliseconds for a replay.
      attr_accessor :replay_current_time_millis


      # Create a new {WorkflowClock} instance.
      #
      # @param [DecisionHelper] decision_helper
      #   The decision helper used by the workflow clock to schedule and execute timers.
      #
      def initialize(decision_helper)
        #Map from timerIDs to OpenRequestInfo
        @scheduled_timers = {}
        @replaying = true
        @replay_current_time_millis
        @decision_helper = decision_helper
      end


      # Get the current time.
      #
      # @return [Time]
      #   A `Time` object initialized to the the current time in milliseconds for a replay. This is the same time that
      #   is provided by the `:replay_current_time_millis` attribute.
      #
      def current_time
        @replay_current_time_millis
      end

      # Create a new timer that executes the supplied block after a specified number of seconds.
      #
      # @param delay_seconds
      #   The number of seconds to wait before executing the block. Set to 0 to execute the block immediately.
      #
      # @param block
      #   A block to execute after `delay_seconds` has expired.
      #
      def create_timer(delay_seconds, block)
        raise IllegalArgumentException if delay_seconds < 0
        if delay_seconds == 0
          if block
            return block.call
          else
            return
          end
        end
        attributes = {}
        timer_id = @decision_helper.get_next_id(:Timer)
        attributes[:timer_id] = timer_id
        attributes[:start_to_fire_timeout] = delay_seconds.to_s
        open_request = OpenRequestInfo.new
        open_request.blocking_promise = Future.new
        if block
          open_request.result = task do
            open_request.blocking_promise.get
            block.call
          end
        else
          open_request.result = open_request.blocking_promise
        end
        external_task do |t|
          t.initiate_task do |handle|
            open_request.completion_handle = handle
            @decision_helper.scheduled_timers[timer_id.to_s] = open_request
            @decision_helper[timer_id.to_s] = TimerDecisionStateMachine.new(timer_id, attributes)
          end
          t.cancellation_handler do |handle, cause|
            state_machine = @decision_helper[timer_id]
            open_request = @decision_helper.scheduled_timers.delete(timer_id)
            open_request.completion_handle.complete
            state_machine.consume(:cancel)
            state_machine.cancelled = true
          end
        end
        return open_request.result.get
      end
    end

  end
end

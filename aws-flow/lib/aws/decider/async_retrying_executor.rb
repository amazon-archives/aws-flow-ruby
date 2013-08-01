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

    class AsyncRetryingExecutor
      def initialize(retrying_policy, clock, execution_id, return_on_start = false)
        @retrying_policy = retrying_policy
        @clock = clock
        @return_on_start = return_on_start
        @execution_id = execution_id
      end
      def execute(command, options = nil)
        return schedule_with_retry(command, nil, Hash.new { |hash, key| hash[key] = 1 }, @clock.current_time, 0) if @return_on_start
        task do
          schedule_with_retry(command, nil, Hash.new { |hash, key| hash[key] = 1 }, @clock.current_time, 0)
        end
      end

      def schedule_with_retry(command, failure, attempt, first_attempt_time, time_of_recorded_failure)
        delay = -1
        if attempt.values.reduce(0, :+) > 1
          raise failure unless @retrying_policy.isRetryable(failure)
          delay = @retrying_policy.next_retry_delay_seconds(first_attempt_time, time_of_recorded_failure, attempt, failure, @execution_id)
          raise failure if delay < 0
        end
        if delay > 0
          task do
            @clock.create_timer(delay, lambda { invoke(command, attempt, first_attempt_time) })
          end
        else
          invoke(command, attempt, first_attempt_time)
        end
      end

      def invoke(command, attempt, first_attempt_time)
        failure_to_retry = nil
        should_retry = Future.new
        return_value = Future.new
        output = Utilities::AddressableFuture.new
        error_handler do |t|
          t.begin { return_value.set(command.call) }
          t.rescue(Exception) do |error|
            failure_to_retry = error
            raise error if error.class <= CancellationException
          end
          t.ensure { should_retry.set(failure_to_retry) }
        end
        task do
          failure = should_retry.get
          if ! failure.nil?
            attempt[failure.class] += 1
            output.set(schedule_with_retry(command, failure, attempt, first_attempt_time, @clock.current_time - first_attempt_time))
          else
            output.set(return_value.get)
          end
          #to_return = return_value.set? ? return_value.get : nil
        end
        return output if @return_on_start
        output.get
      end

    end

    # Represents a policy for retrying failed tasks.
    class RetryPolicy

      # Creates a new RetryPolicy instance
      #
      # @param retry_function
      #   The method to be called for each retry attempt.
      #
      # @param options
      #   A set of {RetryOptions} to modify the retry behavior.
      #
      def initialize(retry_function, options)
        @retry_function = retry_function
        @exceptions_to_exclude = options.exceptions_to_exclude
        @exceptions_to_include = options.exceptions_to_include
        @max_attempts = options.maximum_attempts
        @retries_per_exception = options.retries_per_exception
        @should_jitter = options.should_jitter
        @jitter_function = options.jitter_function
      end

      # @param failure
      #   The failure to test.
      #
      # @return [true, false]
      #   Returns `true` if the task can be retried for this failure.
      #
      def isRetryable(failure)
        if failure.respond_to? :cause
          failure_class = failure.cause.class
        else
          failure_class = failure.class
        end

        return true if @exceptions_to_exclude.empty? && @exceptions_to_include.empty?
        raise "#{failure} appears in both exceptions_to_include and exceptions_to_exclude" if @exceptions_to_exclude.include?(failure_class) && @exceptions_to_include.include?(failure_class)
        # In short, default to false
        # the second part of the statement does an intersection of the 2 arrays to see if any of the ancestors of
        # failure exists in @exceptions_to_include
        return (!@exceptions_to_exclude.include?(failure_class) && !(@exceptions_to_include & failure_class.ancestors).empty?)

         #return (!@exceptions_to_exclude.include?(failure) && @exceptions_to_include.include?(failure))
      end

      # Schedules a new retry attempt
      #
      # @param first_attempt
      #
      # @param time_of_recorded_failure
      #
      # @param attempt
      #
      # @param failure
      #
      def next_retry_delay_seconds(first_attempt, time_of_recorded_failure, attempt, failure = nil, execution_id)
        if attempt.values.reduce(0, :+) < 2
          raise "This is bad, you have less than 2 attempts. More precisely, #{attempt} attempts"
        end
        if @max_attempts && @max_attempts != "NONE"
          return -1 if attempt.values.reduce(0, :+) > @max_attempts + 1
        end
        if failure && @retries_per_exception && @retries_per_exception.keys.include?(failure.class)
          return -1 if attempt[failure.class] > @retries_per_exception[failure.class]
        end
        return -1 if failure == nil

        # Check to see if we should jitter or not and pass in the jitter function to retry function accordingly.
        retry_seconds = @retry_function.call(first_attempt, time_of_recorded_failure, attempt)
        if @should_jitter
           retry_seconds += @jitter_function.call(execution_id, retry_seconds/2)
        end
        return retry_seconds
      end
    end
  end
end

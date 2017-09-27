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

    # internal AsyncRetryingExecutor class
    # @api private
    class AsyncRetryingExecutor
      # @api private
      def initialize(retrying_policy, clock, execution_id, return_on_start = false)
        @retrying_policy = retrying_policy
        @clock = clock
        @return_on_start = return_on_start
        @execution_id = execution_id
      end

      # @api private
      def execute(command, options = nil)
        return schedule_with_retry(command, nil, Hash.new { |hash, key| hash[key] = 1 }, @clock.current_time, nil) if @return_on_start
        output = Utilities::AddressableFuture.new
        result_lock = Utilities::AddressableFuture.new
        error_handler do |t|
          t.begin do
            output.set(schedule_with_retry(command, nil, Hash.new { |hash, key| hash[key] = 1 }, @clock.current_time, nil))
          end
          t.rescue(Exception) do |error|
            @error_seen = error
          end
          t.ensure do
            output.set unless output.set?
            result_lock.set
          end
        end
        result_lock.get
        raise @error_seen if @error_seen
        output
      end

      # @api private
      def schedule_with_retry(command, failure, attempts, first_attempt_time, time_of_recorded_failure)
        delay = -1
        if attempts.values.reduce(0, :+) > 1
          raise failure unless @retrying_policy.isRetryable(failure)
          delay = @retrying_policy.next_retry_delay_seconds(first_attempt_time, time_of_recorded_failure, attempts, failure, @execution_id)
          raise failure if delay < 0
        end
        if delay > 0
          task do
            @clock.create_timer(delay, lambda { invoke(command, attempts, first_attempt_time) })
          end
        else
          invoke(command, attempts, first_attempt_time)
        end
      end

      # @api private
      def invoke(command, attempts, first_attempt_time)
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
            attempts[failure.class] += 1
            output.set(schedule_with_retry(command, failure, attempts, first_attempt_time, @clock.current_time))
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

      # Creates a new `RetryPolicy` instance.
      #
      # @param retry_function
      #   The method that will be called for each retry attempt.
      #
      # @param options
      #   A set of {RetryOptions} used to modify the retry behavior.
      #
      def initialize(retry_function, options)
        @retry_function = retry_function
        @exceptions_to_exclude = options.exceptions_to_exclude
        @exceptions_to_include = options.exceptions_to_include
        @max_attempts = options.maximum_attempts
        @retries_per_exception = options.retries_per_exception
        @should_jitter = options.should_jitter
        @jitter_function = options.jitter_function
        @options = options
      end

      # @param failure
      #   The failure to test.
      #
      # @return [true, false]
      #   Returns `true` if the task can be retried for this failure; `false`
      #   otherwise.
      #
      def isRetryable(failure)
        return true if @exceptions_to_exclude.empty? && @exceptions_to_include.empty?

        if failure.respond_to?(:cause) && !failure.cause.nil?
          failure_class = failure.cause.class
        else
          failure_class = failure.class
        end

        return (!excluded?(failure_class) && included?(failure_class))
      end

      def excluded?(failure_class)
        !(@exceptions_to_exclude & failure_class.ancestors).empty?
      end

      def included?(failure_class)
        !(@exceptions_to_include & failure_class.ancestors).empty?
      end

      # Schedules a new retry attempt with an initial delay.
      #
      # @param first_attempt
      #   The time to delay before the first retry attempt is made.
      #
      # @param time_of_recorded_failure
      #
      # @param attempts
      #   The number of retry attempts to make before the task is considered to
      #   be failed.
      #
      # @param failure
      #   The type of failure to retry. If not specified, then all types of
      #   failures will be retried.
      #
      # @param execution_id
      #
      def next_retry_delay_seconds(first_attempt, time_of_recorded_failure, attempts, failure = nil, execution_id)
        if attempts.values.reduce(0, :+) < 2
          raise "This is bad, you have less than 2 attempts. More precisely, #{attempts} attempts"
        end
        if @max_attempts && @max_attempts != "NONE"
          return -1 if attempts.values.reduce(0, :+) > @max_attempts + 1
        end
        if failure && @retries_per_exception && @retries_per_exception.keys.include?(failure.class)
          return -1 if attempts[failure.class] > @retries_per_exception[failure.class]
        end
        return -1 if failure == nil

        # For reverse compatbility purposes, we must ensure that this function
        # can take 3 arguments. However, we must also consume options in order
        # for the default retry function to work correctly. Because we support
        # ruby 1.9, we cannot use default arguments in a lambda, so we resort to
        # the following workaround to supply a 4th argument if the function
        # expects it.
        call_args = [first_attempt, time_of_recorded_failure, attempts]
        call_args << @options if @retry_function.arity == 4
        retry_seconds = @retry_function.call(*call_args)
        # Check to see if we should jitter or not and pass in the jitter
        # function to retry function accordingly.
        if @should_jitter && retry_seconds > 0
          retry_seconds += @jitter_function.call(execution_id, retry_seconds/2)
        end
        return retry_seconds
      end
    end

  end
end

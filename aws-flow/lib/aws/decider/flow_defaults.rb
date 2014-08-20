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

    # Constants used by the AWS Flow Framework for Ruby.
    #
    # @!attribute exponential_retry_maximum_retry_interval_seconds
    #   The maximum exponential retry interval in seconds.
    #
    #   Use the value `-1` (the default) to set *no maximum*.
    #
    # @!attribute exponential_retry_retry_expiration_seconds
    #   The maximum time that can pass, in seconds, before the exponential retry
    #   attempt is considered to be a failure.
    #
    #   Use the value -1 (the default) to set *no maximum*.
    #
    # @!attribute exponential_retry_backoff_coefficient
    #
    #   The coefficient used to determine how much to back off the interval
    #   timing for an exponential retry scenario. The default value, `2.0`,
    #   causes each retry attempt to wait twice as long as the previous attempt.
    #
    # @!attribute exponential_retry_maximum_attempts
    #   The maximum number of attempts to make for an exponential retry of a
    #   failed task. The default value is `Float::INFINITY`, which indicates
    #   that there should be *no* limit to the number of retry attempts.
    #
    # @!attribute exponential_retry_function
    #   The default exponential retry function.
    #
    # @!attribute default_data_converter
    #   The DataConverter used to interpret results from Amazon SWF. By
    #   default, this is {YAMLDataConverter}.
    #
    # @!attribute exponential_retry_exceptions_to_include
    #   A list of the exception types to include for initiating retry attempts.
    #   By default, all exceptions are included (the default value is
    #   `Exception`, which is the base class for all exceptions.)
    #
    # @!attribute exponential_retry_exceptions_to_exclude
    #   A list of the exception types to exclude from initiating retry attempts.
    #   By default, no exceptions are excluded; this is an empty list.
    #
    # @!attribute jitter_function
    #   The function that is used to determine how long to wait for the next
    #   retry attempt when *should_jitter* is set to `true`.
    #
    # @!attribute should_jitter
    #   Indicates whether there should be any randomness built in to the timing of
    #   the retry attempt. The default value is `true`.
    #
    class FlowConstants

      class << self
        attr_reader :exponential_retry_maximum_retry_interval_seconds, :exponential_retry_retry_expiration_seconds, :exponential_retry_backoff_coefficient, :exponential_retry_maximum_attempts, :exponential_retry_function, :default_data_converter, :exponential_retry_exceptions_to_include, :exponential_retry_exceptions_to_exclude, :jitter_function, :should_jitter, :exponential_retry_initial_retry_interval, :use_worker_task_list
      end

      INFINITY = -1
      RETENTION_DEFAULT = 7
      NUM_OF_WORKERS_DEFAULT = 1
      @exponential_retry_maximum_attempts = Float::INFINITY
      @exponential_retry_maximum_retry_interval_seconds = -1
      @exponential_retry_retry_expiration_seconds = -1
      @exponential_retry_backoff_coefficient = 2.0
      @exponential_retry_initial_retry_interval = 2
      @should_jitter = true
      @exponential_retry_exceptions_to_exclude = []
      @exponential_retry_exceptions_to_include = [Exception]
      @exponential_retry_function = lambda do |first, time_of_failure, attempts, options|

        raise ArgumentError.new("first should be an instance of Time") unless first.instance_of?(Time)
        raise ArgumentError.new("time_of_failure should be nil or an instance of Time") unless time_of_failure.nil? || time_of_failure.instance_of?(Time)
        raise ArgumentError.new("number of attempts should be positive") if (attempts.values.find {|x| x < 0})
        raise ArgumentError.new("number of attempts should be more than 2") if (attempts.values.reduce(0,:+) < 2)
        raise ArgumentError.new("user options must be of type ExponentialRetryOptions") unless options.is_a? ExponentialRetryOptions

        # get values from options
        initial_interval = options.initial_retry_interval
        backoff = options.backoff_coefficient
        max_interval = options.maximum_retry_interval_seconds
        retry_expiration = options.retry_expiration_interval_seconds

        # calculate the initial retry seconds
        result = initial_interval * (backoff ** (attempts.values.reduce(0, :+) - 2))

        # check if the calculated retry seconds is greater than the maximum
        # retry interval allowed. If it is, then replace it with maximum_retry_interval_seconds
        result = max_interval if ( !max_interval.nil? && max_interval != INFINITY && result > max_interval)

        # how much time has elapsed since the first time this task was scheduled
        time_elapsed = time_of_failure.nil? ? 0 : (time_of_failure - first).to_i

        # return -1 if retry will expire
        result = -1 if (! retry_expiration.nil? &&
                        retry_expiration != INFINITY &&
                        (result + time_elapsed) >= retry_expiration)

        return result.to_i
      end

      @jitter_function = lambda do |seed, max_value|
        raise ArgumentError.new("max_value should be greater than 0") unless max_value > 0
        random = Random.new(seed.to_i)
        random.rand(max_value)
      end

      @default_data_converter = YAMLDataConverter.new
      @use_worker_task_list = "USE_WORKER_TASK_LIST"
    end
  end
end

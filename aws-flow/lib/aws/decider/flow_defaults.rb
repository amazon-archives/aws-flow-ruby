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
    # @!attribute [r] default_data_converter
    #   The DataConverter used to interpret results from Amazon SWF.
    #
    #   @return [Object] {YAMLDataConverter}
    #
    # @!attribute [r] exponential_retry_backoff_coefficient
    #   The coefficient used to determine how much to back off the interval
    #     timing for an exponential retry scenario.
    #
    #   @return [Float] `2.0` (each retry takes twice as long as the previous
    #     attempt)
    #
    # @!attribute [r] exponential_retry_exceptions_to_exclude
    #   A list of the exception types to exclude from initiating retry attempts.
    #
    #   @return [Array<Object>] an empty list. No exceptions are excluded.
    #
    # @!attribute [r] exponential_retry_exceptions_to_include
    #   A list of the exception types to include for initiating retry attempts.
    #
    #   @return [Array<Class>] `Exception` (all exceptions are included)
    #
    # @!attribute [r] exponential_retry_function
    #   The default exponential retry function.
    #
    #   @return A *lambda* that takes four parameters: an initial time, a time
    #     of failure, a number of attempts, and a set of
    #     {ExponentialRetryOptions}.
    #
    # @!attribute [r] exponential_retry_initial_retry_interval
    #   The initial retry interval.
    #
    #   @return [Fixnum] `2`
    #
    # @!attribute [r] exponential_retry_maximum_attempts
    #   The maximum number of attempts to make for an exponential retry of a
    #     failed task.
    #
    #   @return [Float] `Float::INFINITY` (there is no limit to the number of
    #     retry attempts)
    #
    # @!attribute [r] exponential_retry_maximum_retry_interval_seconds
    #   The maximum interval that can pass, in seconds, before a retry occurs.
    #
    #   @return [Fixnum] `-1` (no maximum)
    #
    # @!attribute [r] exponential_retry_retry_expiration_seconds
    #   The maximum time that can pass, in seconds, before an exponential retry
    #     attempt is considered to be a failure.
    #
    #   @return [Fixnum] `-1` (no expiration for a retry attempt)
    #
    # @!attribute [r] jitter_function
    #   The function that is used to determine how long to wait for the next
    #     retry attempt when *should_jitter* is set to `true`.
    #
    #   @return a *lambda* that takes a random number seed and a maximum value
    #     (must be > 0).
    #
    # @!attribute [r] should_jitter
    #   Indicates whether there should be any randomness built in to the timing
    #     of the retry attempt.
    #
    #   @return [Boolean] `true`
    #
    # @!attribute [r] use_worker_task_list
    #   Used with activity and workflow options. Indicates that the activity
    #     and/or workflow should use the same task list that the associated
    #     worker is polling on.
    #
    #   @return [String] "USE_WORKER_TASK_LIST"
    #
    class FlowConstants

      class << self
        attr_reader :exponential_retry_maximum_retry_interval_seconds, :exponential_retry_retry_expiration_seconds, :exponential_retry_backoff_coefficient, :exponential_retry_maximum_attempts, :exponential_retry_function, :default_data_converter, :exponential_retry_exceptions_to_include, :exponential_retry_exceptions_to_exclude, :jitter_function, :should_jitter, :exponential_retry_initial_retry_interval, :use_worker_task_list
        attr_accessor :defaults
      end

      # Sizes taken from
      # http://docs.aws.amazon.com/amazonswf/latest/apireference/API_FailWorkflowExecutionDecisionAttributes.html
      DATA_LIMIT = 32768

      # Number of chars that can fit in FlowException's reason
      REASON_LIMIT = 256
      # Number of chars that can fit in FlowException's details. Same as
      # DATA_LIMIT
      DETAILS_LIMIT = DATA_LIMIT
      # This is the truncation overhead for serialization.
      TRUNCATION_OVERHEAD = 8000
      # Truncation string added to the end of a trucated string"
      TRUNCATED = "[TRUNCATED]"

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

      # Selects the data converter to use. By default, YAMLDataConverter is
      # used. S3DataConverter is used when AWS_SWF_BUCKET_NAME environment
      # variable is set.
      def self.data_converter
        return self.default_data_converter unless ENV['AWS_SWF_BUCKET_NAME']
        S3DataConverter.converter
      end

      @defaults = {
        domain: "FlowDefault",
        prefix_name: "FlowDefaultWorkflowRuby",
        execution_method: "start",
        version: "1.0",
        # execution timeout (1 hour)
        execution_start_to_close_timeout: "3600",
        data_converter: data_converter,
        schedule_to_start_timeout: 60,
        start_to_close_timeout: 60,
        retry_policy: { maximum_attempts: 3 },
        task_list: "flow_default_ruby",
        result_activity_prefix: "FlowDefaultResultActivityRuby",
        result_activity_version: "1.0",
        result_activity_method: "run"
      }
      @default_data_converter = YAMLDataConverter.new
      @use_worker_task_list = "USE_WORKER_TASK_LIST"
    end
  end
end

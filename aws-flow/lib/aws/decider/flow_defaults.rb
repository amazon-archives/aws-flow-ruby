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
    class FlowConstants
      class << self
        attr_reader :exponential_retry_maximum_retry_interval_seconds, :exponential_retry_retry_expiration_seconds, :exponential_retry_backoff_coefficient, :exponential_retry_maximum_attempts, :exponential_retry_function, :default_data_converter, :exponential_retry_exceptions_to_include, :exponential_retry_exceptions_to_exclude, :jitter_function, :should_jitter
        # # The maximum exponential retry interval, in seconds. Use the value -1 (the default) to set <i>no maximum</i>.
        # attr_reader :exponential_retry_maximum_retry_interval_seconds

        # # The maximum time that can pass, in seconds, before the exponential retry attempt is considered a failure. Use
        # # the value -1 (the default) to set <i>no maximum</i>.
        # attr_reader :exponential_retry_retry_expiration_seconds

        # # The coefficient used to determine how much to back off the interval timing for an exponential retry scenario.
        # # The default value, 2.0, causes each attempt to wait twice as long as the previous attempt.
        # attr_reader :exponential_retry_backoff_coefficient

        # # The maximum number of attempts to make for an exponential retry of a failed task. The default value is
        # # Float::INFINITY.
        # attr_reader :exponential_retry_maximum_attempts

        # # The default exponential retry function.
        # attr_reader :exponential_retry_function

        # The DataConverter for instances of this class.
        attr_reader :default_data_converter
      end
      INFINITY = -1
      @exponential_retry_maximum_attempts = Float::INFINITY
      @exponential_retry_maximum_retry_interval_seconds = -1
      @exponential_retry_retry_expiration_seconds = -1
      @exponential_retry_backoff_coefficient = 2.0
      @exponential_retry_initial_retry_interval = 2
      @should_jitter = true
      @exponential_retry_exceptions_to_exclude = []
      @exponential_retry_exceptions_to_include = [Exception]
      @exponential_retry_function = lambda do |first, time_of_failure, attempts|
        raise ArgumentError.new("first is not an instance of Time") unless first.instance_of?(Time)
        raise ArgumentError.new("time_of_failure can't be negative") if time_of_failure < 0
        raise ArgumentError.new("number of attempts can't be negative") if (attempts.values.find {|x| x < 0})
        result = @exponential_retry_initial_retry_interval * (@exponential_retry_backoff_coefficient ** (attempts.values.reduce(0, :+) - 2))
        result = @exponential_retry_maximum_retry_interval_seconds if @exponential_retry_maximum_retry_interval_seconds != INFINITY && result > @exponential_retry_maximum_retry_interval_seconds
        seconds_since_first_attempt = time_of_failure.zero? ? 0 : -(first - time_of_failure).to_i
        result = -1 if @exponential_retry_retry_expiration_seconds != INFINITY && (result + seconds_since_first_attempt) >= @exponential_retry_retry_expiration_seconds
        return result.to_i
      end

      @jitter_function = lambda do |seed, max_value|
         random = Random.new(seed.to_i)
         random.rand(max_value)
      end

      @default_data_converter = YAMLDataConverter.new
    end
  end
end

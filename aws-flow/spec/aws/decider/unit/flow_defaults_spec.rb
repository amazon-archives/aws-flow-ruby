require_relative 'setup'

describe FlowConstants do
  options = {
    :initial_retry_interval => 1,
    :backoff_coefficient => 2,
    :should_jitter => false,
    :maximum_retry_interval_seconds => 100
  }
  options = ExponentialRetryOptions.new(options)

  context "#default_exponential_retry_function" do

    it "will test the default retry function with regular cases" do
      time = Time.now
      test_first = [time, time, time]
      test_time_of_failure = [time, time+10, time+100]
      test_attempts = [{Exception=>2}, {Exception=>4}, {ActivityTaskTimedOutException=>5, Exception=>2}]
      test_output = [1, 4, 32]
      arr = test_first.zip(test_time_of_failure, test_attempts, test_output)
      arr.each do |first, time_of_failure, attempts, output|
        result = FlowConstants.exponential_retry_function.call(first, time_of_failure, attempts, options)
        (result == output).should == true
      end
    end

    it "will test for exceptions" do
      expect { FlowConstants.exponential_retry_function.call(-1, nil, {}, options) }.to raise_error(ArgumentError, "first should be an instance of Time")
      expect { FlowConstants.exponential_retry_function.call(Time.now, 0, {}, options) }.to raise_error(ArgumentError, "time_of_failure should be nil or an instance of Time")
      expect { FlowConstants.exponential_retry_function.call(Time.now, nil, {Exception=>-1}, options) }.to raise_error(ArgumentError, "number of attempts should be positive")
      expect { FlowConstants.exponential_retry_function.call(Time.now, nil, {Exception=>-1, ActivityTaskTimedOutException=>-10}, options) }.to raise_error(ArgumentError, "number of attempts should be positive")
      expect { FlowConstants.exponential_retry_function.call(Time.now, nil, {Exception=>2, ActivityTaskTimedOutException=>-10}, options) }.to raise_error(ArgumentError, "number of attempts should be positive")
    end

    it "ensures the default retry function will use the user provided options" do
      first = Time.now
      time_of_failure = nil 
      attempts = {Exception=>2}
      options = {
        :initial_retry_interval => 10,
        :backoff_coefficient => 2,
        :should_jitter => false,
        :maximum_retry_interval_seconds => 5
      }
      options = ExponentialRetryOptions.new(options)
      result = FlowConstants.exponential_retry_function.call(first, time_of_failure, attempts, options)
      result.should == 5
    end

  end

  context "#default_jitter_function" do
    it "ensures that the jitter function checks arguments passed to it" do
      expect { FlowConstants.jitter_function.call(1, -1) }.to raise_error(
        ArgumentError, "max_value should be greater than 0")
    end
    it "ensures that we get same jitter for a particular execution id" do
      (FlowConstants.jitter_function.call(1, 100)).should equal(FlowConstants.jitter_function.call(1, 100))
    end
  end
end


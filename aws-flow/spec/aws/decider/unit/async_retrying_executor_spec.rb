require_relative 'setup'

describe RetryPolicy do
  context "#isRetryable" do
    it "tests the default exceptions included for retry" do
      my_retry_func = lambda do |first, time_of_failure, attempts|
        10
      end
      options = {
        :exceptions_to_include => [ActivityTaskTimedOutException],
        :exceptions_to_exclude => [ActivityTaskFailedException]
      }
      retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
      result = retry_policy.isRetryable(ActivityTaskTimedOutException.new("a", "b", "c", "d"))
      result.should == true

      result = retry_policy.isRetryable(ActivityTaskFailedException.new("a", "b", "c", RuntimeError.new))
      result.should == false
    end


    it "ensures that an exception is raised if the called exception is present in both included and excluded exceptions" do
      my_retry_func = lambda do |first, time_of_failure, attempts|
        10
      end
      options = {
        :exceptions_to_include => [ActivityTaskFailedException],
        :exceptions_to_exclude => [ActivityTaskFailedException]
      }
      retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
      expect {retry_policy.isRetryable(ActivityTaskFailedException.new("a", "b", "c", ActivityTaskFailedException))}.to raise_error
    end
  end

  context "#next_retry_delay_seconds" do
    it "tests exponential retry with a new retry function" do
      my_retry_func = lambda do |first, time_of_failure, attempts|
        10
      end
      options = {
        :should_jitter => false
      }
      retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>10}, Exception, 1)
      result.should == 10
    end

    it "tests the jitter function" do
      my_retry_func = lambda do |first, time_of_failure, attempts|
        10
      end
      options = {
        :should_jitter => true
      }
      retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options, true))
      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>10}, Exception, 1)
      result.should >= 10 && result.should < 15
    end

    it "tests default exceptions included for retry" do
      options = RetryOptions.new
      options.exceptions_to_include.should include Exception
    end

    it "tests max_attempts" do
      my_retry_func = lambda do |first, time_of_failure, attempts|
        10
      end
      options = {
        :maximum_attempts => 5,
        :should_jitter => false
      }
      retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>10}, Exception.new, 1)
      result.should == -1

      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>4, ArgumentError=>3}, Exception.new, 1)
      result.should == -1

      # this should be retried because Exceptions=>5 includes the original exception, followed by 4 retries.
      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>5}, Exception.new, 1)
      result.should == 10

      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>4}, Exception.new, 1)
      result.should == 10
    end

    it "tests retries_per_exception" do
      my_retry_func = lambda do |first, time_of_failure, attempts|
        10
      end
      options = {
        :retries_per_exception => {Exception => 5, ArgumentError => 2},
        :should_jitter => false
      }
      retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>10}, Exception.new, 1)
      result.should == -1

      # this should be retried because Exceptions=>5 includes the original exception, followed by 4 retries.
      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>5}, Exception.new, 1)
      result.should == 10

      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>5, ArgumentError=>3}, ArgumentError.new, 1)
      result.should == -1
    end

    it "ensures that next_retry_delay_seconds honors -1 returned by the retry function" do
      my_retry_func = lambda do |first, time_of_failure, attempts|
        -1
      end
      options = {
        :should_jitter => true
      }
      retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
      result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>10}, Exception, 1)
      result.should == -1
    end

    it "ensures that retry_expiration_interval_seconds works correctly" do
      options = {
        should_jitter: false,
        retry_expiration_interval_seconds: 10,
      }
      retry_policy = RetryPolicy.new(FlowConstants.exponential_retry_function, ExponentialRetryOptions.new(options))
      first = Time.now
      result = retry_policy.next_retry_delay_seconds(first, first+11, {Exception=>10}, Exception, 1)
      result.should == -1
    end
  end
end

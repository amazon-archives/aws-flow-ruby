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

    # A generic activity client.
    class GenericClient

      # The option map for the client.
      attr_accessor :option_map

      # Creates a new generic client.
      def initialize(*args)
        @option_map = {}
      end


      # Sets a map of options for this client.
      #
      # @param opts
      #   The options to set.
      #
      def with_opts(opts = {})
        klass = self.class.default_option_class
        options = klass.new(opts)
        modified_instance = self.dup
        options = klass.new(Utilities::merge_all_options(modified_instance.options, options))
        modified_instance.options = options
        modified_instance
      end

      def reconfigure(*method_names, &block)
        options = Utilities::interpret_block_for_options(self.class.default_option_class, block)
        method_names.each { |method_name| @option_map[method_name.to_sym] = options }
      end

      # @!visibility private
      def bail_if_external
        raise "You cannot use this function outside of a workflow definition" if Utilities::is_external
      end


      # Starts an asynchronous execution of a task. This method returns immediately; it does not wait for the task to
      # complete.
      #
      # @note Trying to use {#send_async} outside of a workflow will fail with an exception.
      #
      # @param task The task to execute.
      #
      # @param args A number of arguments used to start the task execution.
      #
      # @param block A code block with additional options to start the task execution.
      #
      # @example The following two calls are equivalent.
      #    foo.send_async :bar # plus args and block if appropriate
      #
      #    task do
      #      foo.send :bar # plus args and block if appropriate
      #    end
      #
      def send_async(task, *args, &block)
        bail_if_external
        # If there is no block, just make a block for immediate return
        if block.nil?
          modified_options = Proc.new{ {:return_on_start => true } }
          # If there is a block, and it doesn't take any arguments, it will evaluate to a hash. Add an option to the hash
        elsif block.arity == 0
          modified_options = Proc.new do
            result = block.call
            result[:return_on_start] = true
            result
          end
          # Otherwise, it will expect an options object passed in, and will do things on that object. So make our new Proc do that, and add an option
        else modified_options = Proc.new do |x|
            result = block.call(x)
            result.return_on_start = true
            result
          end
        end
        self.send(task, *args, &modified_options)
      end


      # Retries the given method using an exponential fallback function.
      def exponential_retry(method_name, *args, &block)
        future = self._retry(method_name, FlowConstants.exponential_retry_function, block, args)
        Utilities::drill_on_future(future)
      end



      # Used by {#retry}
      # @!visibility private
      def _retry_with_options(lambda_to_execute, retry_function, retry_options, args = NoInput.new)
        retry_policy = RetryPolicy.new(retry_function, retry_options)
        output = Utilities::AddressableFuture.new
        result = nil
        failure = nil
        error_handler do |t|
          t.begin do
            async_retrying_executor = AsyncRetryingExecutor.new(retry_policy, self.decision_context.workflow_clock, self.decision_context.workflow_context.decision_task.workflow_execution.run_id, retry_options.return_on_start)
            result = async_retrying_executor.execute(lambda_to_execute)
          end
          t.rescue(Exception) do |error|
            failure = error
          end
          t.ensure do
            if failure.nil?
              output.set(result)
            else
              output.set(nil)
            end
          end
        end
        return output if retry_options.return_on_start
        output.get
        raise failure unless failure.nil?
        return output.get
      end


      # Retries the given method using an optional retry function and block of {RetryOptions}.
      #
      # @param (see #retry)
      #
      # @!visibility private
      def _retry(method_name, retry_function, block, args = NoInput.new)
        bail_if_external
        retry_options = Utilities::interpret_block_for_options(RetryOptions, block)
        _retry_with_options(lambda { self.send(method_name, *args) }, retry_function, retry_options)
      end


      # Retries the given method using an optional retry function and block of {RetryOptions}.
      #
      # @param method_name
      #   The activity to retry.
      #
      # @param retry_function
      #   The retry function to use
      #
      # @param args
      #   Arguments to send to the method named in the `method_name` parameter.
      #
      # @param block
      #   The {RetryOptions} to set.
      #
      def retry(method_name, retry_function, *args, &block)
        if retry_function.is_a? Fixnum
          retry_time = retry_function
          retry_function = lambda {|first_attempt, time_of_failure, attempt| retry_time}
        end
        future = self._retry(method_name, retry_function, block, args)
        Utilities::drill_on_future(future)
      end


      # @return The decision context for this client.
      def decision_context
        FlowFiber.current[:decision_context]
      end

    end
  end
end

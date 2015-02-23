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

# This file contains the externally visible parts of flow that are expected to be used by customers of flow.
module AWS
  module Flow
    module Core

      # A basic exception class with no context.
      class NoContextException < Exception; end

      # @param [Future] future
      #   Unused; defaults to **nil**.
      #
      # @param block
      #   The block of code to be executed when the task is run.
      #
      # @raise [NoContextException]
      #   If the current fiber does not respond to `Fiber.__context__`.
      #
      # @return [Future]
      #   The tasks result, which is a {Future}.
      #
      def task(future = nil, &block)
        fiber = ::Fiber.current
        raise NoContextException unless fiber.respond_to? :__context__
        context = fiber.__context__
        t = Task.new(nil, &block)
        task_context = TaskContext.new(:parent => context.get_closest_containing_scope, :task => t)
        context << t
        t.result
      end

      #
      # @param block
      #   The block of code to be executed when the daemon task is run.
      #
      # @return [Future]
      #   The tasks result, which is a {Future}.
      #
      # @raise [NoContextException]
      #   If the current fiber does not respond to `Fiber.__context__`.
      #
      def daemon_task(&block)
        fiber = ::Fiber.current
        raise NoContextException unless fiber.respond_to? :__context__
        context = fiber.__context__
        t = DaemonTask.new(nil, &block)
        task_context = TaskContext.new(:parent => context.get_closest_containing_scope, :task => t)
        context << t
        t.result
      end

      #
      # @param block
      #   The block of code to be executed when the external task is run.
      #
      # @return [nil]
      #
      # @raise [NoContextException]
      #   If the current fiber does not respond to `Fiber.__context__`.
      #
      def external_task(&block)
        fiber = ::Fiber.current
        raise NoContextException unless fiber.respond_to? :__context__
        context = fiber.__context__
        t = ExternalTask.new(:parent => context.get_closest_containing_scope, &block)
        task_context = TaskContext.new(:parent => context.get_closest_containing_scope, :task => t)
        context << t
        nil
      end

      # Creates a new error handler for asynchronous tasks.
      #
      # @param block
      #   A block that defines the {BeginRescueEnsure#begin}, {BeginRescueEnsure#rescue}, and {BeginRescueEnsure#ensure}
      #   methods.
      #
      # @return
      #   The result of the `begin` statement if there is no error; otherwise the value of the `return` statement.
      #
      # @raise [NoContextException]
      #   If the current fiber does not respond to `Fiber.__context__`.
      #
      def error_handler(&block)
        fiber = ::Fiber.current
        raise NoContextException unless fiber.respond_to? :__context__
        context = fiber.__context__
        begin_rescue_ensure = BeginRescueEnsure.new(:parent => context.get_closest_containing_scope)
        bge = BeginRescueEnsureWrapper.new(block, begin_rescue_ensure)
        context << bge
        context << begin_rescue_ensure
        begin_rescue_ensure
      end

      # @api private
      #
      def _error_handler(&block)
        error_handler(&block).result
      end


      # Waits for the passed-in function to complete, setting values for the provided futures when it does.
      #
      # @param function
      #   The function to wait for.
      #
      # @param [Array<Future>] futures
      #   A list of futures to provide values for when the function completes.
      #
      # @return [Array<Future>]
      #   A list of the set futures, in the order of being set.
      #
      def wait_for_function(function, *futures)
        wait_for_function_helper(nil, function, *futures)
      end

      # Blocks until *any* of the arguments are set.
      #
      # @param [Array<Future>] futures
      #   A list of futures to wait for. The function will return when at least one of these is set.
      #
      # @return [Array<Future>]
      #   A list of the set futures, in the order of being set.
      #
      def wait_for_any(*futures)
        wait_for_function(lambda {|result, future_list| result.length >= 1 }, futures)
      end

      # Blocks until *all* of the arguments are set.
      #
      # @param [Array<Future>] futures
      #   A list of futures to wait for. The function will return only when all of them are set.
      #
      # @return [Array<Future>]
      #   A list of the set futures, in the order of being set.
      #
      def wait_for_all(*futures)
        wait_for_function(lambda {|result, future_list| result.size == future_list.size}, futures)
      end

      # Blocks until *any* of the arguments are set.
      #
      # @param [Array<Future>] futures
      #   A list of futures to wait for. The function will return when at least one of these is set.
      #
      # @param [Integer] timeout
      #   The timeout value after which it will raise a Timeout::Error
      #
      # @return [Array<Future>]
      #   A list of the set futures, in the order of being set.
      #
      def timed_wait_for_any(timeout, *futures)
        timed_wait_for_function(timeout, lambda {|result, future_list| result.length >= 1 }, futures)
      end

      # Blocks until *all* of the arguments are set or until timeout expires
      #
      # @param [Array<Future>] futures
      #   A list of futures to wait for. The function will return only when all of them are set.
      #
      # @param [Integer] timeout
      #   The timeout value after which it will raise a Timeout::Error
      #
      # @return [Array<Future>]
      #   A list of the set futures, in the order of being set.
      #
      def timed_wait_for_all(timeout, *futures)
        timed_wait_for_function(timeout, lambda {|result, future_list| result.size == future_list.size}, futures)
      end

      # Waits for the passed-in function to complete, setting values for the provided futures when it does.
      #
      # @param function
      #   The function to wait for.
      #
      # @param [Array<Future>] futures
      #   A list of futures to provide values for when the function completes.
      #
      # @param [Integer] timeout
      #   The timeout value after which it will raise a Timeout::Error
      #
      # @return [Array<Future>]
      #   A list of the set futures, in the order of being set.
      #
      def timed_wait_for_function(timeout, function, *futures)
        wait_for_function_helper(timeout, function, *futures)
      end

      # Helper method to refactor away the common implementation of
      # wait_for_function and timed_wait_for_function.
      def wait_for_function_helper(timeout, function, *futures)
        futures.flatten!

        f = futures.select { |x| x.is_a?(ExternalFuture) }

        if f.size > 0 && f.size != futures.size
          raise ArgumentError, "The futures array must contain either all "\
            "objects of Future or all objects of ExternalFuture"
        end

        conditional = f.size == 0 ? FiberConditionVariable.new :
          ExternalConditionVariable.new

        return nil if futures.empty?
        result = futures.select(&:set?)
        return futures.find(&:set?)if function.call(result, futures)
        futures.each do |f|
          f.on_set do |set_one|
            result << set_one
            conditional.broadcast if function.call(result, futures)
          end
        end

        if conditional.is_a?(FiberConditionVariable)
          conditional.wait
        else
          conditional.wait(timeout)
          raise Timeout::Error.new unless function.call(result, futures)
        end
        result
      end

    end
  end
end

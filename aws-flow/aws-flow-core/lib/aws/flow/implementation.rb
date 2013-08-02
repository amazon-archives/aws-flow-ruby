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

# This file contains the externally visible parts of flow that are expected to be used by customers of flow
module AWS
  module Flow
    module Core
      class NoContextException < Exception; end

      # @param block
      #   The block of code to be executed when the task is run.
      #
      # @raise [NoContextException]
      #   If the current fiber does not respond to {#__context__}.
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
      #   If the current fiber does not respond to {#__context__}.
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
      #   If the current fiber does not respond to {#__context__}.
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


      #
      #
      # * *Args*    :
      #   - block -> a block, which is passed in a BeginRescueEnsureWrapper, and which will define the BeginRescueEnsure#begin, BeginRescueEnsure#rescue, and BeginRescueEnsure#ensure methods
      # * *Returns* :
      #   - The result of the begin statement, if there is no error, otherwise the value of the return statement
      # * *Raises* :
      #   - +NoContextException+ -> If the current fiber does not respond to #__context__
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

      # @param block
      #   A code block, which is passed within a {BeginRescueEnsureWrapper}, and which must define the
      #   {BeginRescueEnsure#begin}, {BeginRescueEnsure#rescue}, and {BeginRescueEnsure#ensure} methods.
      # @!visibility private
      def _error_handler(&block)
        error_handler(&block).result
      end


      def wait_for_function(function, *futures)
        conditional = FiberConditionVariable.new
        futures.flatten!
        return nil if futures.empty?
        result = futures.select(&:set?)
        return futures.find(&:set?)if function.call(result, futures)
        futures.each do |f|
          f.on_set do |set_one|
            result << set_one
            conditional.broadcast if function.call(result, futures)
          end
        end
        conditional.wait
        result
      end

      # Blocks until *any* of the arguments are set.
      #
      # @param [Array] futures
      #   A list of futures to wait for. The function will return when at least one of these is set.
      #
      # @return [Array]
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
      # @return [Array]
      #   A list of the set futures, in the order of being set.
      #
      def wait_for_all(*futures)
        wait_for_function(lambda {|result, future_list| result.size == future_list.size}, futures)
      end
    end
  end
end

##
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
##

# This module contains the Root of the heirarchy for calls into flow, the AsyncScope

module AWS
  module Flow
    module Core

      def gate_by_version(version, method, &block)
        if RUBY_VERSION.send(method, version)
          block.call
        end
      end

      # @!visibility private
      class AsyncScope
        attr_accessor :stackTrace, :root, :failure, :root_context

        def is_complete?
          @root_context.complete
        end

        def get_closest_containing_scope
          @root_error_handler
        end

        def cancel(error); @root_error_handler.cancel(error); end

        def initialize(&block)
          @root_context = RootAsyncScope.new



          # 1 for the function that skips frames
          # 1 for the create function
          # 1 for the the initialize of the backtrace

          # "./lib/aws/rubyflow/asyncBacktrace.rb:75:in `caller'"
          # "./lib/aws/rubyflow/asyncBacktrace.rb:21:in `create'"
          # "./lib/aws/rubyflow/asyncScope.rb:18:in `initialize'"

          @root_context.backtrace = AsyncBacktrace.create(nil, 3)
          @root_error_handler = BeginRescueEnsure.new(:parent => @root_context)
          begin
            @root_error_handler.begin lambda { block.call if ! block.nil? }
            @root_error_handler.rescue(Exception, lambda { |e| raise e })
          end
          @root_context << @root_error_handler
        end

        # Collects all the heirs of a task for use in async_stack_dump
        def get_heirs
          @root_error_handler.get_heirs
        end

        # Execute all queued tasks. If execution of those tasks results in addition of new tasks to the queue, execute
        # them as well.
        #
        # Unless there are external dependencies or bugs in the tasks to be executed, a single call to this method
        # performs the complete asynchronous execution.
        #
        # @note In the presence of external dependencies, it is expected that {AsyncScope#eventLoop} is called every
        # time after a change in the state in a dependency can unblock asynchronous execution.
        #
        def eventLoop
          #TODO Figure out when to raise Done raise "Done" if ! @root_task.alive?
          raise IllegalStateException, "Already complete" if is_complete?
          @root_context.eventLoop
          # TODO Does this need to be taken care of? It's supposed to protect
          # against people having errors that are classes, so like, passing
          # Exception into cancel. We might want to just catch that at the
          # entry point
          if @root_context.failure
            if @root_context.failure.respond_to? :message
              failure_message = @root_context.failure.message + "\n" +
                @root_context.failure.backtrace.join("\n")
              raise @root_context.failure, failure_message
            else
              raise @root_context.failure
            end
          end

          return is_complete?
        end

        def <<(task)
          @root_context << task
          task.parent = @root_context
        end
      end

      # @!visibility private
      class RootAsyncScope < FlowFiber

        attr_accessor :backtrace, :failure, :executor, :complete

        def initialize(options = {}, &block)
          @parent = options[:parent_context]
          @daemon = options[:daemon]
          @context = @parent
          @executor = AsyncEventLoop.new
          @task_queue = []
          @complete = false
          @task_queue << Task.new(context, &block) if block
        end

        # The only thing that should be removed from the RootAsyncScope is the
        # root BeginRescueEnsure, so upon removal we are complete
        def remove(task)
          @complete = true
        end

        # As with remove, the only thing that is under RootAsyncScope should be
        # the root BeginRescueEnsure, so upon failure we will be complete. Also
        # sets failure variable for later raising.
        def fail(task, error)
          @failure = error
          @complete = true
        end

        def <<(this_task)
          @executor << this_task
        end

        # Reture self, a RootAsyncScope is the closest containing scope
        def get_closest_containing_scope
          self
        end

        # Call out to the AsyncEventLoop
        def eventLoop
          @executor.executeQueuedTasks
        end


        private
        DELEGATED_METHODS = [:push, :<<, :enq, :empty?, :length, :size, :delete, :shift]

        def method_missing(method_name, *args)
          if DELEGATED_METHODS.include? method_name
            @executor.send(method_name, *args)
          else
            super
          end
        end
      end

      # @!visibility private
      class AsyncEventLoop

        def initialize
          @tasks = []
        end

        def remove(task)
          @tasks.delete(task)
        end
        # TODO Make sure that it's okay to fail from the AsyncEventLoop, and that
        # this is the correct behavior
        def fail(task, error)
          raise error
        end
        def <<(task)
          @tasks << task

        end


        # TODO should this be synchronized somehow?

        # Actually executes the eventLoop
        def executeQueuedTasks
          until @tasks.empty?
            task = @tasks.shift
            task.resume if task.alive?
          end
        end
      end

    end
  end
end

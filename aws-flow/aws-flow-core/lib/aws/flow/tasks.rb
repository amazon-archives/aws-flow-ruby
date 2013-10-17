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

# Contains all the task methods, which allow arbitrary blocks of code to be run asynchronously.

module AWS
  module Flow
    module Core
      # @api private
      class Task < FlowFiber
        attr_reader :result, :block
        attr_accessor :backtrace, :__context__, :parent

        # Creates a new task.
        #
        # @param __context__
        #   A task needs a reference to the __context__ that created it so that when the "task" macro is called it can
        #   find the __context__, which the new task should be added to.
        #
        # @param block
        #   A block of code that will be run by the task.
        #
        # @api private
        def initialize(__context__, &block)
          @__context__ = __context__
          @result = Future.new
          @block = block

          # Is the task alive?
          #
          # @return Boolean
          #   true if the task is alive and has not been canceled.
          #
          # @api private
          def alive?
            super && !@cancelled
            #!!@alive# && !@cancelled
          end

          # Retrieves the executor for this task.
          #
          # @return
          #   The executor for this task.
          #
          # @api private
          def executor
            @__context__.executor
          end

          super() do
            begin
              # Not return because 1.9 will freak about local jump problems if you
              # try to return, as this is inside a block.
              next if @cancelled
              @result.set(lambda(&block).call)
              next if @cancelled
              @__context__.remove(self)
            rescue Exception => e
              if @backtrace != e
                backtrace = AsyncBacktrace.create_from_exception(@backtrace, e)
                e.set_backtrace(backtrace.backtrace) if backtrace
              end
              @__context__.fail(self, e)
            ensure
            end
          end
        end

        #
        # Passes all `get_heirs` calls to the class that is holding the context,
        # to ensure uniform handling of `get_heirs`.
        #
        # @api private
        def get_heirs
          @__context__.get_heirs
        end

        # Will always be false. Provides a common API for BeginRescueEnsure to
        # ensure they are maintaining their nonDaemonHeirsCount correctly.
        #
        # @api private
        def is_daemon?
          return false
        end

        # Used by {Future#signal} to schedule the task for re-evaluation.
        #
        # This will simply add the task back to the list of things to be run in the parent's event loop.
        #
        # @api private
        def schedule
          @__context__ << self
        end

        # Prevents the execution of this particular task, if possible.
        #
        # @param error
        #   The error that is the cause of the cancellation.
        #
        # @api private
        def cancel(error)
          @cancelled = true
          @__context__.remove(self)
        end

        # Fails the given task with the specified error.
        #
        # @param error
        #   The error that is the cause of the failure.
        #
        # @api private
        def fail(this_task, error)
          @__context__.fail(this_task, error)
        end
        # def fail(this_task, error)
        #   raise error
        # end

        # Adds a task to this task's context.
        #
        # @param this_task
        #   The task to add.
        #
        # @api private
        def <<(this_task)
          @__context__.parent << this_task
        end

        # Removes a task from this task's context.
        #
        # @param this_task
        #   The task to remove.
        #
        # @api private
        def remove(this_task)
          @__context__.remove(this_task)
        end
      end

      # Represents a task that might not execute. Similar to a regular task in
      # all functions except that it is not assured a chance to execute. Whereas
      # a begin/run/execute block cannot be closed while there are still
      # `nonDaemonHeirs`, a DaemonTask can enter the closed state with daemon
      # heirs, making them essentially unrunnable.
      #
      # @api private
      class DaemonTask < Task
        # @api private
        def is_daemon?
          return true
        end
      end

      # Used to bridge asynchronous execution to external asynchronous APIs or
      # events. It is passed a block, like so:
      #
      #     external_task do |t|
      #       t.cancellation_handler { |h, cause| h.fail(cause) }
      #       t.initiate_task { |h| trace << :task_started; h.complete; }
      #     end
      #
      # The {ExternalTask#initiate_task} method is expected to call an external
      # API and return without blocking.  Completion or failure of the external
      # task is reported through ExternalTaskCompletionHandle, which is passed
      # into the initiate_task and cancellation_handler blocks. The cancellation
      # handler, defined in the same block as the initiate_task, is used to
      # report the cancellation of the external task.
      #
      # @api private
      class ExternalTask < FlowFiber
        attr_reader :block
        attr_accessor :cancelled, :inCancellationHandler, :parent, :backtrace, :__context__


        # Will always be false, provides a common API for BeginRescueEnsure's to
        # ensure they are maintaining their nonDaemonHeirsCount correctly.
        #
        # @api private
        def is_daemon?
          false
        end

        #
        # Passes the get_heirs calls to the context, to ensure uniform handling
        # of get_heirs
        #
        # @api private
        def get_heirs
          @__context__.get_heirs
        end

        # @api private
        def initialize(options = {}, &block)
          @inCancellationHandler = false
          @block = block
          # TODO: What should the default value be?
          @parent = options[:parent]
          @handle = ExternalTaskCompletionHandle.new(self)
          block.call(self)
        end

        # This method is here because the way we create external tasks is a
        # little tricky. If the parent isn't passed in on construction(as is the
        # case with the external_task function), then the parent will only be
        # set after ExternalTask#initialize is called. We'd prefer to set it in
        # initialize, however, the backtrace relies on the parent's backtrace,
        # and so we cannot do that. Instead, we use this method to lazily create
        # it when it is called. The method itself simply sets the backtrace to
        # the make_backtrace of the parent's backtrace, if the backtrace is not
        # already set, and will otherwise simply return the backtrace
        #
        # @api private
        def backtrace
          @backtrace ||= make_backtrace(@parent.backtrace)
        end

        # Add a task that removes yourself, and pass it through the parents executor.
        #
        # @api private
        def remove_from_parent
          @__context__.executor << FlowFiber.new { @parent.remove(self) }
        end

        # Add a task that fails yourself with the suppiled error, and pass it
        # through the parents executor.
        #
        # @api private
        def fail_to_parent(error)
          @__context__.executor << FlowFiber.new { @parent.fail(self, error) }
        end


        # Part of the interface provided by fiber, has to overridden to properly
        # reflect that an external tasks alive-ness relies on its
        # ExternalTaskCompletionHandle.
        #
        # @api private
        def alive?
          ! @handle.completed
        end

        # @api private
        def cancel(cause)
          return if @cancelled
          return if @handle.failure != nil || @handle.completed
          @cancelled = true
          if @cancellation_task != nil
            begin
              @inCancellationHandler = true
              @cancellation_task.call(cause)
            rescue Exception => e
              if ! self.backtrace.nil?
                backtrace = AsyncBacktrace.create_from_exception(@backtrace, e)
                e.set_backtrace(backtrace.backtrace) if backtrace
              end
              @handle.failure = e
            ensure
              @inCancellationHandler = false
              if ! @handle.failure.nil?
                fail_to_parent(@handle.failure)
              elsif @handle.completed
                remove_from_parent
              end
            end
          else
            remove_from_parent
          end
        end

        # Store the passed-in cancellation handler block for later reference.
        #
        # @api private
        def cancellation_handler(&block)
          @cancellation_task = lambda { |cause| block.call(@handle, cause) }
        end

        # Store the passed-in block for later.
        #
        # @api private
        def initiate_task(&block)
          @initiation_task = lambda { block.call(@handle) }
        end

        # From the interface provided by fiber, will execute the external task.
        #
        # @api private
        def resume
          return if @cancelled
          begin
            @cancellation_handler = @initiation_task.call
          rescue Exception => e
            backtrace = AsyncBacktrace.create_from_exception(self.backtrace, e)
            e.set_backtrace(backtrace.backtrace) if backtrace
            @parent.fail(self, e)
          end
        end
      end

      # Used to complete or fail an external task initiated through
      # ExternalTask#initiate_task, and thus handles the logic of what to do
      # when the external task is failed.
      #
      # @api private
      class ExternalTaskCompletionHandle
        attr_accessor :completed, :failure, :external_task

        # @api private
        def initialize(external_task)
          @external_task = external_task
        end

        # Merges the backtrace, sets the @failure, and then fails the task from
        # the parent.
        #
        # @param error
        #   The exception to fail on.
        #
        # @raise IllegalStateException
        #   Raises if failure hasn't been set, or if the task is already completed.
        #
        # @api private
        def fail(error)
          if ! @failure.nil?
            raise IllegalStateException, "Invalid ExternalTaskCompletionHandle"
          end
          if @completed
            raise IllegalStateException, "Already completed"
          end
          #TODO Might want to flip the logic to only alert if variable is set
          if @stacktrace.nil?
            if ! @external_task.backtrace.nil?
              backtrace = AsyncBacktrace.create_from_exception(@external_task.backtrace, error)
              error.set_backtrace(backtrace.backtrace) if backtrace
            end
          end
          @failure = error
          if ! @external_task.inCancellationHandler
            @external_task.fail_to_parent(error)
          end
        end

        # Sets the task to complete, and removes it from its parent.
        #
        # @raise IllegalStateException
        #   If the failure hasn't been set, or if the task is already completed.
        #
        # @api private
        def complete
          if ! failure.nil?
            raise IllegalStateException, ""
          end

          if @completed
            raise IllegalStateException, "Already Completed"
          end
          @completed = true
          @external_task.remove_from_parent if ! @external_task.inCancellationHandler
        end
      end

      # Holds some metadata for tasks and stores the parent link for tasks.  It
      # separates some of the concerns between tasks and what they have to know
      # to follow back up the chain.
      #
      # All the methods here will simply delegate calls, either up to the
      # parent, or down to the task.
      #
      # @api private
      class TaskContext

        attr_accessor :daemon, :parent, :backtrace, :cancelled

        # @api private
        def initialize(options = {})
          @parent = options[:parent]
          @task = options[:task]
          @task.__context__ = self
          @non_cancelling = options[:non_cancelling]
          @daemon = options[:daemon]
        end

        # @api private
        def get_closest_containing_scope
          @task
          # @ parent
        end

        # @api private
        def alive?
          @task.alive?
        end

        # @api private
        def executor
          @parent.executor
        end

        # @api private
        def get_heirs
          str = "I am a #{@task.class}
        and my block looks like #{@task.block}"
        end

        # @api private
        def fail(this_task, error)
          @parent.fail(this_task, error)
        end

        # @api private
        def remove(thread)
          @parent.remove(thread)
        end

        # @api private
        def cancel(error_type)
          @task.cancelled = true
          @parent.cancel(self)
        end

        # @api private
        def <<(task)
          @parent << task
        end
      end
    end
  end
end

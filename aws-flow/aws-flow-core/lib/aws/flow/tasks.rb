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

# Contains all the task methods, which allow arbitrary blocks of code to be run asynchronously

module AWS
  module Flow
    module Core
      class Task < FlowFiber
        attr_reader :result, :block
        attr_accessor :backtrace, :__context__, :parent

        # A Task needs a reference to the __context__ that created it so that when
        # the "task" macro is called it can find the __context__ which the new Task
        # should be added to.
        def initialize(__context__, &block)
          @__context__ = __context__
          @result = Future.new
          @block = block

          def alive?
            super && !@cancelled
            #!!@alive# && !@cancelled
          end

          def executor
            @__context__.executor
          end

          super() do
            begin
              # Not return because 1.9 will freak about local jump problems if you
              # try to return, as this is inside a block
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
        #  Passes the get_heirs calls to the context, to ensure uniform handling of
        #  get_heirs
        #
        def get_heirs
          @__context__.get_heirs
        end

        #
        #   returns true/false, depending on if we are in a Daemon Task or not
        #
        def is_daemon?
          return false
        end

        #
        # Used by Future#signal, in order to schedule the item for re-evaluation. This
        # will simply add it back to the parent, adding the task back to the list of
        # things to ber un in the event loop
        #
        def schedule
          @__context__ << self
        end



        #
        #
        # * *Args*    :
        #   - error -> Error that is the cause of the cancellation
        # * *Returns* :
        #   - N/A
        # * *Raises* :
        #   - +N/A+ ->
        # cancel will prevent the execution of this particular task, if possible
        def cancel(error)
          @cancelled = true
          @__context__.remove(self)
        end

        def fail(this_task, error)
          @__context__.fail(this_task, error)
        end
        # def fail(this_task, error)
        #   raise error
        # end

        def <<(this_task)
          @__context__.parent << this_task
        end

        def remove(this_task)
          @__context__.remove(this_task)
        end


      end



      # Similar to a regular Task in all functioning except that it is not assured a chance to execute. Whereas a
      # begin/run/execute block cannot be closed out while there are still nonDaemonHeirs, it can happily entered the
      # closed state with daemon heirs, making them essentially unrunnable.
      class DaemonTask < Task
        def is_daemon?
          return true
        end
      end



      # External Tasks are used to bridge asynchronous execution to external asynchronous APIs or events. It is passed a block, like so
      #
      #   external_task do |t|
      #     t.cancellation_handler { |h, cause| h.fail(cause) }
      #     t.initiate_task { |h| trace << :task_started; h.complete; }
      #   end
      #
      # The ExternalTask#initiate_task method is expected to call an external API and
      # return without blocking. Completion or failure of the externalTask is reported
      # through ExternalTaskCompletionHandle, which is passed into the initiate_task and cancellation_handler blocks. The cancellation handler, defined in the
      # same block as the initiate_task, is used to report the cancellation of the
      # external task.
      class ExternalTask < FlowFiber
        attr_reader :block
        attr_accessor :cancelled, :inCancellationHandler, :parent, :backtrace, :__context__


        # Will always be false, provides a common api for BRE's to ensure they are
        # maintaining their nonDaemonHeirsCount correctly
        def is_daemon?
          false
        end

        #
        #  Passes the get_heirs calls to the context, to ensure uniform handling of
        #  get_heirs
        #
        def get_heirs
          @__context__.get_heirs
        end


        def initialize(options = {}, &block)
          @inCancellationHandler = false
          @block = block
          # TODO: What should the default value be?
          @parent = options[:parent]
          @handle = ExternalTaskCompletionHandle.new(self)
          block.call(self)
        end

        # This method is here because the way we create ExternalTasks is a little
        # tricky - if the parent isn't passed in on construction(as is the case with
        # the external_task function), then the parent will only be set after
        # ExternalTask#initialize is called. We'd prefer to set it in the initiailze,
        # however, the backtrace relies on the parent's backtrace, and so we cannot do
        # that. Instead, we use this method to lazily create it, when it is
        # called. The method itself simply sets the backtrace to the the
        # make_backtrace of the parent's backtrace, if the backtrace is not already
        # set, and will otherwise simply return the backtrace
        def backtrace
          @backtrace ||= make_backtrace(@parent.backtrace)
        end

        # Add a task which removes yourself, and pass it through the parents executor
        def remove_from_parent
          @__context__.executor << FlowFiber.new { @parent.remove(self) }
        end

        # Add a task which fails yourself with the suppiled error, and pass it through
        # the parents executor
        def fail_to_parent(error)
          @__context__.executor << FlowFiber.new { @parent.fail(self, error) }
        end


        # Part of the interface provided by Fiber, has to overridden to properly
        # reflect that an ExternalTasks alive-ness relies on it's
        # ExternalTaskCompletionHandle
        def alive?
          ! @handle.completed
        end


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

        # Store the cancellation handler block passed in for later reference
        def cancellation_handler(&block)
          @cancellation_task = lambda { |cause| block.call(@handle, cause) }
        end

        # Store the block passed in for later
        def initiate_task(&block)
          @initiation_task = lambda { block.call(@handle) }
        end

        # From the interface provided by Fiber, will execute the External Task
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

      class DaemonExternalTask < ExternalTask
        def is_daemon?
          return true
        end
      end


      # Used to complete or fail an external task initiated through
      # ExternalTask#initiate_task, and thus handles the logic of what to do when the
      # external task is failed.
      class ExternalTaskCompletionHandle
        attr_accessor :completed, :failure, :external_task

        def initialize(external_task)
          @external_task = external_task
        end

        #
        #
        # * *Args*    :
        #   - error -> The exception to fail on
        # * *Returns* :
        #   -
        # * *Raises* :
        #   - +IllegalStateException+ -> Raises if failure hasn't been set, or if the task is already completed
        #
        # Will merge the backtrace, set the @failure, and then fail the task from the parent
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

        #
        #
        # * *Args*    :
        #   - N/A ->
        # * *Returns* :
        #   -
        # * *Raises* :
        #   - +IllegalStateException+ -> If the failure hasn't been set, or if the task is already completed
        # Set's the task to complete, and removes it from it's parent
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

      # TaskContext is the class that holds some meta-information for tasks, and
      # which stores the parent link for tasks. It seperates some of the concerns
      # between tasks and what they have to know to follow back up the chain.
      #
      #  All the methods here will simply delegate calls, either up to the parent, or down to the task
      class TaskContext

        attr_accessor :daemon, :parent, :backtrace, :cancelled

        def initialize(options = {})
          @parent = options[:parent]
          @task = options[:task]
          @task.__context__ = self
          @daemon = options[:daemon]
        end

        def get_closest_containing_scope
          @parent
        end

        def alive?
          @task.alive?
        end

        def executor
          @parent.executor
        end

        def get_heirs
          str = "I am a #{@task.class}
        and my block looks like #{@task.block}"
        end

        def fail(this_task, error)
          @parent.fail(this_task, error)
        end

        def remove(fiber)
          @parent.remove(fiber)
        end

        def cancel(error_type)
          @task.cancelled = true
          @parent.cancel(self)
        end

        def <<(task)
          @parent << task
        end

      end
    end
  end
end

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

# This file contains our Future implementation, which allows us to have asynchronous, blocking promises

module AWS
  module Flow
    module Core
      class AlreadySetException < Exception; end

      # Represents the result of an asynchronous computation. Methods are
      # provided to:
      #
      # * retrieve the result of the computation, once it is complete ({Future#get}).
      # * check if the computation is complete ({Future#set?})
      # * execute a block when computation is complete ({Future#on_set})
      #
      # The result of a Future can only be retrieved when the computation has
      # completed.  {Future#get} blocks execution, if necessary, until the
      # Future is ready.  This is okay: because it will block that fiber,
      # another fiber will start executing.
      #
      class Future

        # Sets the value of the {Future}, and notifies all of the fibers that
        # tried to call {#get} when this future wasn't ready.
        # @api private
        def set(result=nil)
          raise AlreadySetException if @set
          @set = true
          @result = result
          @conditional.broadcast if @conditional
          @listeners.each { |b| b.call(self) } if @listeners
          self
        end

        # Is the object is an AWS Flow future? AWS Flow futures *must* have a
        # {#get} method.
        # @api private
        def is_flow_future?
          true
        end

        # Blocks if future is not set. Returns the result of the future once
        # {#set} is true.
        #
        # @return
        #   The result of the future.
        #
        # @raise CancellationError
        #   when the task is cancelled.
        def get
          until @set
            @conditional ||= FiberConditionVariable.new
            @conditional.wait
          end
          @result
        end

        # @return
        #   true if the {Future} has been set.
        def set?
          @set
        end

        # Unsets the future.
        #
        # @api private
        def unset
          @set = nil
          @result = nil
        end

        # Adds a callback, passed in as a block, which will fire when the future
        # is set.
        def on_set(&block)
          @listeners ||= []
          # TODO probably want to use lambda here
          @listeners << block
        end
      end

      # Represents a fiber condition variable.
      # Based on the Ruby core source:
      # https://github.com/ruby/ruby/blob/trunk/lib/thread.rb
      # @api private
      class FiberConditionVariable
        #
        # Creates a new ConditionVariable
        #
        # @api private
        def initialize
          @waiters = []
        end

        # Have the current fiber wait on this condition variable, and wake up
        # when the FiberConditionVariable is signaled/broadcasted.
        #
        # @api private
        def wait
          fiber = ::Fiber.current
          @waiters << fiber
          Fiber.yield
          self
        end

        #
        # Wakes up the first fiber in line waiting for this lock.
        #
        # @api private
        def signal
          t = @waiters.shift
          t.schedule if t && t.alive?
          self
        end

        #
        # Wakes up all fibers waiting for this lock.
        #
        # @api private
        def broadcast
          signal until @waiters.empty?
          self
        end
      end
    end
  end
end

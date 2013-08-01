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

      # A Future represents the result of an asynchronous computation. Methods are
      # provided to check if the computation is complete(Future#set), to wait for
      # its completion(Future#wait), and to retrieve the result of the
      # computation(Future#get). The result can only be retrieved using method get
      # when the computation has completed, blocking if necessary until it is
      # ready. This is okay, however, because it will block that Fiber, and
      # another Fiber will start executing
      class Future

        # Sets the value of the future, and notifies all of the Fibers that tried
        # to get when this future wasn't ready.
        def set(result=nil)
          raise AlreadySetException if @set
          @set = true
          @result = result
          @conditional.broadcast if @conditional
          @listeners.each { |b| b.call(self) } if @listeners
          self
        end

        # Blocks if Future is not set
        # raises CancellationError when task is cancelled
        def get
          until @set
            @conditional ||= FiberConditionVariable.new
            @conditional.wait
          end
          @result
        end

        def set?
          @set
        end

        def unset
          @set = nil
          @result = nil
        end

        # Add a callback, block, which will fire when the future is set
        def on_set(&block)
          @listeners ||= []
          # TODO probably want to use lambda here
          @listeners << block
        end
      end

      # Based on the ruby core source:
      # https://github.com/ruby/ruby/blob/trunk/lib/thread.rb
      class FiberConditionVariable
        #
        # Creates a new ConditionVariable
        #
        def initialize
          @waiters = []
        end


        # Have the current fiber wait on this condition variable, and wake up when
        # the FiberConditionVariable is signalled/broadcaster
        def wait
          fiber = ::Fiber.current
          @waiters << fiber
          Fiber.yield
          self
        end

        #
        # Wakes up the first fiber in line waiting for this lock.
        #
        def signal
          t = @waiters.shift
          t.schedule if t && t.alive?
          self
        end

        #
        # Wakes up all fibers waiting for this lock.
        #
        def broadcast
          signal until @waiters.empty?
          self
        end
      end
    end
  end
end

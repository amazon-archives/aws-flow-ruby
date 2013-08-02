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

# This file contains our implementation of fibers for 1.8
module AWS
  module Flow
    module Core
      require 'fiber'
      class FlowFiber < Fiber
        def initialize(*args)
          ObjectSpace.define_finalizer(self, self.class.finalize(self.object_id))
          super(args)
        end
        class << self
          attr_accessor :local_variables
        end
        @local_variables = Hash.new {|hash, key| hash[key] = {}}
        def self.finalize(obj_id)
          proc { FlowFiber.local_variables.delete(obj_id) }
        end
        def self.[](index)
          self.local_variables[index]
        end
        def self.[]=(key, value)
          self.local_variables[key] = value
        end

        # Will unset all the values for ancestors of this fiber, assuming that
        # they have the same value for key. That is, they will unset upwards until
        # the first time the value stored at key is changed
        def self.unset(current_fiber, key)
          current_value = FlowFiber[current_fiber.object_id][key]
          parent = FlowFiber[current_fiber.object_id][:parent]
          ancestor_fibers = []
          while parent != nil
            ancestor_fibers << parent
            parent = FlowFiber[parent.object_id][:parent]
          end
          ancestor_fibers.each do |fiber|
            FlowFiber[fiber.object_id].delete(key) if FlowFiber[fiber.object_id][key] == current_value
          end
          FlowFiber[current_fiber.object_id].delete(key)
        end

        def initialize
          # Child fibers should inherit their parents FiberLocals
          FlowFiber[Fiber.current.object_id].each_pair do |key, val|
            FlowFiber[self.object_id][key] = val
          end
          FlowFiber[self.object_id][:parent] = Fiber.current
          super
        end

        def [](key)
          FlowFiber[self.object_id][key]
        end
        def []=(key, value)
          FlowFiber[self.object_id][key] = value
        end

      end

    end
  end
end

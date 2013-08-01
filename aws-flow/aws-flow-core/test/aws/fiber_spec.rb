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

describe Fiber do
  it "makes sure that Fibers behave the same" do
    root_fiber = Fiber.current
    scope = AsyncScope.new() do
      p "yay"
    end
    Fiber.current.should == root_fiber
  end
  it "makes sure that FiberLocal variables inherit correctly" do
    scope = AsyncScope.new() do
      FlowFiber.current[:test] = 5
      FlowFiber.current[:test].should == 5
      task do
        FlowFiber.current[:test].should == 5
      end
    end
    scope.eventLoop
  end

  it "makes sure that fiber values get unset correctly" do
    first_scope = AsyncScope.new do
      FlowFiber.current[:test] = 5
      FlowFiber.current[:test].should == 5
    end
    second_scope = AsyncScope.new do
      @current_thread = FlowFiber.current
      FlowFiber.current[:test] = 5
      FlowFiber.current[:test].should == 5
      task do
        task do
          FlowFiber.unset(FlowFiber.current, :test)
        end
      end
      FlowFiber.current[:test]
    end
    first_scope.eventLoop
    second_scope.eventLoop
    FlowFiber[@current_thread][:test].should == nil
  end

  it "makes sure that fibers pass by reference" do
    class CustomObject
      attr_accessor :this_stuff
    end
    x = CustomObject.new
    @trace = []
    first_fiber = FlowFiber.new do
      x.this_stuff = 6
      Fiber.yield
      @trace << x.this_stuff
    end

    second_fiber = FlowFiber.new do
      x.this_stuff = x.this_stuff * 2
      @trace << x.this_stuff
    end
    first_fiber.resume
    second_fiber.resume
    first_fiber.resume
    @trace.select{|x| x == 12}.length == 2

  end
end

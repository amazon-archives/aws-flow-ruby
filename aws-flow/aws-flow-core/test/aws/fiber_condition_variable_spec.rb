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

describe FiberConditionVariable do
  let(:condition) { FiberConditionVariable.new }
  let(:trace) { [] }

  it "blocks a Fiber and wakes it up on signal" do
    scope = AsyncScope.new do
      task do
        trace << :first
        condition.wait
        # TODO technically this should only be checked in a TaskContext test;
        # a Future test would just make sure that this comes after :second
        trace << :fourth
      end

      task do
        trace << :second
        condition.signal
        trace << :third
      end
    end

    scope.eventLoop
    trace.should == [:first, :second, :third, :fourth]
  end

  it "blocks multiple Fibers and wakes them up on signal" do
    scope = AsyncScope.new do
      task do
        trace << :first
        condition.wait
        # TODO technically this should only be checked in a TaskContext test;
        # a Future test would just make sure that this comes after :second
        trace << :sixth
      end

      task do
        trace << :second
        condition.wait
        trace << :seventh
      end

      task do
        trace << :third
        condition.signal
        trace << :fourth
        condition.signal
        trace << :fifth
      end
    end

    scope.eventLoop
    trace.should == [:first, :second, :third, :fourth, :fifth, :sixth, :seventh]
  end

  it "blocks a Fiber and wakes it up on broadcast" do
    scope = AsyncScope.new do
      task do
        trace << :first
        condition.wait
        # TODO technically this should only be checked in a TaskContext test;
        # a Future test would just make sure that this comes after :second
        trace << :fourth
      end

      task do
        trace << :second
        condition.broadcast
        trace << :third
      end
    end
    scope.eventLoop
    trace.should == [:first, :second, :third, :fourth]
  end

  it "blocks multiple Fibers and wakes them up on broadcast" do
    scope = AsyncScope.new do
      task do
        trace << :first
        condition.wait
        # TODO technically this should only be checked in a TaskContext test;
        # a Future test would just make sure that this comes after :second
        trace << :fifth
      end

      task do
        trace << :second
        condition.wait
        trace << :sixth
      end

      task do
        trace << :third
        condition.broadcast
        trace << :fourth
      end
    end

    scope.eventLoop
    trace.should == [:first, :second, :third, :fourth, :fifth, :sixth]
  end



  [:broadcast, :signal].each do |method_to_test|
    it "ensures that multiple broadcasts cannot cause multiple runs of fibers" do
      trace = []
      scope = AsyncScope.new do
        task do
          trace << :first
          condition.wait
          trace << :fourth
        end
        task do
          trace << :second
          condition.send method_to_test
          condition.send method_to_test
          trace << :third
        end
      end
      scope.eventLoop
      trace.should == [:first, :second, :third, :fourth]
    end

    it "ensures that calling #{method_to_test} with no waiters doesn't error" do
      condition.send method_to_test
    end

    it "ensures that #{method_to_test}ing a task that is dead has no effect" do
      condition = FiberConditionVariable.new
      trace = []
      scope = AsyncScope.new
      a = Task.new(scope.root_context) do
        trace << :first
        condition.wait
        trace << :should_never_be_hit
      end
      b = Task.new(scope.root_context) do
        trace << :second
        condition.send(method_to_test)
        trace << :third
      end
      a.resume
      a.should_receive(:alive?).and_return(false)
      b.resume
      trace.should == [:first, :second, :third]
    end
  end
end

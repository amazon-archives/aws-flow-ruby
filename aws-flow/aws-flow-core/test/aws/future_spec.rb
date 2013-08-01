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

describe Future do
  let(:future) { Future.new }
  let(:trace) { [] }
  it "throws an exception when set multiple times" do
    future.set(:foo)
    expect { future.set(:bar) }.to raise_error AlreadySetException
  end

  it "returns the value which was set" do
    value = :foo
    future.set(value)
    future.get.should == value
  end

  it "blocks a task until a value is ready" do
    scope = AsyncScope.new do
      task do
        trace << :first
        future.get
        # TODO technically this should only be checked in a TaskContext test;
        # a Future test would just make sure that this comes after :second
        trace << :fourth
      end

      task do
        trace << :second
        future.set(:foo)
        trace << :third
      end
    end

    scope.eventLoop
    trace.should == [:first, :second, :third, :fourth]
  end

  it "blocks multiple Fibers until a value is ready" do
    scope = AsyncScope.new do
      task do
        trace << :first
        future.get
        trace << :fifth
      end

      task do
        trace << :second
        future.get
        trace << :sixth
      end

      task do
        trace << :third
        future.set(:foo)
        trace << :fourth
      end
    end
    scope.eventLoop
    trace.should == [:first, :second, :third, :fourth, :fifth, :sixth]
  end

  it "supports wait_for_any" do
    scope = AsyncScope.new do
      f1 = Future.new
      f2 = Future.new
      f3 = Future.new
      trace << :before_task
      task do
        trace << :before_set
        f2.set("bar")
        trace << :after_set
      end
      trace << :before_wait

      f = Future.wait_for_any(f1, f2, f3).first
      trace << :after_wait
      f.should == f2
      f.get.should == "bar"
    end
    trace.length.should == 0
    scope.eventLoop
    trace.should == [:before_task, :before_wait, :before_set, :after_set, :after_wait]
  end

  it "supports wait_for_any with all set" do
    scope = AsyncScope.new do
      f1 = Future.new
      f2 = Future.new
      f3 = Future.new
      trace << :before_task
      task do
        trace << :before_set
        f2.set("bar")
        f1.set("baz")
        f3.set("foo")
        trace << :after_set
      end
      trace << :before_wait
      r1, r2, r3 = Future.wait_for_any(f1, f2, f3)
      trace << :after_wait
      r1.should == f2
      r1.get.should == "bar"
      r2.should == f1
      r2.get.should == "baz"
      r3.should == f3
      r3.get.should == "foo"
    end
    trace.length.should == 0
    scope.eventLoop
    trace.should == [:before_task, :before_wait, :before_set, :after_set, :after_wait]
  end

  it "supports wait_for_all" do
    scope = AsyncScope.new do
      f1 = Future.new
      f2 = Future.new
      f3 = Future.new
      trace << :before_task
      task do
        trace << :before_f2_set
        f2.set("bar")
        task do
          trace << :before_f1_set
          f1.set("baz")
          task do
            trace << :before_f3_set
            f3.set("foo")
            trace << :after_set
          end
        end
      end
      trace << :before_wait
      r1, r2, r3 = Future.wait_for_all(f1, f2, f3)
      trace << :after_wait
      r1.should == f2
      r1.get.should == "bar"
      r2.should == f1
      r2.get.should == "baz"
      r3.should == f3
      r3.get.should == "foo"
    end
    trace.length.should == 0
    scope.eventLoop
    trace.should == [:before_task, :before_wait, :before_f2_set, :before_f1_set, :before_f3_set, :after_set, :after_wait]
  end

  it "supports wait_for_all with no futures" do
    scope = AsyncScope.new do
      task do
        wait_for_all
      end
    end
    scope.eventLoop
    scope.is_complete?.should == true
  end

  it "supports wait_for_any with no futures" do
    scope = AsyncScope.new do
      task do
        wait_for_any
      end
    end
    scope.eventLoop
    scope.is_complete?.should == true
  end

  it "supports wait_for_all with a set future" do
    scope = AsyncScope.new do
      future = Future.new.set
      wait_for_all(future)
    end
    scope.eventLoop
    scope.is_complete?.should == true
  end

  it "supports wait_for_any with a set future" do
    scope = AsyncScope.new do
      future = Future.new.set
      wait_for_any(future)
    end
    scope.eventLoop
    scope.is_complete?.should == true
  end

  it "supports wait_for_all with one set future and one not set" do
    future2 = Future.new
    scope = AsyncScope.new do
      future = Future.new.set
      wait_for_all(future, future2)
    end
    scope.eventLoop
    future2.set
    scope.eventLoop
    scope.is_complete?.should == true
  end

end

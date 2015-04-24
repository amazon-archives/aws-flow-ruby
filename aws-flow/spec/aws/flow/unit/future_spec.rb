require 'spec_helper'

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

describe ExternalFuture do
  let(:future) { ExternalFuture.new }
  let(:trace) { [] }
  context "#set" do
    it "throws an exception when set multiple times" do
      future.set(:foo)
      expect { future.set(:bar) }.to raise_error AlreadySetException
    end
  end

  context "#get" do

    it "returns the value which was set" do
      value = :foo
      future.set(value)
      future.get.should == value
    end

    it "blocks a thread until a value is ready" do
      future = ExternalFuture.new

      t1 = Thread.new { future.get }
      Test::Unit::wait("run", 1, t1)
      t1.status.should == "sleep"
      future.set
      Test::Unit::wait("run", 1, t1)
      t1.status.should == false
    end

    it "works with an already set future" do
      future = ExternalFuture.new.set
      future.get
      # future.get will be a non blocking call if it is already set. If not,
      # then the next line will never be executed
      future.set?.should == true
    end

    it "blocks multiple threads until a value is ready" do
      future = ExternalFuture.new
      t1 = Thread.new { future.get }
      t2 = Thread.new { future.get }
      t3 = Thread.new { future.get }

      Test::Unit::wait("run", 1, t1, t2, t3)

      t1.status.should == "sleep"
      t2.status.should == "sleep"
      t3.status.should == "sleep"

      future.set

      Test::Unit::wait("run", 1, t1, t2, t3)

      t1.status.should == false
      t2.status.should == false
      t3.status.should == false
    end

    context "#timeout" do
      it "raises an exception if timeout occurs and leaves future unset" do
        future = ExternalFuture.new
        expect{future.get(1)}.to raise_error(Timeout::Error)
        future.set?.should == false
      end

      it "raises an exception if timeout occurs multiple times" do
        future = ExternalFuture.new
        expect{future.get(1)}.to raise_error(Timeout::Error)
        expect{future.get(1)}.to raise_error(Timeout::Error)
        expect{future.get(1)}.to raise_error(Timeout::Error)
      end

      it "nil timeout will block until the future is set" do
        future = ExternalFuture.new
        t = Thread.new { future.get(nil) }
        Test::Unit::wait("run", 1, t)
        t.status.should == "sleep"
        future.set
        Test::Unit::wait("run", 1, t)
        t.status.should == false
      end

    end

  end

  context "#wait_for_any" do

    it "works with multiple futures" do
      f1 = ExternalFuture.new
      f2 = ExternalFuture.new
      f3 = ExternalFuture.new
      t = Thread.new do
        f1.set("foo")
      end
      r1, r2, r3 = AWS::Flow::Core.wait_for_any(f1, f2, f3)
      r1.get.should == "foo"
      f2.set?.should be_false
      f3.set?.should be_false
    end

    it "works with no futures" do
      wait_for_any
      # Just need to check if the test completes
      true.should == true
    end

    it "works with an already set future" do
      future = ExternalFuture.new.set
      wait_for_any(future)
      future.set?.should == true
    end

    it "works with one set and one unset future" do
      future = ExternalFuture.new.set
      future2 = ExternalFuture.new
      wait_for_any(future, future2)
      future.set?.should == true
      future2.set?.should be_false
    end

  end

  context "#wait_for_all" do

    it "works with multiple futures" do
      f1 = ExternalFuture.new
      f2 = ExternalFuture.new
      f3 = ExternalFuture.new
      t = Thread.new do
        f1.set("foo")
        f2.set("bar")
        f3.set("baz")
      end
      r1, r2, r3 = AWS::Flow::Core.wait_for_all(f1, f2, f3)
      r1.get.should == "foo"
      r2.get.should == "bar"
      r3.get.should == "baz"
    end

    it "works with no futures" do
      wait_for_all
      # Just need to check if the test completes
      true.should == true
    end

    it "works with an already set future" do
      future = ExternalFuture.new.set
      wait_for_all(future)
      future.set?.should == true
    end

    it "works with one set and one unset future" do
      future = ExternalFuture.new.set
      future2 = ExternalFuture.new
      Thread.new { future2.set }
      wait_for_all(future, future2)
      future.set?.should == true
      future2.set?.should == true
    end

  end


  [:timed_wait_for_all, :timed_wait_for_any].each do |method|

    context "#{method}" do

      it "raises when timeout expires" do
        future = ExternalFuture.new
        expect { AWS::Flow.send(method, 1, future) }.to raise_error(Timeout::Error)
      end

      it "doesn't raise when timeout doesn't expires" do
        future = ExternalFuture.new
        Thread.new { sleep 1; future.set }
        expect { AWS::Flow.send(method, 10, future) }.to_not raise_error
      end

      it "doesn't raise when timeout is nil" do
        future = ExternalFuture.new
        Thread.new { sleep 1; future.set }
        expect { AWS::Flow.send(method, nil, future) }.to_not raise_error
      end

    end
  end

end

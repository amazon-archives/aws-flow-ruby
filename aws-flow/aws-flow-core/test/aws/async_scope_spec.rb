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

describe AsyncScope do

  it "makes sure that basic AsyncScoping works" do
    trace = []
    this_scope = AsyncScope.new() do
      task { trace << :inside_task}
    end
    trace.should == []
    this_scope.eventLoop
    trace.should == [:inside_task]
  end

  context "empty AsyncScope" do
    let(:scope) { AsyncScope.new }
    let(:trace) { [] }

    it "adds tasks like a queue" do
      scope << Task.new(scope.root_context) { trace << :task }
      scope.eventLoop
      trace.should == [:task]
    end

    it "adds a task if given one on construction" do
      scope = AsyncScope.new { trace << :task }
      scope.eventLoop
      trace.should == [:task]
    end


    it "runs a task asynchronously" do
      scope << Task.new(scope.root_context) { trace << :inner }
      trace << :outer
      scope.eventLoop
      trace.should == [:outer, :inner]
    end

    it "runs multiple tasks in order" do
      scope << Task.new(scope.root_context) { trace << :first_task }
      scope << Task.new(scope.root_context) { trace << :second_task }
      scope.eventLoop
      trace.should == [:first_task, :second_task]
    end

    it "resumes tasks after a Future is ready" do
      f = Future.new
      scope = AsyncScope.new do
        task do
          trace << :first
          f.get
          trace << :fourth
        end

        task do
          trace << :second
          f.set(:foo)
          trace << :third
        end
      end
      scope.eventLoop
      trace.should == [:first, :second, :third, :fourth]
    end

    it "tests to make sure that scopes complete after an event loop" do
      scope = AsyncScope.new do
        error_handler do |t|
          t.begin do
            x = Future.new
            y = Future.new
            error_handler do |t|
              t.begin {}
              t.ensure { x.set }
            end
            x.get
            error_handler do |t|
              t.begin {}
              t.ensure { y.set }
            end
            y.get
            error_handler do |t|
              t.begin {}
              t.ensure {}
            end
          end
        end
      end

      completed = scope.eventLoop
      completed.should == true
    end
    it "ensures you can cancel an async scope " do
      condition = FiberConditionVariable.new
      @task = nil
      scope = AsyncScope.new do
        task do
          condition.wait
        end
      end
      scope.eventLoop
      scope.cancel(CancellationException.new)
      expect { scope.eventLoop }.to raise_error CancellationException
    end
  end
end

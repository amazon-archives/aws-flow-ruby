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

require 'pp'

# TODO Split out fibers-related tests into a fibers_spec and
# put the interface-related tests into something like interface_spec
# Or maybe they can stay in flow_spec


# Doesn't make sense to have this for daemon tasks, as the BRE will close before
# it raises

{:DaemonTask => lambda { |&x| daemon_task(&x) },
  :Task => lambda { |&x| task(&x) } }.each_pair do |name, task_creation|




  describe "#{name} method" do
    let(:condition) { FiberConditionVariable.new }
    # TODO Reinstate this test once we figure out how to fix Fibers in 1.8 sharing state

    it "fails when called from outside of a context" do
      expect do
        task_creation.call do
          "Invalid call"
        end
      end.to raise_error(NoContextException)
    end

    it "should execute #{name} children recursively"do
      @executed = []
      def recursive(i, task_creation)
        return if i == 0
        task_creation.call do
          recursive(i - 1, task_creation)
          @executed << i
        end
      end
      scope = AsyncScope.new do
        task { condition.wait }
        recursive(100, task_creation)
      end
      @executed.length.should == 0
      scope.eventLoop
      @executed.length.should == 100
    end



    context 'tests that #{name} do things in the right order' do
      let(:trace) { [] }
      let(:condition) { FiberConditionVariable.new }
      it "executes the block asynchronously" do
        trace << :outside_task
        scope = AsyncScope.new do
          task_creation.call { trace << :inside_task; condition.signal }
          task { condition.wait }
        end
        scope.eventLoop
        trace.should == [:outside_task, :inside_task]
      end

      it "returns something which contains the block's return value" do
        result = nil
        scope = AsyncScope.new do
          result = task_creation.call { condition.signal; :task_executed }
          task { condition.wait }
        end
        scope.eventLoop
        result.get.should == :task_executed
      end

      it "executes multiple #{name}s in order" do
        scope = AsyncScope.new do
          task_creation.call { trace << :first_task }
          task_creation.call { trace << :second_task; condition.signal }
          task { condition.wait }
        end
        scope.eventLoop
        trace.should == [:first_task, :second_task]
      end

      it "executes nested #{name} after the parent task" do

        scope = AsyncScope.new do
          task_creation.call do
            task_creation.call { trace << :inside_task; condition.signal }
            trace << :outside_task
          end
          task { condition.wait }
        end
        scope.eventLoop
        trace.should == [:outside_task, :inside_task]
      end

      it "executes nested #{name}" do
        scope = AsyncScope.new do
          task_creation.call do
            trace << :outside_task
            task_creation.call { trace << :inside_task; condition.signal }
          end
          task { condition.wait }
        end
        scope.eventLoop
        trace.should == [:outside_task, :inside_task]
      end
    end
  end
end
{:Task => lambda { |x, &y| Task.new(x, &y) },
  :DaemonTask => lambda { |x, &y| DaemonTask.new(x, &y) }}.each_pair do |name, task_creation|

  describe name do
    let(:trace) { [] }
    let(:scope) { AsyncScope.new }
    let(:task) { task_creation.call(scope.root_context) { trace << :inner } }

    context "result for a task that is not run" do
      let(:result) { task.result }
      subject { result }
      its(:class) { should == Future }
      it { should_not be_set }
    end

    it "executes a block asynchronously" do
      trace << :outer
      task.resume
      trace.should == [:outer, :inner]
    end

    it "is the currently running Fiber when the block executes" do
      task = task_creation.call(scope.root_context) do
        fiber = ::Fiber.current
        fiber.should == task
        trace << :executed
      end
      task.resume
      trace.should == [:executed]
    end
    it "doesn't let the block return from the block's parent method" do
      def create_and_run_task(task_creation)
        t = task_creation.call(scope.root_context) { return :inner_return }
        t.resume
        :outer_return
      end
      result = create_and_run_task(task_creation)
      result.should == :outer_return
    end

    # context "cancelled Task" do
    #   it "makes sure that a cancelled task does not run" do
    #     task = task_creation.call(scope.root_context) { trace << :should_never_happen }
    #     scope << task
    #     task.cancel(StandardError)

    #     trace.should == []
    #   end
    # end

    context "Dead Fiber" do
      let(:dead_task) { t = task_creation.call(scope.root_context) { 1 + 1 }; t.resume; t }
      subject { dead_task }
      it { should_not be_alive }
      it "should raise FiberError on resume" do
        expect { dead_task.resume }.to raise_error FiberError
      end
    end

    it "makes sure that tasks return a Future" do
      a = task_creation.call(scope.root_context) { trace << :first }
      b = task_creation.call(scope.root_context) do
        a.result.get
        trace << :second
      end
      scope << a
      scope << b
      scope.eventLoop
      trace.should == [:first, :second]
    end
  end
end

{:Fiber => lambda {|&block| Fiber.new &block},
  :Task => lambda {|&block| Task.new(AsyncScope.new.root_context, &block)},
}.each_pair do |klass, initialize|
  describe "#{klass}#alive?" do
    let(:simple_unit) { initialize.call { 2 + 2 } }
    let(:waiting_unit) { initialize.call { Fiber.yield } }
    it "tells you the #{klass} is alive before the #{klass} starts" do
      simple_unit.alive?.should == true
    end

    it "tells you the #{klass} is not alive once the #{klass} has exited" do
      simple_unit.resume
      simple_unit.alive?.should == false
    end

    it "tells you the #{klass} is alive if the #{klass} is partially executed" do
      waiting_unit.resume
      # TODO: Discuss whether this should fail or not
      waiting_unit.alive?.should == true
    end
  end
end

describe Task do

  it "ensures that raising an error in a task is handled correctly" do
    scope = AsyncScope.new do
      task do
        raise "Boo"
      end
    end
    begin
      scope.eventLoop
    rescue Exception => e
      e.message.should =~ /Boo.*/
    end
  end

  it "ensures that cancelling a task will not have it remove itself twice " do
    condition = FiberConditionVariable.new
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin do
          task do
            condition.wait
          end
          task do
            condition.signal
            # i.e.,
            raise "simulated error"
          end
        end
      end
    end
    expect { scope.eventLoop }.to raise_error "simulated error"
  end


end

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


def change_test(options = {})
  subject { this_bre }
  its(:heirs) { options[:heirs] ? (should_not be_empty) : (should be_empty) }
  its(:nonDaemonHeirsCount) { should == options[:nonDaemonHeirsCount] }
end

describe BeginRescueEnsure do
  let(:condition) { FiberConditionVariable.new }
  let(:flowfactory) { FlowFactory.new }

  def make_trying_BRE(begin_tasks = [])
    scope = AsyncScope.new do
      @error_handler = error_handler do |t|
        t.begin do
          begin_tasks.each { |task| task.call}
          condition.wait
        end
        t.rescue(IOError) {|x| condition.wait }
        t.rescue(CancellationException) { |x| x }
      end
    end
    scope.eventLoop
    @error_handler
  end

  def make_catching_BRE(begin_tasks = [])
    scope = AsyncScope.new do
      @error_handler = error_handler do |t|
        t.begin do
          raise IOError
        end
        t.rescue(IOError) do |x|
          begin_tasks.each { |task| task.call }
          condition.wait
        end
      end
    end
    scope.eventLoop
    @error_handler
  end

  let(:trying_BRE) do
    make_trying_BRE
  end

  let(:catching_BRE) do
    make_catching_BRE
  end
  context "BeginRescueEnsure in the :trying state" do
    subject { trying_BRE }
    its(:heirs) { should_not be_empty }
    its(:nonDaemonHeirsCount) { should == 1 }

    context "which adds a task," do
      let(:trying_BRE_with_one_task) do
        make_trying_BRE([lambda { task { condition.wait  } }])
      end
      let(:first_task) { trying_BRE_with_one_task.heirs.first }
      subject { trying_BRE_with_one_task }
      its(:heirs) { should_not be_empty }
      its(:nonDaemonHeirsCount) { should == 2 }

      context "and then has the task status changed," do
        context "and cancel the BRE" do
          let(:this_bre) { trying_BRE_with_one_task.cancel(StandardError.new);  trying_BRE_with_one_task }
          # As a note: we trigger the rescue task, so we have a task in the heirs
          change_test :heirs => true, :nonDaemonHeirsCount => 1
        end

        context "and fail the the task" do
          let(:this_bre) { expect { trying_BRE_with_one_task.fail(first_task, StandardError.new) }.to raise_error StandardError; trying_BRE_with_one_task }
          change_test :heirs => false, :nonDaemonHeirsCount => 0
        end

        context "and remove the task " do
          let(:this_bre) { trying_BRE_with_one_task.remove(first_task); trying_BRE_with_one_task }
          change_test :heirs => true, :nonDaemonHeirsCount => 1
        end
      end
    end

    context "which adds two tasks" do
      let(:trying_BRE_with_two_tasks) do
        make_trying_BRE([lambda { task { condition.wait }}, lambda { task { condition.wait }}])
      end
      let(:first_task) { trying_BRE_with_two_tasks.heirs.first }
      subject { trying_BRE_with_two_tasks }
      its(:heirs) { should_not be_empty }
      its(:nonDaemonHeirsCount) { should == 3 }

      context "And then we change the task status" do
        context "and cancel the BRE" do
          let(:this_bre) { trying_BRE_with_two_tasks.cancel(StandardError.new); trying_BRE_with_two_tasks }
          change_test :heirs => true, :nonDaemonHeirsCount => 1
        end

        context "and fail the the task" do
          let(:this_bre) { expect { trying_BRE_with_two_tasks.fail(first_task, StandardError.new) }.to raise_error StandardError; trying_BRE_with_two_tasks }
          change_test :heirs => false, :nonDaemonHeirsCount => 0
        end

        context "and remove the task " do
          let(:this_bre) { trying_BRE_with_two_tasks.remove(first_task); trying_BRE_with_two_tasks }
          change_test :heirs => true, :nonDaemonHeirsCount => 2
        end
      end
    end
  end

  context "BeginRescueEnsure in the :catching state" do
    subject { catching_BRE}
    its(:heirs) { should_not be_empty }
    its(:nonDaemonHeirsCount) { should == 1 }

    context "which adds a task," do
      let(:catching_BRE_with_one_task) do
        make_catching_BRE([lambda { task { condition.wait } }])
      end
      let(:first_task) { catching_BRE_with_one_task.heirs.first }
      subject { catching_BRE_with_one_task }
      its(:heirs) { should_not be_empty }
      its(:nonDaemonHeirsCount) { should == 2 }

      context "and then has the task status changed," do
        context "and cancel the BRE" do
          let(:this_bre) { catching_BRE_with_one_task.cancel(StandardError.new); catching_BRE_with_one_task }
          change_test :heirs => true, :nonDaemonHeirsCount => 2
        end

        context "and fail the the task" do
          let(:this_bre) { expect { catching_BRE_with_one_task.fail(first_task, StandardError.new) }.to raise_error StandardError; catching_BRE_with_one_task }
          change_test :heirs => false, :nonDaemonHeirsCount => 0
        end

        context "and remove the task " do
          let(:this_bre) { catching_BRE_with_one_task.remove(first_task); catching_BRE_with_one_task }
          change_test :heirs => true, :nonDaemonHeirsCount => 1
        end
      end
    end

    context "which adds two tasks" do
      let(:catching_BRE_with_two_tasks) do
        make_catching_BRE([lambda { task { condition.wait } },lambda { task { condition.wait } } ])
      end
      let(:first_task) { catching_BRE_with_two_tasks.heirs.first }
      subject { catching_BRE_with_two_tasks }
      its(:heirs) { should_not be_empty }
      its(:nonDaemonHeirsCount) { should == 3 }

      context "And then we change the task status" do
        context "and cancel the BRE" do
          let(:this_bre) { catching_BRE_with_two_tasks.cancel(StandardError.new); catching_BRE_with_two_tasks }
          change_test :heirs => true, :nonDaemonHeirsCount => 3
        end

        context "and fail the the task" do
          let(:this_bre) { expect { catching_BRE_with_two_tasks.fail(first_task, StandardError.new) }.to raise_error StandardError; catching_BRE_with_two_tasks }
          change_test :heirs => false, :nonDaemonHeirsCount => 0
        end

        context "and remove the task " do
          let(:this_bre) { catching_BRE_with_two_tasks.remove(first_task); catching_BRE_with_two_tasks }
          change_test :heirs => true, :nonDaemonHeirsCount => 2
        end
      end

    end
  end

  before(:each) { class RescueTestingError < StandardError; end }
  let(:trace) { [] }
  let(:flowfactory) { FlowFactory.new }
  let(:scope) { flowfactory.generate_scope }

  it "makes sure that error handling without resuming does nothing" do

    scope = AsyncScope.new do
      _error_handler do |t|
        t.begin { trace << :in_the_begin }
      end
    end
    trace.should == []
  end

  {:Task => lambda { |&x| task(&x) }, :DaemonTask => lambda { |&x| daemon_task(&x) }}.each_pair do |name, task_creation|

    # TODO: Reinstate this test, if it is easy enough
    #
    # it "makes sure that error handling with a #{name} free floating raises" do
    #   scope = AsyncScope.new do
    #     error_handler do |t|
    #       t.begin { trace << :in_the_begin }
    #       t.rescue(RescueTestingError) {}
    #       task_creation.call { trace << :in_the_daemon }
    #     end
    #   end
    #   expect { scope.eventLoop }.to raise_error NoContextException, /You have not scoped this task correctly!/
    # end

    it "make sure that #{name} in a BRE runs correctly, and that the BRE supports signal " do
      condition = FiberConditionVariable.new
      scope = AsyncScope.new do
        error_handler do |t|
          t.begin do
            trace << :in_the_begin
            task_creation.call { trace << :in_the_daemon; condition.signal }
            condition.wait
          end
          t.rescue(RescueTestingError) {}
        end
      end
      scope.eventLoop
      trace.should == [:in_the_begin, :in_the_daemon]
    end
  end

  it "makes sure that a BRE can finish with a daemon task still alive" do
    condition = FiberConditionVariable.new
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin do
          trace << :in_the_begin
          @daemon_task = daemon_task { condition.wait; :should_never_be_hit }
        end
        t.rescue(RescueTestingError) {}
      end
    end
    scope.eventLoop
    trace.should == [:in_the_begin]
  end

  # TODO Reinstate this test, after fixing it so it doesn't add to BRE directly
  # it "makes sure that a daemon task can cancel a BRE" do
  #   flowfactory = FlowFactory.new
  #   condition = FiberConditionVariable.new
  #   scope = flowfactory.generate_scope
  #   bre = flowfactory.generate_BRE(:begin => lambda { condition.wait })
  #   daemon_task = DaemonTask.new(bre) { condition.wait }
  #   task = Task.new(bre) { condition.wait }
  #   bre << task
  #   bre << daemon_task
  #   scope.eventLoop
  #   bre.remove(task)
  #   scope.eventLoop
  #   trace.should == []
  # end


  # TODO Reinstate this test, after fixing it so it doesn't add to BRE directly
  # it "makes sure that a daemon task cancelling does not override other fails" do
  #   flowfactory = FlowFactory.new
  #   condition = FiberConditionVariable.new
  #   scope = flowfactory.generate_scope
  #   bre = flowfactory.generate_BRE(:begin => lambda { condition.wait })
  #   daemon_task = DaemonTask.new(bre) { condition.wait }
  #   task = Task.new(bre) { condition.wait }
  #   bre << daemon_task
  #   bre << task
  #   scope.eventLoop
  #   daemon_task.cancel(StandardError)
  #   bre.fail(task, RescueTestingError)
  #   expect { debugger; scope.eventLoop }.to raise_error RescueTestingError
  #   trace.should == []
  # end

  # TODO Fix these up with the FlowFactory
  it "makes sure that creating a task in the rescue block will run it" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin { raise RescueTestingError}

        t.rescue(RescueTestingError) do
          trace << :in_the_rescue
          task { trace << :in_the_task }
        end
      end
      scope.eventLoop
      trace.should == [:in_the_beginning, :in_the_task]
    end
  end

  it "makes sure that creating a task inside a BRE block will run it" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin do
          trace << :in_the_beginning
          task { trace << :in_the_task }
        end
        t.rescue(StandardError) {|x| }
      end
    end
    scope.eventLoop
    trace.should == [:in_the_beginning, :in_the_task]
  end

  it "ensures you can have error handling without ensure" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin { trace << :in_the_begin }
        t.rescue(StandardError) { trace << :in_the_rescue }
      end
    end
    scope.eventLoop
    trace.should == [:in_the_begin]
  end

  it "ensures that rescue picks the first matching rescue block" do
    scope = AsyncScope.new do
      @error_handler = _error_handler do |t|
        t.begin do
          trace << :in_the_begin
          raise RescueTestingError
        end
        t.rescue(RescueTestingError) { trace << :in_the_rescue }
        t.rescue(StandardError) { trace << :in_the_bad_rescue }
      end
    end
    scope.eventLoop
    trace.should == [:in_the_begin, :in_the_rescue]
  end

  it "ensures that rescue blocks trickle to the first applicable block" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin do
          trace << :in_the_begin
          raise RescueTestingError
        end
        t.rescue(IOError) { trace << :bad_rescue }
        t.rescue(RescueTestingError) { trace << :in_the_rescue }
      end
    end
    scope.eventLoop
    trace.should == [:in_the_begin, :in_the_rescue]
  end

  it "ensures that the same rescue exception twice causes an exception" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin {}
        t.rescue(RescueTestingError) {}
        t.rescue(RescueTestingError) {}
      end
    end
    # TODO Make rescue give a specific error in this case

    expect { scope.eventLoop }.to raise_error /You have already registered RescueTestingError/
  end

  it "ensures that stack traces work properly" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin { raise "simulated" }
        t.rescue(Exception) do |e|
          e.backtrace.should include "------ continuation ------"
          e.message.should == "simulated"
        end
      end
    end
    scope.eventLoop
  end

  it "makes sure an error is raised if none of the rescue blocks apply to it" do
    scope = AsyncScope.new  do
      error_handler do |t|
        t.begin { raise RescueTestingError }
        t.rescue(IOError) { p "nothing doing here" }
      end
    end
    expect { scope.eventLoop }.to raise_error RescueTestingError
  end

  it "ensures that the ensure block is called with a rescue path" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin do
          trace << :in_the_begin
          raise RescueTestingError
        end
        t.rescue(RescueTestingError) { trace << :in_the_rescue }
        t.ensure { trace << :in_the_ensure }
      end
    end
     scope.eventLoop
    trace.should == [:in_the_begin, :in_the_rescue, :in_the_ensure]
  end

  it "calls ensure after subtask ensure" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin do
          trace << :in_the_top_begin
          error_handler do |h|
            h.begin { trace << :in_the_inner_begin }
            h.rescue(StandardError) { }
            h.ensure { trace << :in_the_inner_ensure }
          end
          t.rescue(StandardError) {  }
          t.ensure { trace << :in_the_outer_ensure}
        end
      end
      scope.eventLoop
      trace.should == [:in_the_top_begin, :in_the_inner_begin, :in_the_inner_ensure, :in_the_outer_ensure]
    end
  end

  it "ensures that an ensure block will be called when an exception is raised" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin do
          trace << :in_the_begin
          raise StandardError
        end
        t.rescue(RescueTestingError) { trace << :this_is_not_okay }
        t.ensure { trace << :in_the_ensure }
      end
    end
    expect { scope.eventLoop }.to raise_error StandardError
    trace.should == [:in_the_begin, :in_the_ensure]
  end

  it "ensures that the ensure block is called without an applicable rescue case
  passes the error up" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin do
          trace << :in_the_begin
          raise RescueTestingError
        end
        t.rescue(IOError) { trace << :in_the_rescue }
        t.ensure { trace << :in_the_ensure }
        expect { scope.eventLoop }.to raise_error RescueTestingError
        trace.should == [:in_the_begin, :in_the_ensure]
      end
    end
  end

  it "ensures that two begins raise an error" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin { trace << :yay }
        t.begin { trace << :oh_no }
        t.rescue(StandardError) { trace << :should_never_be_hit }
      end
    end
    expect { scope.eventLoop }.to raise_error RuntimeError, /Duplicated begin/
  end

  it "ensures that two ensure statements raise an error" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin { trace << :yay }
        t.rescue(StandardError) { trace << :should_never_be_hit }
        t.ensure { trace << :this_is_okay }
        t.ensure { trace << :but_this_is_not }
      end
    end
    expect { scope.eventLoop }.to raise_error /Duplicated ensure/
  end
  # TODO: Reinstate this test
  # it "ensures that you can't call resume inside the error handler" do
  #   scope = AsyncScope.new do
  #     error_handler do |t|
  #       t.begin { trace << :in_the_begin }
  #       t.rescue(StandardError) { trace << :in_the_rescue }
  #       t.ensure { trace << :in_the_ensure }
  #       t.resume
  #     end
  #   end
  #   expect { scope.eventLoop }.to raise_error NoMethodError
  # end

  it "ensures that you can't access the private variables of a BeginRescueEnsure block" do
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin { "This is the begin" }
        t.rescue(StandardError) { "This is the rescue" }
        t.ensure { trace << t.begin_task }
      end
    end
    expect { scope.eventLoop }.to raise_error
    trace.should == []
  end

  describe "BRE's  closed behavior" do
    before (:each) do
      @scope = AsyncScope.new do
        @error_handler = error_handler do |t|
          t.begin do
            "this is the beginning"
          end
          t.rescue StandardError do
            "this is the rescue"
          end
        end
      end
    end

    it "ensures that BRE's end at closed" do
      @scope.eventLoop
      @error_handler.current_state.should == :closed
    end

  end
  context "Cancelling a BRE in the created state" do
    scope = AsyncScope.new
    condition = FiberConditionVariable.new
    error_handler = BeginRescueEnsure.new(:parent => scope.root_context)
    scope << error_handler
    error_handler.begin lambda { raise StandardError; condition.wait }
    error_handler.rescue(IOError, lambda { "this is the rescue" })
    error_handler.cancel(StandardError)
    subject { error_handler }
    its(:current_state) { should == :closed }
  end

  it "makes sure that get_heirs works" do
    flowfactory = FlowFactory.new
    condition = FiberConditionVariable.new
    scope = flowfactory.generate_scope
    bre = flowfactory.generate_BRE(:begin => lambda { task { condition.wait; trace << :yay} })
    scope.eventLoop
    bre.get_heirs.length.should > 1
    trace.should == []
  end
  #TODO this test shouldn't be needed, but it's a good comparison for flowfactory being borked
  it "makes sure that get_heirs works" do
    condition = FiberConditionVariable.new
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin { task { condition.wait; trace << :yay }}
        t.rescue(StandardError) {}
      end
    end
    scope.eventLoop
    trace.should == []
  end

  it "makes sure that you can have a schedule in the middle of a BRE" do
    future = Future.new
    scope = AsyncScope.new do
      error_handler do |t|
        t.begin { task { future.get }}
      end
    end
    scope.eventLoop
    future.set(nil)
    completed = scope.eventLoop
    completed.should == true
  end
  # it "makes sure that failing a begin task will cause the other tasks to not get run" do
  #   trace = []
  #   scope = AsyncScope.new do
  #     error_handler do |t|
  #       t.begin do
  #         condition = FiberConditionVariable.new
  #         error_handler do |first_t|
  #           first_t.begin do
  #             task do
  #               trace << "in the error"
  #               condition.signal
  #               raise "simulated error"
  #             end
  #             debugger
  #             condition.wait
  #             other_future = task do
  #               trace << "This should not be"
  #             end
  #           end
  #           first_t.rescue(StandardError) do |error|
  #             raise error
  #           end
  #           first_t.ensure {condition.signal}
  #         end

  #         error_handler do |second_t|
  #           second_t.begin do
  #             other_future = task do
  #               trace << "This should not be"
  #             end
  #           end
  #           second_t.rescue(StandardError) do |error|
  #             raise error
  #           end
  #           second_t.ensure {}
  #         end
  #         t.rescue(StandardError) {|error| raise error}
  #         t.ensure {}
  #       end
  #     end
  #   end
  #   debugger
  #   expect { scope.eventLoop }.to raise_error StandardError
  #   trace.should == []
  # end


end

describe "BRE#alive?" do
  context "checking different types of BREs" do
    let(:flowfactory) { FlowFactory.new }
    let(:scope) { flowfactory.generate_scope }
    before(:each) do
     @regular_bre = flowfactory.generate_BRE
      @rescue_bre = flowfactory.generate_BRE(:begin => lambda { raise StandardError })
    end
    context "regular bre" do
      subject { @regular_bre }
      it { should be_alive }
      context "and then we run the scope" do
        let(:run_bre) { scope.eventLoop; @regular_bre }
        subject { run_bre }
        it { should_not be_alive }
      end
    end
    context "rescue bre" do
      subject { @rescue_bre }
      it { should be_alive }
      context "and then we run the scope" do
        let(:run_bre) { scope.eventLoop; @rescue_bre }
        subject { run_bre }
        it { should_not be_alive }
      end
    end
  end
end
describe "Misc" do
  it "makes sure that the return value of a BRE is the begin" do
    scope = AsyncScope.new do
      @x = _error_handler do |t|
        t.begin { 5 }
      end
    end
    scope.eventLoop
    @x.get.should == 5
  end
  it "meakes sure that if there is a failure, the result is the rescue value" do
    scope = AsyncScope.new do
      @x = _error_handler do |t|
        t.begin { raise StandardError }
        t.rescue(StandardError) { 6 }
      end
    end
    scope.eventLoop
    @x.get.should == 6
  end
end

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

describe "ExternalTask#alive?" do
  let(:scope) { AsyncScope.new }
  let(:simple_unit) do
    ExternalTask.new(:parent => scope ) do |t|
      t.initiate_task { |h| 2 + 2; h.complete }
    end
  end
  it "tells you that ExternalTask is alive before the ExternalTask starts" do
    simple_unit.alive?.should == true
  end
  it "tells you that ExternalTask is not alive once ExternalTask has exited" do
    task_context = TaskContext.new(:parent => scope.get_closest_containing_scope, :task => simple_unit)
    simple_unit.resume
    simple_unit.alive?.should == false
  end
end

describe ExternalTask do
  let(:trace) { [] }

  before(:each) do

    class ExternalTaskException < Exception; end
    @this_scope = AsyncScope.new() do
    end
    @this_task = ExternalTask.new(:parent => @this_scope) do |t|
      t.cancellation_handler { |h, cause| h.fail(cause) }
      t.initiate_task { |h| trace << :task_started; h.complete }
    end
    task_context = TaskContext.new(:parent => @this_scope.get_closest_containing_scope, :task => @this_task)
    @this_scope << @this_task
  end

  it "should complete okay" do

    @this_scope.eventLoop
    trace.should == [:task_started]
  end

  it "cancelling should cause an exception to be raised" do
    @this_task.cancel(ExternalTaskException.new)
    expect { @this_scope.eventLoop }.to raise_error ExternalTaskException
  end

  it "shouldn't cancel an already completed task" do
    @this_scope.eventLoop
    @this_task.cancel(Exception)
  end

  describe "cancelling an external task without a cancellation handler" do
    before(:each) do
      @this_scope = AsyncScope.new() do

      end
      @this_task = ExternalTask.new() do |t|
        t.initiate_task { |h| trace << :task_started; h.complete }
      end
      @this_scope << @this_task
    end
    it "ensures that calling cancel results in no error" do
      task_context = TaskContext.new(:parent => @this_scope.get_closest_containing_scope, :task => @this_task)
      @this_task.cancel(ExternalTaskException)
    end
    it "ensures that attempting to run a cancelled task has no effect" do
      task_context = TaskContext.new(:parent => @this_scope.get_closest_containing_scope, :task => @this_task)
      @this_task.cancel(ExternalTaskException)
      @this_scope.eventLoop
      trace.should == []
    end
  end

  it "ensures that raising in the cancellation handler is handled " do
    scope = AsyncScope.new
    external_task = ExternalTask.new() do |t|
      t.initiate_task { |h| h.complete }
      t.cancellation_handler { |h, cause| raise "Oh no!" }
    end
    scope << external_task
    task_context = TaskContext.new(:parent => scope.get_closest_containing_scope, :task => external_task)
    external_task.cancel(Exception)
    expect { scope.eventLoop }.to raise_error /Oh no!/
  end

  it "ensures that cancelling a completed task is handled" do
    @this_scope.eventLoop
    @this_task.cancel(Exception)
  end

  it "ensures that cancelling a cancelled task is handled" do
    @this_scope.eventLoop
    @this_task.cancel(Exception)
    @this_task.cancel(Exception)
  end

  it "ensures that failing a task after completion raises an error" do
    @this_scope = AsyncScope.new() do
    end
    @this_task = ExternalTask.new() do |t|
      t.cancellation_handler { |h| h.fail }
      t.initiate_task { |h| trace << :task_started; h.complete; h.fail(Exception) }
    end
    task_context = TaskContext.new(:parent => @this_scope.get_closest_containing_scope, :task => @this_task)
    @this_scope << @this_task
    expect { @this_scope.eventLoop }.to raise_error /Already completed/
  end

  it "ensures that completing an external_task allows the asyncScope to move forward" do
    @handle = nil
    @this_scope = AsyncScope.new() do
      external_task do |t|
        t.cancellation_handler {|h| h.fail}
        t.initiate_task {|h| @handle = h }
      end
    end
    @this_scope.eventLoop
    @this_scope.is_complete?.should == false
    @handle.complete
    @this_scope.eventLoop
    @this_scope.is_complete?.should == true
  end
end

describe "external_task function" do

  it "ensures that cancelling will raise the externalTasks cancellation handler" do
    trace = []
    this_scope = AsyncScope.new() do
      trace << :async_start
      external_task do |t|
        t.cancellation_handler {|h, cause| trace << :cancellation_handler; h.complete }
        t.initiate_task { |h|  trace << :external_task}
      end
      task { trace << :task; raise IllegalStateException }
      trace << :async_done
    end
    expect { this_scope.eventLoop }.to raise_error IllegalStateException
    trace.should == [:async_start, :async_done, :external_task, :task, :cancellation_handler]
  end

  it "tests explicit cancellation of BRE cancelling externalTask correctly" do
    handle_future = Future.new
    trace = []

    scope = AsyncScope.new do
      trace << :async_start
      bre = error_handler do |t|
        t.begin do
          external_task do |t|
            trace << :external_task;
            t.cancellation_handler {|h, cause| trace << :cancellation_handler; h.complete}
            t.initiate_task { |h|  handle_future.set(h) }
          end

          task do
            trace << :in_the_cancel_task
            handle_future.get
            bre.cancel(nil)
          end
        end
        t.rescue(Exception) { |e| e.class.should <= CancellationException }
      end

      trace << :async_done
    end
    scope.eventLoop
    trace.should == [:async_start, :async_done, :external_task, :in_the_cancel_task, :cancellation_handler]
  end

  it "ensures that having an external_task within an AsyncScope works" do
    trace = []
    this_scope = AsyncScope.new() do
      external_task do |t|
        t.cancellation_handler { |h| h.fail }
        t.initiate_task { |h| trace << :task_started; h.complete; }
      end
    end
    trace.should == []
    this_scope.eventLoop
    trace.should == [:task_started]
  end

end

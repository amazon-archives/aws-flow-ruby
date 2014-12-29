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

require 'yaml'
require 'aws-sdk-v1'
require 'logger'
require_relative 'setup'


class SimpleTestHistoryEvent
  def initialize(id); @id = id; end
  def attributes; TestHistoryAttributes.new(@id); end
end
class TestHistoryAttributes
  def initialize(id); @id = id; end
  [:activity_id, :workflow_id, :timer_id].each do |method|
    define_method(method) { @id }
  end
end

describe "RubyFlowDecider" do
  before(:all) do
    class MyWorkflow
      extend AWS::Flow::Workflows
      version "1"
      # TODO more of the stuff from the proposal
    end

    @swf, @domain = setup_swf
    if @domain.workflow_executions.with_status(:open).count.count > 0
      @domain.workflow_executions.with_status(:open).each { |wf| wf.terminate }
    end
  end
  before(:each) do
    kill_executors
    kill_executors
  end
  after(:each) do
    kill_executors
    kill_executors
  end

  it "runs an empty workflow, making sure base configuration stuff is correct" do
    target_workflow = @domain.workflow_types.page(:per_page => 1000).select { |x| x.name == "blank_workflow_test"}
    if target_workflow.length == 0
      workflow_type = @domain.workflow_types.create("blank_workflow_test", '1',
                                                    :default_task_list => "initial_test_tasklist",
                                                    :default_child_policy => :request_cancel,
                                                    :default_task_start_to_close_timeout => 3600,
                                                    :default_execution_start_to_close_timeout => 24 * 3600)
    else
      workflow_type = target_workflow.first
    end
    workflow_execution = workflow_type.start_execution :input => "yay"
    workflow_execution.terminate
  end

  describe WorkflowFactory do
    it "makes sure that you can use the basic workflow_factory" do
      task_list = "workflow_factory_task_list"
      class WorkflowFactoryActivity
        extend AWS::Flow::Activities
        activity :run_activity1 do
          {
            version: "1.0",
            default_task_list: "workflow_factory_task_list",
            default_task_schedule_to_close_timeout: 60,
            default_task_schedule_to_start_timeout: 30,
            default_task_start_to_close_timeout: 30,
          }
        end
        def run_activity1(arg)
          "#{arg} is what the activity recieved"
        end
      end

      class WorkflowFactoryWorkflow
        extend AWS::Flow::Workflows
        workflow :entry_point do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: "workflow_factory_task_list",
            default_task_start_to_close_timeout: 120,
            default_child_policy: :request_cancel,
          }
        end
        activity_client(:activity) { { from_class: "WorkflowFactoryActivity" } }
        def entry_point(arg)
          activity.run_activity1("#{arg} recieved as input")
        end
      end

      worker = WorkflowWorker.new(@domain.client, @domain, task_list, WorkflowFactoryWorkflow)
      activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, WorkflowFactoryActivity)
      worker.register
      activity_worker.register

      client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "WorkflowFactoryWorkflow" } }

      workflow_execution = client.start_execution("some input")

      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { worker.start }
      @forking_executor.execute { activity_worker.start }

      wait_for_execution(workflow_execution)

      workflow_execution.events.map(&:event_type).should == ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
      @forking_executor.shutdown(1)
    end
  end


  class ParentActivity
    class << self
      attr_accessor :task_list
    end
  end
  class ParentWorkflow
    class << self
      attr_accessor :task_list, :activity_class
    end
  end

  class GeneralActivity
    class << self; attr_accessor :task_list; end
  end
  class MyWorkflow
    class << self; attr_accessor :task_list; end
  end

  def general_test(attributes, &block)
    task_list = attributes[:task_list] || "general_task_list"
    class_name = attributes[:class_name] || "General"

    new_activity_class = Class.new(ParentActivity) do
      extend AWS::Flow::Activities
      activity :run_activity1, :run_activity2 do
        {
          default_task_list: task_list,
          default_task_schedule_to_start_timeout: "60",
          default_task_start_to_close_timeout: "60",
          version: "1.0",
          prefix_name: "#{class_name}Activity",
        }
      end
      def run_activity1; end
      def run_activity2; end
    end
    @activity_class = Object.const_set("#{class_name}Activity", new_activity_class)
    new_workflow_class = Class.new(ParentWorkflow) do
      extend AWS::Flow::Workflows
      workflow(:entry_point) {
        {
          version: "1.0",
          default_execution_start_to_close_timeout: 300,
          default_task_list: task_list,
          prefix_name: "#{class_name}Workflow"
        }
      }
      def entry_point
        activity.run_activity1
      end
    end

    @workflow_class = Object.const_set("#{class_name}Workflow", new_workflow_class)
    @workflow_class.activity_class = @activity_class
    @workflow_class.task_list = task_list
    @activity_class.task_list = task_list
    @workflow_class.class_eval do
      activity_client(:activity) { { from_class: self.activity_class } }
    end
    @worker = WorkflowWorker.new(@domain.client, @domain, task_list, @workflow_class)
    @activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, @activity_class)

    @worker.register
    @activity_worker.register
    @my_workflow_client = workflow_client(@domain.client, @domain) { { from_class: @workflow_class } }
  end

  describe "Workflow/Activity return values/exceptions" do
    it "ensures that an activity returning more than 32k data fails the activity" do
      general_test(:task_list => "ActivityTaskLargeOutput", :class_name => "ActivityTaskLargeOutput")
      @activity_class.class_eval do
        def run_activity1
          # Make sure we return something that's over 32k. Note this won't
          # necessarily work with all converters, as it's pretty trivially
          # compressible
          return ":" + "a" * 33000
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      history_events = workflow_execution.events.map(&:event_type)
      # Previously, it would time out, as the failure would include the original
      # large output that killed the completion and failure call. Thus, we need to
      # check that we fail the ActivityTask.
      history_events.should include "ActivityTaskFailed"

      workflow_execution.events.to_a.last.attributes.details.should_not =~ /Psych/
      workflow_execution.events.to_a.last.attributes.reason.should == Utilities.validation_error_string("Activity")
      history_events.last.should == "WorkflowExecutionFailed"
    end

    it "ensures that an activity returning an exception of size more than 32k fails the activity correctly and truncates the message" do
      general_test(:task_list => "ActivityTaskExceptionLargeOutput", :class_name => "ActivityTaskExceptionLargeOutput")
      @activity_class.class_eval do
        def run_activity1
          raise  ":" + "a" * 33000
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      history_events = workflow_execution.events.map(&:event_type)
      # Previously, it would time out, as the failure would include the original
      # large output that killed the completion and failure call. Thus, we need to
      # check that we fail the ActivityTask.
      history_events.should include "ActivityTaskFailed"

      workflow_execution.events.to_a.last.attributes.details.should_not =~ /Psych/
      history_events.last.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should include("[TRUNCATED]")
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.default_data_converter.load(details)
      exception.class.should == AWS::Flow::ActivityTaskFailedException
    end

    it "ensures that an activity returning a Cancellation Exception of size more than 32k fails the activity" do
      general_test(:task_list => "ActivityTaskCancellationExceptionLargeOutput", :class_name => "ActivityTaskCancellationExceptionLargeOutput")
      @activity_class.class_eval do
        def run_activity1
          raise  CancellationException.new("a" * 33000)
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      history_events = workflow_execution.events.map(&:event_type)
      history_events.should include "ActivityTaskFailed"

      history_events.last.should == "WorkflowExecutionFailed"
      event = workflow_execution.events.to_a.select { |x| x.event_type == "ActivityTaskFailed"}
      event.first.attributes.reason.should == Utilities.validation_error_string("Activity")
      event.first.attributes.details.should == "AWS::SimpleWorkflow::Errors::ValidationException"
    end

    it "ensures that a workflow output > 32k fails the workflow" do
      general_test(:task_list => "WorkflowOutputTooLarge", :class_name => "WorkflowOutputTooLarge")
      @workflow_class.class_eval do
        def entry_point
          return ":" + "a" * 33000
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      last_event.attributes.reason.should == Utilities.validation_error_string_partial("Workflow")
    end

    it "ensures that a workflow exception details > 32k fails the workflow correctly and truncates the details" do
      general_test(:task_list => "WorkflowExceptionDetailsTooLarge", :class_name => "WorkflowExceptionDetailsTooLarge")
      @workflow_class.class_eval do
        def entry_point
          e = RuntimeError.new("a")
          e.set_backtrace("a"*25769)
          raise e
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.default_data_converter.load(details)
      exception.class.should == RuntimeError
      exception.backtrace.first.should include ("[TRUNCATED]")
    end

    it "ensures that a workflow exception message > 256 characters fails the workflow correctly and truncates the message" do
      general_test(:task_list => "WorkflowExceptionMessageTooLarge", :class_name => "WorkflowExceptionMessageTooLarge")
      @workflow_class.class_eval do
        def entry_point
          raise  "a" * 257
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should include("[TRUNCATED]")
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.default_data_converter.load(details)
      exception.class.should == RuntimeError
    end


    it "ensures that a respond_decision_task_completed call with response > 32k that we can't truncate fails the workflow correctly" do
      class CustomException < FlowException
        def initialize(reason, details)
          @something = "a"*50000
          super(reason, details)
        end
      end
      general_test(:task_list => "CustomWorkflowExceptionTooLarge", :class_name => "CustomWorkflowExceptionTooLarge")
      @workflow_class.class_eval do
        def entry_point
          raise  CustomException.new("asdf", "sdf")
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should == Utilities.validation_error_string("Workflow")
    end

    it "ensures that an activity input > 32k data fails the workflow" do
      general_test(:task_list => "ActivityTaskLargeInput", :class_name => "ActivityTaskLargeInput")
      @workflow_class.class_eval do
        def entry_point
          activity.run_activity1("A"*50000)
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      worker = WorkflowWorker.new(@domain.client, @domain, "ActivityTaskLargeInput", @workflow_class)
      worker.register
      worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      last_event.attributes.reason.should == Utilities.validation_error_string("Workflow")
      last_event.attributes.details.should == "AWS::SimpleWorkflow::Errors::ValidationException"
    end


    it "ensures that a child workflow input > 32k fails the workflow" do
      general_test(:task_list => "ChildWorkflowInputTooLarge", :class_name => "ChildWorkflowInputTooLarge")
      @workflow_class.class_eval do
        workflow(:child) do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 300,
            default_task_list: "ChildWorkflowInputTooLarge",
            prefix_name: "ChildWorkflowInputTooLargeWorkflow"
          }
        end
        def entry_point
          child_client = AWS::Flow::workflow_client(nil, nil) { { from_class: "ChildWorkflowInputTooLargeWorkflow" } }
          child_client.child("A"*50000)
        end
        def child(input); end
      end

      worker = WorkflowWorker.new(@domain.client, @domain, "ChildWorkflowInputTooLarge", @workflow_class)
      worker.register
      workflow_execution = @my_workflow_client.start_execution
      worker.run_once

      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should == Utilities.validation_error_string("Workflow")
      workflow_execution.events.to_a.last.attributes.details.should == "AWS::SimpleWorkflow::Errors::ValidationException"
    end



    it "ensures that a child workflow exception > 32k fails the workflow correctly and truncates the stacktrace" do
      general_test(:task_list => "ChildWorkflowExceptionTooLarge", :class_name => "ChildWorkflowExceptionTooLarge")
      @workflow_class.class_eval do
        workflow(:child) do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 300,
            default_task_list: "ChildWorkflowExceptionTooLarge",
            prefix_name: "ChildWorkflowExceptionTooLargeWorkflow"
          }
        end
        def entry_point
          child_client = AWS::Flow::workflow_client(nil, nil) { { from_class: "ChildWorkflowExceptionTooLargeWorkflow" } }
          child_client.child
        end
        def child
          raise  ":" + "a" * 33000
        end
      end

      worker = WorkflowWorker.new(@domain.client, @domain, "ChildWorkflowExceptionTooLarge", @workflow_class)
      worker.register
      workflow_execution = @my_workflow_client.start_execution
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once

      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should include("[TRUNCATED]")
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.default_data_converter.load(details)
      exception.class.should == AWS::Flow::ChildWorkflowFailedException
      exception.cause.class.should == RuntimeError
    end


    it "ensures that a child child workflow exception > 32k fails the workflow correctly and truncates the stacktrace" do
      general_test(:task_list => "ChildChildWorkflowExceptionTooLarge", :class_name => "ChildChildWorkflowExceptionTooLarge")
      @workflow_class.class_eval do
        workflow(:child, :child_1) do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 300,
            default_task_list: "ChildChildWorkflowExceptionTooLarge",
            prefix_name: "ChildChildWorkflowExceptionTooLargeWorkflow"
          }
        end
        def entry_point
          child_client = AWS::Flow::workflow_client(nil, nil) { { from_class: "ChildChildWorkflowExceptionTooLargeWorkflow" } }
          child_client.child
        end
        def child
          child_1_client = AWS::Flow::workflow_client(nil, nil) { { from_class: "ChildChildWorkflowExceptionTooLargeWorkflow" } }
          child_1_client.child_1
        end
        def child_1
          raise  ":" + "a" * 33000
        end
      end
      worker = WorkflowWorker.new(@domain.client, @domain, "ChildChildWorkflowExceptionTooLarge", @workflow_class)
      worker.register
      workflow_execution = @my_workflow_client.start_execution
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once

      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should include("[TRUNCATED]")
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.default_data_converter.load(details)
      exception.class.should == AWS::Flow::ChildWorkflowFailedException
      exception.cause.class.should == AWS::Flow::ChildWorkflowFailedException
    end
  end

  it "ensures that activities can be processed with different configurations" do
    class TwoConfigActivity
      extend Activities
      activity :run_activity1 do
        {
          :default_task_heartbeat_timeout => "600",
          :default_task_list => "TwoConfigTaskList",
          :default_task_schedule_to_start_timeout => 120,
          :default_task_start_to_close_timeout => 120,
          :version => "1",
        }
      end
      def run_activity1
      end
    end

    class TwoConfigWorkflow
      extend Workflows
      activity_client(:activity) { { :from_class => TwoConfigActivity }}
      workflow :entry_point do
        {
          :version => 1,
          :default_execution_start_to_close_timeout => 30,
          :default_child_policy => "request_cancel",
          :default_task_list => "TwoConfigTaskList"
        }
      end
      def entry_point
        activity.run_activity1
        activity.run_activity1 { {:task_list => "other_config_task_list"} }
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "TwoConfigTaskList", TwoConfigWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, "TwoConfigTaskList", TwoConfigActivity) {{ :use_forking => false }}
    activity_worker_different_config = ActivityWorker.new(@swf.client, @domain, "other_config_task_list", TwoConfigActivity) {{ :use_forking => false }}
    my_workflow_client = workflow_client(@swf.client, @domain) {{:from_class => TwoConfigWorkflow}}

    worker.register
    activity_worker.register
    workflow_execution = my_workflow_client.start_execution
    worker.run_once
    activity_worker.run_once
    worker.run_once
    activity_worker_different_config.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last == "WorkflowExecutionCompleted"
  end

  it "ensures that not filling in details/reason for activity_task_failed is handled correctly" do
    general_test(:task_list => "ActivityTaskFailedManually", :class_name => "ActivityTaskFailedManually")
    $task_token = nil

    @activity_class.class_eval do
      activity :run_activityManual do
        {
          :default_task_heartbeat_timeout => "600",
          :default_task_list => task_list,
          :default_task_schedule_to_start_timeout => 120,
          :default_task_start_to_close_timeout => 120,
          :version => "1",
          :manual_completion => true
        }
      end
      def run_activityManual
        $task_token = activity_execution_context.task_token
      end
    end

    @workflow_class.class_eval do
      def entry_point
        begin
          activity.run_activityManual
        rescue Exception => e
          #pass
        end
      end
    end

    activity_worker = ActivityWorker.new(@swf.client, @domain, "ActivityTaskFailedManually", @activity_class) {{ :use_forking => false }}
    activity_worker.register

    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    activity_worker.run_once

    @swf.client.respond_activity_task_failed(:task_token => $task_token)

    @worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "ensures that raising inside a with_retry propagates up correctly" do
    general_test(:task_list => "WithRetryPropagation", :class_name => "WithRetryPropagation")
    @workflow_class.class_eval do
      def entry_point
        error = nil
        begin
          with_retry(:maximum_attempts => 1) { activity.run_activity1 }
        rescue ActivityTaskFailedException => e
          error = e
        end
        return error
      end
    end
    @activity_class.class_eval do
      def run_activity1; raise "Error!"; end
    end

    @forking_executor = ForkingExecutor.new(:max_workers => 3)
    @forking_executor.execute { @worker.start }
    @forking_executor.execute { @activity_worker.start }
    sleep 5

    workflow_execution = @my_workflow_client.start_execution

    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
    workflow_execution.events.to_a[-1].attributes.result.should =~ /Error!/

  end

  it "ensures that backtraces are set correctly with yaml" do
    general_test(:task_list => "Backtrace_test", :class_name => "BacktraceTest")
    @workflow_class.class_eval do
      def entry_point
        begin
          activity.run_activity1
        rescue ActivityTaskFailedException => e
          error = e
          e.backtrace.nil?.should == false
        end
        return error.backtrace
      end
    end
    @activity_class.class_eval do
      def run_activity1
        raise "Error!"
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once
    @worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.to_a[-1].attributes.result.should =~ /Error!/
  end
  describe "Handle_ tests" do
    # This also effectively tests "RequestCancelExternalWorkflowExecutionInitiated"

    # TODO: These three tests will sometimes fail, seemingly at random. We need to fix this.
    it "ensures that handle_child_workflow_execution_canceled is correct" do
      class OtherCancellationChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_child_cancelled_workflow", :default_execution_start_to_close_timeout => 600} }
        def entry_point(arg)
          create_timer(20)
        end
      end
      class BadCancellationChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_parent_cancelled_workflow", :default_execution_start_to_close_timeout => 600} }

        def entry_point(arg)
          domain = get_test_domain
          client = workflow_client(domain.client, domain) { {:from_class => "OtherCancellationChildWorkflow"} }
          workflow_future = client.send_async(:start_execution, 5)
          client.request_cancel_workflow_execution(workflow_future)
        end
      end
      child_worker = WorkflowWorker.new(@swf.client, @domain, "new_child_cancelled_workflow", OtherCancellationChildWorkflow)
      child_worker.register
      parent_worker = WorkflowWorker.new(@swf.client, @domain, "new_parent_cancelled_workflow", BadCancellationChildWorkflow)
      parent_worker.register
      client = workflow_client(@swf.client, @domain) { {:from_class => "BadCancellationChildWorkflow"} }
      workflow_execution = client.entry_point(5)

      parent_worker.run_once
      child_worker.run_once
      parent_worker.run_once

      wait_for_decision(workflow_execution)
      workflow_execution.events.map(&:event_type).should include "ExternalWorkflowExecutionCancelRequested"
      child_worker.run_once

      wait_for_decision(workflow_execution, "ChildWorkflowExecutionCanceled")
      workflow_execution.events.map(&:event_type).should include "ChildWorkflowExecutionCanceled"
      parent_worker.run_once

      wait_for_execution(workflow_execution)
      workflow_execution.events.to_a.last.attributes.details.should =~ /AWS::Flow::Core::Cancellation/
    end

    it "ensures that handle_child_workflow_terminated is handled correctly" do
      class OtherTerminationChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_child_terminated_workflow", :default_execution_start_to_close_timeout => 600} }

        def entry_point(arg)
          create_timer(5)
        end

      end
      $workflow_id = nil
      class BadTerminationChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_parent_terminated_workflow", :default_execution_start_to_close_timeout => 600} }
        def other_entry_point
        end

        def entry_point(arg)
          domain = get_test_domain
          client = workflow_client(domain.client, domain) { {:from_class => "OtherTerminationChildWorkflow"} }
          workflow_future = client.send_async(:start_execution, 5)
          $workflow_id = workflow_future.workflow_execution.workflow_id.get
        end
      end
      worker2 = WorkflowWorker.new(@swf.client, @domain, "new_child_terminated_workflow", OtherTerminationChildWorkflow)
      worker2.register
      worker = WorkflowWorker.new(@swf.client, @domain, "new_parent_terminated_workflow", BadTerminationChildWorkflow)
      worker.register
      client = workflow_client(@swf.client, @domain) { {:from_class => "BadTerminationChildWorkflow"} }
      workflow_execution = client.entry_point(5)

      worker.run_once
      worker2.run_once
      wait_for_decision(workflow_execution)
      @swf.client.terminate_workflow_execution({:workflow_id => $workflow_id, :domain => @domain.name})
      wait_for_decision(workflow_execution, "ChildWorkflowExecutionTerminated")
      worker.run_once
      wait_for_execution(workflow_execution)
      validate_execution_failed(workflow_execution)
      workflow_execution.events.to_a.last.attributes.details.should =~ /AWS::Flow::ChildWorkflowTerminatedException/
    end

    it "ensures that handle_child_workflow_timed_out is handled correctly" do
      class OtherTimedOutChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_child_timed_out_workflow", :default_execution_start_to_close_timeout => 5} }

        def entry_point(arg)
          create_timer(5)
        end

      end
      $workflow_id = nil
      class BadTimedOutChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_parent_timed_out_workflow", :default_execution_start_to_close_timeout => 600} }
        def other_entry_point
        end

        def entry_point(arg)
          domain = get_test_domain
          client = workflow_client(domain.client, domain) { {:from_class => "OtherTimedOutChildWorkflow"} }
          workflow_future = client.send_async(:start_execution, 5)
          $workflow_id = workflow_future.workflow_execution.workflow_id.get
        end
      end
      worker2 = WorkflowWorker.new(@swf.client, @domain, "new_child_timed_out_workflow", OtherTimedOutChildWorkflow)
      worker2.register
      worker = WorkflowWorker.new(@swf.client, @domain, "new_parent_timed_out_workflow", BadTimedOutChildWorkflow)
      worker.register
      client = workflow_client(@swf.client, @domain) { {:from_class => "BadTimedOutChildWorkflow"} }
      workflow_execution = client.entry_point(5)
      worker.run_once
      sleep 8
      worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.to_a.last.attributes.details.should =~ /AWS::Flow::ChildWorkflowTimedOutException/
    end

    it "ensures that handle_start_child_workflow_execution_failed is fine" do
      general_test(:task_list => "handle_start_child_workflow_execution_failed", :class_name => "HandleStartChildWorkflowExecutionFailed")
      class FooBar
        extend Workflows
        workflow :bad_workflow do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: "handle_start_child_workflow_execution_failed_child"
          }
        end
        def bad_workflow
          raise "Child workflow died"
        end
      end
      @workflow_class.class_eval do
        def entry_point
          domain = get_test_domain
          wf = AWS::Flow.workflow_client(domain.client, domain) { { from_class: "FooBar" } }
          wf.start_execution
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      child_worker = WorkflowWorker.new(@domain.client, @domain, "handle_start_child_workflow_execution_failed_child", FooBar)
      child_worker.register
      @worker.run_once
      child_worker.run_once
      @worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
      # Make sure this is actually caused by a child workflow failed
      workflow_execution.events.to_a.last.attributes.details.should =~ /ChildWorkflowFailed/
    end

    it "ensures that handle_timer_canceled is fine" do
      general_test(:task_list => "handle_timer_canceled", :class_name => "HandleTimerCanceled")
      @workflow_class.class_eval do
        def entry_point
          bre = error_handler do |t|
            t.begin do
              create_timer(100)
            end
            t.rescue(CancellationException) {}
          end
          create_timer(1)
          bre.cancel(CancellationException.new)
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_history = workflow_execution.events.map(&:event_type)
      workflow_history.count("TimerCanceled").should == 1
      workflow_history.count("WorkflowExecutionCompleted").should == 1
    end

    it "ensures that activities under a bre get cancelled" do
      general_test(:task_list => "activite under bre", :class_name => "ActivitiesUnderBRE")
      @workflow_class.class_eval do
        def entry_point
          bre = error_handler do |t|
            t.begin { activity.send_async(:run_activity1) }
          end
          create_timer(1)
          bre.cancel(CancellationException.new)
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @worker.run_once
      workflow_execution.events.map(&:event_type).count("ActivityTaskCancelRequested").should == 1
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.to_a.last.attributes.reason.should == "AWS::Flow::Core::CancellationException"
    end

    it "ensures that start_timer_failed is handled correctly" do
      general_test(:task_list => "start_timer_failed", :class_name => "StartTimerFailed")
    end

    it "ensures that get_state_method works fine" do
      general_test(:task_list => "get_state_method", :class_name => "GetStateTest")
      @workflow_class.class_eval do
        get_state_method :get_state_test
        def get_state_test
          "This is the workflow state!"
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      worker = WorkflowWorker.new(@swf.client, @domain, "get_state_method", @workflow_class)
      worker.run_once
      workflow_execution.events.to_a[3].attributes.execution_context.should =~ /This is the workflow state!/
    end

    it "ensures that handle_request_cancel_activity_task_failed works" do
      general_test(:task_list => "handle_request_cancel_activity_task_failed", :class_name => "HandleRCActivityTaskFailed")
      class AsyncDecider
        alias_method :old_handle_request_cancel_activity_task_failed, :handle_request_cancel_activity_task_failed
        # We have to replace this method, otherwise we'd fail on handling the
        # error because we can't find the decision in the decision_map. There
        # is similar behavior in javaflow
        def handle_request_cancel_activity_task_failed(event)
          event_double = SimpleTestHistoryEvent.new("Activity1")
          self.send(:old_handle_request_cancel_activity_task_failed, event_double)
        end
      end

      class ActivityDecisionStateMachine
        alias_method :old_create_request_cancel_activity_task_decision, :create_request_cancel_activity_task_decision
        def create_request_cancel_activity_task_decision
          { :decision_type => "RequestCancelActivityTask",
            :request_cancel_activity_task_decision_attributes => {:activity_id => "bad_id"} }
        end
      end

      @workflow_class.class_eval do
        def entry_point
          future = activity.send_async(:run_activity1)
          create_timer(1)
          activity.request_cancel_activity_task(future)
        end
      end


      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @worker.run_once
      @worker.run_once

      # In the future, we might want to verify that it transitions the state
      # machine properly, but at a base, it should not fail the workflow.
      workflow_execution.events.map(&:event_type).last.should == "DecisionTaskCompleted"
      class AsyncDecider
        alias_method :handle_request_cancel_activity_task_failed, :old_handle_request_cancel_activity_task_failed
      end
      class ActivityDecisionStateMachine
        alias_method  :create_request_cancel_activity_task_decision,:old_create_request_cancel_activity_task_decision
      end
    end
  end


  describe "General Testing" do
    it "makes sure that you can register a workflow with defaults" do
      general_test(:task_list => "workflow registration", :class_name => "WFRegister")
      @workflow_class.class_eval do
        workflow(:test_method) do
          {
            :version => 1,
            :default_task_list => "foo",
            :default_execution_start_to_close_timeout => 30,
            :default_child_policy => "request_cancel"
          }
        end
      end
      worker = WorkflowWorker.new(@swf.client, @domain, "test", @workflow_class)

      worker.register
      sleep 5
      @domain.workflow_types.to_a.find{|x| x.name == "#{@workflow_class}.test_method"}.should_not be_nil
    end

    it "tests that workflow clock gives the same value over multiple replays" do
      general_test(:task_list => "replaying_test", :class_name => "Replaying_clock")
      @workflow_class.class_eval do
        def entry_point

        end
      end
    end
    it "tests to make sure we set replaying correctly" do
      general_test(:task_list => "is_replaying", :class_name => "Replaying")
      @workflow_class.class_eval do
        def entry_point
          activity.run_activity1
          decision_context.workflow_clock.replaying
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      # TODO Kinda hacky, we should be using the workflow_class's data_converter
      workflow_execution.events.to_a.last.attributes[:result].include? "false"
    end

    it "makes sure that having a workflow with outstanding activities will close if one fails" do
      general_test(:task_list => "outstanding_activity_failure", :class_name => "OutstandingActivityFailure")
      @workflow_class.class_eval do
        def entry_point
          activity.send_async(:run_activity1)
          task do
            activity.run_activity2 {{:task_list => "foo"}}
          end
        end
      end
      @activity_class.class_eval do
        def run_activity1
          raise "simulated error"
        end
        def run_activity2

        end
      end

      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }

      workflow_execution = @my_workflow_client.start_execution

      wait_for_execution(workflow_execution)

      history = workflow_execution.events.map(&:event_type)
      history.last.should == "WorkflowExecutionFailed"

      history.should include "ActivityTaskCancelRequested"
      #@worker.run_once
      #@activity_worker.run_once
      #wait_for_decision(workflow_execution)
      #@worker.run_once
      #wait_for_decision(workflow_execution)
      #@worker.run_once

      #wait_for_execution(workflow_execution)
      #history = workflow_execution.events.map(&:event_type)
      #history.last.should == "WorkflowExecutionFailed"
      ## Should look something like: ["WorkflowExecutionStarted",
      # "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted",
      # "ActivityTaskScheduled", "ActivityTaskScheduled", "ActivityTaskStarted",
      # "ActivityTaskFailed", "DecisionTaskScheduled", "DecisionTaskStarted",
      # "DecisionTaskCompleted", "ActivityTaskCancelRequested",
      # "ActivityTaskCanceled", "DecisionTaskScheduled", "DecisionTaskStarted",
      # "DecisionTaskCompleted", "WorkflowExecutionFailed"]
      #history.should include "ActivityTaskCancelRequested"
    end

    it "makes sure that you can use the :exponential_retry key" do
      general_test(:task_list => "exponential_retry_key", :class_name => "ExponentialRetryKey")
      @workflow_class.class_eval do
        def entry_point
          activity.run_activity1  do
            {
              :exponential_retry => {:maximum_attempts => 1},
              :schedule_to_start_timeout => 1
            }
          end
        end
      end
      worker = WorkflowWorker.new(@domain.client, @domain, "exponential_retry_key", @workflow_class)
      workflow_execution = @my_workflow_client.start_execution
      4.times { worker.run_once }
      wait_for_execution(workflow_execution)
      workflow_execution.events.to_a.last.event_type.should == "WorkflowExecutionFailed"
    end

    it "ensures that you can use an arbitrary logger" do
      testing_file = "/tmp/testing"
      general_test(:task_list => "arbitrary logger", :class_name => "ArbitraryLogger")
      File.delete(testing_file) if File.exists? testing_file
      logger = Logger.new(testing_file)
      logger.level = Logger::DEBUG
      worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary logger", @workflow_class) { {:logger => logger} }
      activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary logger", @activity_class) { { :logger => logger, :execution_workers => 20, :use_forking => false} }
      workflow_execution = @my_workflow_client.start_execution
      worker.run_once
      file = File.open(testing_file)
      # The file should have something in it(i.e., not blank)
      file.read.should_not =~ /""/
      # Clear the file so we can be sure the activity worker works too
      File.open(testing_file, 'w') {}
      file = File.open(testing_file).read.should_not =~ /""/
      activity_worker.run_once
    end
    it "makes sure that raising an exception in the wf definition is fine" do
      general_test(:task_list => "exception in wf", :class_name => "WFException")
      @workflow_class.class_eval do
        def entry_point
          raise Exception
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
    end
    it "makes sure that the return value of an activity is directly useable" do
      general_test(:task_list => "return value activity", :class_name => "ActivityReturn")
      @activity_class.class_eval do
        def run_activity1
          return 5
        end
      end
      @workflow_class.class_eval do
        def entry_point
          x = activity.run_activity1
          x.should == 5
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
    end
    it "makes sure that there is an easy way to get workflow_id" do
      general_test(:task_list => "workflow_id method", :class_name => "WFID")
      @workflow_class.class_eval do
        def entry_point
          workflow_id
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
    end
    it "makes sure that arguments get passed correctly" do
      task_list = "argument_task_list"
      class TaskListProvider
        class << self; attr_accessor :task_list; end
      end
      TaskListProvider.task_list = task_list

      class ArgumentActivity < TaskListProvider
        extend AWS::Flow::Activities
        activity :run_activity1 do
          {
            version: "1.0",
            default_task_list: self.task_list,
            default_task_schedule_to_close_timeout: "120",
            default_task_schedule_to_start_timeout: "60",
            default_task_start_to_close_timeout: "60"
          }
        end

        def run_activity1(arg)
          arg.should == 5
          arg + 1
        end
      end
      class ArgumentWorkflow < TaskListProvider
        extend AWS::Flow::Workflows
        workflow :entry_point do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: self.task_list,
            default_task_start_to_close_timeout: 10,
            default_child_policy: :request_cancel,
          }
        end
        activity_client(:activity) { { from_class: "ArgumentActivity" } }
        def entry_point(arg)
          arg.should == 5
          activity.run_activity1(arg)
        end
      end

      worker = WorkflowWorker.new(@domain.client, @domain, task_list, ArgumentWorkflow)
      activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, ArgumentActivity)
      worker.register
      activity_worker.register
      client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "ArgumentWorkflow" } }

      workflow_execution = client.start_execution(5)
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { worker.start }
      @forking_executor.execute { activity_worker.start }

      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).should ==
        ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
      workflow_execution.events.to_a.last.attributes[:result].should =~ /6/
      @forking_executor.shutdown(1)
    end

    it "makes sure that a standard error works" do
      general_test(:task_list => "regular error raise", :class_name => "StandardError")
      @workflow_class.class_eval do
        def entry_point
          activity.run_activity1
        end
      end

      @activity_class.class_eval do
        def run_activity1
          raise "This is a simulated error"
        end
      end
      workflow_execution = @my_workflow_client.start_execution

      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)

      workflow_execution.events.map(&:event_type).count("WorkflowExecutionFailed").should ==  1
    end


    it "ensures that exceptions to include functions properly" do
      general_test(:task_list => "exceptions_to_include", :class_name => "ExceptionsToInclude")
      @workflow_class.class_eval do
        def entry_point
          activity.exponential_retry(:run_activity1) { {:exceptions_to_exclude => [SecurityError] } }
        end
      end
      @activity_class.class_eval do
        def run_activity1
          raise SecurityError
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
    end
    class YAMLPlusOne
      def dump(obj)
        obj.to_yaml + "1"
      end
      def load(source)
        source = source[0..-2]
        YAML.load source
      end
    end
    it "makes sure you can set a different converter for activities" do
      class DifferentActivityConverterActivity
        extend Activities
        activity :test_converter do
          {
            :data_converter => YAMLPlusOne.new,
            :default_task_list => "different converter activity",
            :version => "1",
            :default_task_heartbeat_timeout => "600",
            :default_task_schedule_to_close_timeout => "60",
            :default_task_schedule_to_start_timeout => "60",
            :default_task_start_to_close_timeout => "60",
          }
        end
        def test_converter
          "this"
        end
      end
      activity_worker = ActivityWorker.new(@swf.client, @domain,"different converter activity", DifferentActivityConverterActivity)
      class DifferentActivityConverterWorkflow
        extend Workflows
        workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "different converter activity"} }
        activity_client(:activity) { { :from_class => DifferentActivityConverterActivity } }
        def entry_point
          activity.test_converter
        end
      end
      worker = WorkflowWorker.new(@swf.client, @domain, "different converter activity", DifferentActivityConverterWorkflow)
      my_workflow_client = workflow_client(@swf.client, @domain) { { :from_class => DifferentActivityConverterWorkflow } }
      worker.register
      activity_worker.register
      workflow_execution = my_workflow_client.start_execution
      worker.run_once
      activity_worker.run_once
      worker.run_once
      activity_completed_index = workflow_execution.events.map(&:event_type).index("ActivityTaskCompleted")
      workflow_execution.events.to_a[activity_completed_index].attributes.result.should =~ /1\z/
    end

    it "makes sure that timers work" do
      general_test(:task_list => "Timer_task_list", :class_name => "Timer")
      @workflow_class.class_eval do
        def entry_point
          create_timer(5)
          activity.run_activity1
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).should ==
        ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "TimerStarted", "TimerFired", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
      @forking_executor.shutdown(1)
    end


    it "makes sure that timers can have a block passed in" do
      general_test(:task_list => "timer_with_block", :class_name => "TimerBlock")
      @workflow_class.class_eval do
        def entry_point
          create_timer(5) { activity.run_activity1 }
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }
      wait_for_execution(workflow_execution)
      @forking_executor.shutdown(1)
      workflow_execution.events.map(&:event_type).should ==
        ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "TimerStarted", "TimerFired", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
    end

    it "makes sure that you can have an asynchronous timer" do
      general_test(:task_list => "async_timer", :class_name => "Async")
      @workflow_class.class_eval do
        def entry_point
          create_timer_async(5)
          activity.run_activity1
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }

      wait_for_execution(workflow_execution)

      @forking_executor.shutdown(1)
      after_first_decision = workflow_execution.events.to_a.slice(4, 2).map(&:event_type)
      after_first_decision.should include "TimerStarted"
      after_first_decision.should include "ActivityTaskScheduled"
    end
    it "makes sure that you can have an asynchronous timer with a block" do
      general_test(:task_list => "async_timer_with_block", :class_name => "AsyncBlock")
      @workflow_class.class_eval do
        def entry_point
          create_timer_async(5) { activity.run_activity1 }
          activity.run_activity2
        end
      end
      @activity_worker = ActivityWorker.new(@swf.client, @domain, "async timer with block", AsyncBlockActivity)
      @activity_worker.register
      workflow_execution = @my_workflow_client.start_execution
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }
      wait_for_execution(workflow_execution)
      @forking_executor.shutdown(1)
      activity_scheduled = workflow_execution.events.to_a.each_with_index.map{|x, i| i if x.event_type == "ActivityTaskScheduled"}.compact
      history_events = workflow_execution.events.to_a
      history_events[activity_scheduled.first - 1].event_type == "TimerStarted" ||
        history_events[activity_scheduled.first + 1].event_type == "TimerStarted"
      history_events[activity_scheduled.first].attributes[:activity_type].name.should == "AsyncBlockActivity.run_activity2"
      history_events[activity_scheduled.last].attributes[:activity_type].name.should == "AsyncBlockActivity.run_activity1"
    end

    describe "Child Workflows" do

      it "is a basic child workflow test" do

        class ChildWorkflowsTestChildWorkflow
          extend AWS::Flow::Workflows
          workflow :child do
            {
              version: "1.0",
              default_execution_start_to_close_timeout: 600,
              default_task_start_to_close_timeout: 10,
            }
          end
          def child; sleep 1; end
        end

        class ChildWorkflowsTestParentWorkflow
          extend AWS::Flow::Workflows
          workflow :parent do
            {
              version: "1.0",
              default_execution_start_to_close_timeout: 600,
              default_task_list: "test"
            }
          end
          def parent
            domain = get_test_domain
            client = AWS::Flow::workflow_client(domain.client, domain) { { from_class: "ChildWorkflowsTestChildWorkflow", task_list: "test2" } } 
            client.send_async(:start_execution)
            client.send_async(:start_execution)
          end
        end

        parent_client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "ChildWorkflowsTestParentWorkflow" } }
        @child_worker = WorkflowWorker.new(@domain.client, @domain, "test2", ChildWorkflowsTestChildWorkflow)
        @parent_worker = WorkflowWorker.new(@domain.client, @domain, "test", ChildWorkflowsTestParentWorkflow)

        @forking_executor = ForkingExecutor.new(:max_workers => 3)
        @forking_executor.execute { @parent_worker.start }
        @forking_executor.execute { @child_worker.start }
        @forking_executor.execute { @child_worker.start }
        sleep 2

        workflow_execution = parent_client.start_execution
        wait_for_execution(workflow_execution)

        events = workflow_execution.events.map(&:event_type)
        workflow_execution.events.to_a.last.attributes.result.should_not =~ /secret_access_key/
        events.should include("ChildWorkflowExecutionStarted", "ChildWorkflowExecutionCompleted", "WorkflowExecutionCompleted")
      end

      it "ensures that workflow clock provides at least basic support for current_time_millis" do
        general_test(:task_list => "workflow_clock_basic", :class_name => "WorkflowClockBasic")

        @workflow_class.class_eval do
          class << self
            attr_accessor :time_hash, :replaying_hash
          end
          def entry_point
            def record_point(name)
              self.class.replaying_hash[name] << decision_context.workflow_clock.replaying
              self.class.time_hash[name] << decision_context.workflow_clock.current_time
            end
            record_point(:first)
            create_timer(5)
            record_point(:second)
            create_timer(3)
            record_point(:third)
          end
        end
        @workflow_class.time_hash = Hash.new {|hash, key| hash[key] = []}
        @workflow_class.replaying_hash =  Hash.new {|hash, key| hash[key] = []}
        workflow_execution = @my_workflow_client.start_execution
        3.times { @worker.run_once }
        # Maintain the invariant that you should *not* be replaying only once
        @workflow_class.replaying_hash.values.each {|x| x.count(false).should be 1}
        # Maintain the invariant that at the same point in the code,
        # replay_current_time_millis will return the same value
        @workflow_class.time_hash.values.each do |array|
          array.reduce {|first, second| first if first.should == second}
        end
      end

      it "ensures that a child workflow failing raises a ChildWorkflowExecutionFailed" do
        class FailingChildChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "failing_child_workflow", :default_execution_start_to_close_timeout => 600} }
          def entry_point(arg)
            raise "simulated error"
          end
        end
        class FailingHostChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "failing_parent_workflow", :default_execution_start_to_close_timeout => 600} }
          def other_entry_point
          end

          def entry_point(arg)
            domain = get_test_domain
            client = workflow_client(domain.client, domain) { {:from_class => "FailingChildChildWorkflow"} }
            begin
              client.start_execution(5)
            rescue Exception => e
              #pass
            end
          end
        end
        worker2 = WorkflowWorker.new(@swf.client, @domain, "failing_child_workflow", FailingChildChildWorkflow)
        worker2.register
        worker = WorkflowWorker.new(@swf.client, @domain, "failing_parent_workflow", FailingHostChildWorkflow)
        worker.register
        client = workflow_client(@swf.client, @domain) { {:from_class => "FailingHostChildWorkflow"} }
        workflow_execution = client.entry_point(5)
        worker.run_once
        worker2.run_once
        worker2.run_once
        worker.run_once
        events = workflow_execution.events.map(&:event_type)
        events.should include "ChildWorkflowExecutionFailed"
        events.should include "WorkflowExecutionCompleted"
      end

      it "ensures that a child workflow can use data_converter correctly" do
        class DataConverterChildChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "data_converter_child_workflow", :default_execution_start_to_close_timeout => 600, :data_converter => YAMLPlusOne.new} }
          def entry_point(arg)
            return arg + 1
          end
        end
        class DataConverterHostChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "data_converter_parent_workflow", :default_execution_start_to_close_timeout => 600} }
          def other_entry_point
          end

          def entry_point(arg)
            domain = get_test_domain
            client = workflow_client(domain.client, domain) { {:from_class => "DataConverterChildChildWorkflow"} }
            task { client.start_execution(5) }
          end
        end
        worker2 = WorkflowWorker.new(@swf.client, @domain, "data_converter_child_workflow", DataConverterChildChildWorkflow)
        worker2.register
        worker = WorkflowWorker.new(@swf.client, @domain, "data_converter_parent_workflow", DataConverterHostChildWorkflow)
        worker.register

        client = workflow_client(@swf.client, @domain) { {:from_class => "DataConverterHostChildWorkflow"} }
        workflow_execution = client.entry_point(5)
        worker.run_once
        worker2.run_once
        worker.run_once
        # We have to find the index dynamically, because due to how scheduled/starts work, it isn't necessarily in the same place in our history.
        child_execution_completed_index = workflow_execution.events.map(&:event_type).index("ChildWorkflowExecutionCompleted")

        workflow_execution.events.to_a[child_execution_completed_index].attributes.result.should =~ /1\z/
      end

      it "makes sure that the new way of doing child workflows works" do
        class OtherNewChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "new_child_workflow", :default_execution_start_to_close_timeout => 600} }
          def entry_point(arg)
            sleep 2
          end

        end
        class BadNewChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "new_parent_workflow", :default_execution_start_to_close_timeout => 600} }
          def other_entry_point
          end

          def entry_point(arg)
            domain = get_test_domain
            client = workflow_client(domain.client, domain) { {:from_class => "OtherNewChildWorkflow"} }
            task { client.start_execution(5) }
            task { client.start_execution(5) }
          end
        end
        worker2 = WorkflowWorker.new(@swf.client, @domain, "new_child_workflow", OtherNewChildWorkflow)
        worker2.register
        worker = WorkflowWorker.new(@swf.client, @domain, "new_parent_workflow", BadNewChildWorkflow)
        worker.register
        client = workflow_client(@swf.client, @domain) { {:from_class => "BadNewChildWorkflow"} }
        workflow_execution = client.entry_point(5)
        worker.run_once
        worker2.run_once
        worker2.run_once
        worker.run_once
        worker.run_once if workflow_execution.events.map(&:event_type).last == "DecisionTaskCompleted"
        events = workflow_execution.events.map(&:event_type)
        events.should include "ChildWorkflowExecutionStarted"
        events.should include "ChildWorkflowExecutionCompleted"
        events.should include "WorkflowExecutionCompleted"
      end
    end
    it "makes sure that you can use retries_per_exception" do
      general_test(:task_list => "retries_per_exception", :class_name => "RetriesPerException")
      @activity_class.class_eval do
        def run_activity1
          raise StandardError
        end
      end
      @workflow_class.class_eval do
        activity_client :activity do |options|
          options.default_task_heartbeat_timeout = "600"
          options.default_task_list = self.task_list
          options.default_task_schedule_to_close_timeout = "5"
          options.default_task_schedule_to_start_timeout = "5"
          options.default_task_start_to_close_timeout = "5"
          options.version = "1"
          options.prefix_name = self.activity_class.to_s
        end
        def entry_point
          activity.exponential_retry(:run_activity1) do |o|
            o.retries_per_exception = {
              ActivityTaskTimedOutException => Float::INFINITY,
              ActivityTaskFailedException => 3
            }
          end
        end
      end

      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      @worker.run_once
      @activity_worker.run_once

      @worker.run_once
      @worker.run_once
      @activity_worker.run_once

      @worker.run_once

      wait_for_execution(workflow_execution)
      workflow_history = workflow_execution.events.map(&:event_type)
      workflow_history.count("ActivityTaskFailed").should == 3

      workflow_history.count("WorkflowExecutionFailed").should == 1
    end

    it "makes sure that continueAsNew within a timer works" do
      general_test(:task_list => "continue_as_new_timer", :class_name => "ContinueAsNewTimer")
      @workflow_class.class_eval do
        def entry_point
          create_timer(5) do
            continue_as_new do |options|
              options.execution_start_to_close_timeout = 600
              options.task_list = "continue_as_new_timer"
              options.tag_list = []
              options.version = "1"
            end
          end
        end
      end
      @workflow_execution = @my_workflow_client.entry_point
      @worker.run_once
      @worker.run_once
      @workflow_execution.events.map(&:event_type).last.should ==
        "WorkflowExecutionContinuedAsNew"
      @workflow_execution.status.should ==
        :continued_as_new
    end

    it "ensures that you can write a continue_as_new with less configuration" do
      general_test(:task_list => "continue_as_new_config", :class_name => "ContinueAsNewConfiguration")
      @workflow_class.class_eval do
        def entry_point
          continue_as_new
        end
      end
      @workflow_execution = @my_workflow_client.entry_point
      @worker.run_once
      @workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionContinuedAsNew"
    end

    it "makes sure that basic continueAsNew works" do
      general_test(:task_list => "continue_as_new", :class_name => "ContinueAsNew")
      @workflow_class.class_eval do
        def entry_point
          continue_as_new do |options|
            options.workflow_name = @workflow_class.to_s
            options.execution_method = :entry_point
            options.execution_start_to_close_timeout = 600
            options.task_list = "continue_as_new"
            options.tag_list = []
            options.task_start_to_close_timeout = 30
            options.child_policy = "REQUEST_CANCEL"
            options.version = "1"
          end
        end
      end

      @workflow_execution = @my_workflow_client.entry_point
      @worker.run_once
      @workflow_execution.events.map(&:event_type).last.should ==
        "WorkflowExecutionContinuedAsNew"
      @workflow_execution.status.should ==
        :continued_as_new
    end

    it "makes sure that exponential retry returns values correctly" do
      class ExponentialActivity
        extend AWS::Flow::Activity
        activity :run_activity1 do
          {
            version: "1.0",
            default_task_list: "exponential_test_return_task_list",
            default_task_schedule_to_close_timeout: "30",
            default_task_schedule_to_start_timeout: "15",
            default_task_start_to_close_timeout: "15",
          }
        end
        def run_activity1
          return 5
        end
      end

      class ExponentialWorkflow
        extend AWS::Flow::Workflows
        workflow :start do
          {
            version: "1.0",
            default_task_list: "exponential_test_return_task_list",
            default_execution_start_to_close_timeout: 600,
            default_task_start_to_close_timeout: 60,
            default_child_policy: "REQUEST_CANCEL"
          }
        end
        activity_client(:activity) { { from_class: "ExponentialActivity" } }
        def start
          x = activity.exponential_retry(:run_activity1) {
            {
              retries_per_exception: {
                ActivityTaskTimedOutException => Float::INFINITY,
                ActivityTaskFailedException => 3
              }
            }
          }
          x.should == 5
        end
      end

      task_list = "exponential_test_return_task_list"

      worker = WorkflowWorker.new(@domain.client, @domain, task_list, ExponentialWorkflow)
      activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, ExponentialActivity)
      worker.register
      activity_worker.register
      client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "ExponentialWorkflow" } }
      workflow_execution = client.start_execution
      worker.run_once
      activity_worker.run_once
      activity_worker.run_once unless workflow_execution.events.map(&:event_type).include? "ActivityTaskCompleted"
      worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
    end

    it "makes sure that signals work correctly" do

      class SignalWorkflow
        extend AWS::Flow::Workflows
        workflow :entry_point do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
          }
        end

        signal :this_signal
        def this_signal(input)
          @wait.broadcast
          @input = input
        end

        def entry_point
          @input = "bad_input"
          @wait ||= FiberConditionVariable.new
          @wait.wait
          @input.should =~ /new input!/
        end

      end

      worker = build_worker(SignalWorkflow, "SignalWorkflow_tasklist")
      worker.register
      client = build_client(from_class: "SignalWorkflow")

      workflow_execution = client.start_execution

      worker.run_once
      client.signal_workflow_execution("this_signal", workflow_execution) { {:input => "new input!"}}
      worker.run_once

      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
    end

    it "makes sure that internal signalling works" do

      class SignallingActivity
        extend AWS::Flow::Activities
        activity :run_activity1 do
          {
            version: "1.0",
            default_task_list: "SignalWorker_activity_tasklist",
            default_task_schedule_to_close_timeout: "10",
            default_task_schedule_to_start_timeout: "10",
            default_task_start_to_close_timeout: "8",
          }
        end
        def run_activity1
          return 5
        end
      end

      class SignalInternalWorkflow
        extend AWS::Flow::Workflows

        workflow :entry_point do
          {
            version: "1.0",
            default_task_list: "SignalWorkflow_tasklist",
            default_execution_start_to_close_timeout: 600,
            default_child_policy: :request_cancel,
          }
        end

        activity_client(:activity) { { from_class: "SignallingActivity" } }

        def entry_point
          client = build_client(from_class: "SignaleeWorkflow")
          workflow_future = client.send_async(:start_execution)
          activity.run_activity1
          client.signal_workflow_execution(:this_signal, workflow_future)
        end
      end

      class SignaleeWorkflow
        extend AWS::Flow::Workflows

        workflow :entry_point do
          {
            version: "1.0",
            default_task_list: "WorkflowSignalee_tasklist",
            default_execution_start_to_close_timeout: 600,
            default_child_policy: :request_cancel,
          }
        end
        signal :this_signal

        def entry_point
          @wait ||= FiberConditionVariable.new
          @wait.wait
        end
        def this_signal
          @wait.broadcast
        end
      end

      worker_signalee = build_worker(SignaleeWorkflow, "WorkflowSignalee_tasklist")
      worker_signaler = build_worker(SignalInternalWorkflow, "SignalWorkflow_tasklist")
      activity_worker = build_worker(SignallingActivity, "SignalWorker_activity_tasklist")
      worker_signaler.register
      worker_signalee.register
      activity_worker.register

      client = build_client(from_class: "SignalInternalWorkflow")
      workflow_execution = client.start_execution

      worker_signaler.run_once
      worker_signalee.run_once
      activity_worker.run_once
      wait_for_decision(workflow_execution, "ActivityTaskCompleted")

      worker_signaler.run_once
      wait_for_decision(workflow_execution)

      worker_signalee.run_once
      wait_for_decision(workflow_execution, "ChildWorkflowExecutionCompleted")

      worker_signaler.run_once
      wait_for_execution(workflow_execution)

      workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1

    end
  end

  it "makes sure that an error fails an activity" do
    task_list = "exponential_retry_test"
    GeneralActivity.task_list = task_list
    MyWorkflow.task_list = task_list
    class GeneralActivity
      class << self
        attr_accessor :task_list
      end
      extend Activity
      activity :run_activity1 do |options|
        options.default_task_list = GeneralActivity.task_list
        options.default_task_schedule_to_start_timeout = "600"
        options.default_task_start_to_close_timeout = "600"
        options.version = "1"
      end
      def run_activity1
        raise "error"
      end
    end
    class MyWorkflow
      class << self
        attr_accessor :task_list
      end
      extend AWS::Flow::Workflows
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "GeneralActivity"
        options.version = "1"
        options.default_task_list = MyWorkflow.task_list
        options.default_task_schedule_to_start_timeout = "60"
        options.default_task_start_to_close_timeout = "60"
      end
      entry_point :entry_point
      def entry_point(arg)
        activity.run_activity1
      end
    end

    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(MyWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(GeneralActivity)
    worker.register
    activity_worker.register
    my_workflow_factory = workflow_factory(@swf.client, @domain) do |options|
      options.workflow_name = "MyWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_list = task_list
      options.task_start_to_close_timeout = 120
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client


    @forking_executor = ForkingExecutor.new(:max_workers => 3)
    @forking_executor.execute { worker.start }
    @forking_executor.execute { activity_worker.start }
    workflow_execution = my_workflow.start_execution(5)
    wait_for_execution(workflow_execution)
    @forking_executor.shutdown(1)
    workflow_execution.events.map(&:event_type).count("ActivityTaskFailed").should == 1
  end

  it "is a good example of the service" do
    # Definition of the activity
    class AddOneActivity
      extend Activity
      activity :run_activity1 do |options|
        options.default_task_list = "add_one_task_list"
        options.version = "1"
        options.default_task_heartbeat_timeout = "600"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "30"
      end
      def run_activity1(arg)
        arg.should == 5
        arg + 1
      end
    end
    # Definition of the workflow logic
    class MyWorkflow
      extend AWS::Flow::Workflows
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "AddOneActivity"
        # If we had the activity somewhere we couldn't reach it, we would have
        # to have the lines below, but since the have access to the activity, we
        # can simply "peek" at its configuration, and use those

        # options.default_task_heartbeat_timeout = "600"
        # options.default_task_list = "add_one_task_list"
        # options.default_task_schedule_to_close_timeout = "600"
        # options.default_task_schedule_to_start_timeout = "600"
        # options.default_task_start_to_close_timeout = "600"
      end

      # The default place to start the execution of a workflow is "entry_point",
      # but you can specify any entry point you want with the entry_point method
      entry_point :start_my_workflow
      def start_my_workflow(arg)
        # Should is a Rspec assert statement. E.g. "assert that the variable arg
        # is equal to 5"
        arg.should == 5
        # This makes sure that if there is an error, such a time out, then the
        # activity will be rescheduled
        activity.exponential_retry(:run_activity1, arg) do |o|
          o.maximum_attempts = 3
        end
      end
    end

    # Set up the workflow/activity worker
    task_list = "add_one_task_list"
    # @swf and @domain are set beforehand with the aws ruby sdk
    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(MyWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(AddOneActivity)
    worker.register
    activity_worker.register

    # Get a workflow client to start the workflow
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "MyWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_list = task_list
      options.task_start_to_close_timeout = 120
      options.child_policy = :request_cancel
    end
    # Forking executors have some possibility of race conditions, so we will
    # avoid them by putting in a small sleep. There is no plan to fix at current, as
    # we don't expect forking executor to be used by most customers.
    my_workflow_client = my_workflow_factory.get_client
    sleep 5
    workflow_execution = my_workflow_client.start_execution(5)
    # We use an executor here so as to be able to test this feature within one
    # working process, as activity_worker.start and worker.start will block
    # otherwise
    forking_executor = ForkingExecutor.new(:max_workers => 2)
    forking_executor.execute { activity_worker.start }
    forking_executor.execute { worker.start }

    # Sleep to give the threads some time to compute, as we'll run right out of
    # the test before they can run otherwise
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
  end

  it "is an example of joining a parallel split" do
    # Definition of the activity
    class ParallelSplitActivity
      extend Activity
      activity :run_activity1, :run_activity2, :run_activity3 do |options|
        options.default_task_list = "parallel_split_task_list"
        options.version = "1"
        options.default_task_heartbeat_timeout = "600"
        options.default_task_schedule_to_close_timeout = "120"
        options.default_task_schedule_to_start_timeout = "120"
        options.default_task_start_to_close_timeout = "120"
      end
      def run_activity1(arg)
        arg + 1
      end
      def run_activity2(arg)
        arg + 2
      end
      def run_activity3(arg)
        arg + 3
      end
    end
    # Definition of the workflow logic
    class ParallelWorkflow
      extend AWS::Flow::Workflows
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "ParallelSplitActivity"
        # If we had the activity somewhere we couldn't reach it, we would have
        # to have the lines below, but since the have access to the activity, we
        # can simply "peek" at its configuration, and use those

        # options.default_task_heartbeat_timeout = "600"
        # options.default_task_list = "parallel_split_task_list"
        # options.default_task_schedule_to_close_timeout = "120"
        # options.default_task_schedule_to_start_timeout = "120"
        # options.default_task_start_to_close_timeout = "120"
      end

      # The default place to start the execution of a workflow is "entry_point",
      # but you can specify any entry point you want with the entry_point method
      entry_point :start_my_workflow
      def start_my_workflow(arg)
        future_array = []
        [:run_activity1, :run_activity2, :run_activity3].each do |this_activity|
          # send_async will not block here, but will instead return a
          # future. So, at the end of this each loop, future_array will contain
          # 3 promises corresponding to the values that will eventually be
          # returned from calling the activities
          future_array << activity.send_async(this_activity, arg)

          # wait_for_all will block until all the promises in the enumerable
          # collection that it is given are ready. There is also wait_for_any,
          # which will return when any of the promises are ready. In this way,
          # you can join on a parallel split.
        end
        wait_for_all(future_array)
      end
    end

    # Set up the workflow/activity worker
    task_list = "parallel_split_task_list"
    # @swf and @domain are set beforehand with the aws ruby sdk
    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(ParallelWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list) { {:use_forking => false}}
    activity_worker.add_activities_implementation(ParallelSplitActivity)
    worker.register
    activity_worker.register

    # Get a workflow client to start the workflow
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ParallelWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_list = task_list
      options.task_start_to_close_timeout = 10
      options.child_policy = :request_cancel
    end

    my_workflow_client = my_workflow_factory.get_client
    workflow_execution = my_workflow_client.start_execution(5)

    forking_executor = ForkingExecutor.new(:max_workers => 2)
    forking_executor.execute { activity_worker.start }
    forking_executor.execute { worker.start }

    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
  end

  it "is an example of error handling in rubyflow" do
    class ErrorHandlingActivity
      extend Activity
      activity :run_activity1, :run_activity2 do |options|
        options.default_task_list = "error_handling_task_list"
        options.version = "1"
        options.default_task_heartbeat_timeout = "600"
        options.default_task_schedule_to_close_timeout = "10"
        options.default_task_schedule_to_start_timeout = "10"
        options.default_task_start_to_close_timeout = "10"
      end
      def run_activity1(arg)
        raise StandardError, "run_activity1 failed"
      end
      def run_activity2(arg)
        raise StandardError, "run_activity2 failed"
      end
    end
    # Definition of the workflow logic
    class MyWorkflow
      extend AWS::Flow::Workflows
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "ErrorHandlingActivity"
        # If we had the activity somewhere we couldn't reach it, we would have
        # to have the lines below, but since the have access to the activity, we
        # can simply "peek" at its configuration, and use those

        # options.default_task_heartbeat_timeout = "600"
        # options.default_task_list = "error_handling_task_list"
        # options.default_task_schedule_to_close_timeout = "120"
        # options.default_task_schedule_to_start_timeout = "120"
        # options.default_task_start_to_close_timeout = "120"
      end

      # The default place to start the execution of a workflow is "entry_point",
      # but you can specify any entry point you want with the entry_point method
      entry_point :start_my_workflow
      def start_my_workflow(arg)
        # activity.run_activity1(arg) will "block", and so we can use the normal
        # ruby error handler semantics, and if there is a failure, it will
        # propagate here
        error_seen = nil
        begin
          activity.run_activity1(arg)
        rescue Exception => e
          error_seen = e.class
          # Do something with the error
        ensure
          # Should is a Rspec assert statement. E.g. "assert that the variable error_seen
          # is equal to StandardError
          error_seen.should == ActivityTaskFailedException
          # Do something to clean up after
        end
        # Since send_async won't "block" here, but will schedule a task for
        # processing later and evaluate any expressions after the send_async, we
        # should use the asynchronous error handler so as to make sure that
        # exceptions raised by the send_async will be caught hereerror_seen = nil
        error_handler do |t|
          t.begin { activity.send_async(:run_activity2, arg) }
          t.rescue(Exception) do |error|
            error_seen = error.class
            # Do something with the error
          end
          t.ensure do
            error_seen.should == ActivityTaskFailedException
            # Do something to clean up after
          end
          end
        5
      end
    end

    # Set up the workflow/activity worker
    task_list = "error_handling_task_list"
    # @swf and @domain are set beforehand with the aws ruby sdk
    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(MyWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(ErrorHandlingActivity)
    worker.register
    activity_worker.register

    # Get a workflow client to start the workflow
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "MyWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_list = task_list
      options.task_start_to_close_timeout = 20
      options.child_policy = :request_cancel
    end

    my_workflow_client = my_workflow_factory.get_client
    workflow_execution = my_workflow_client.start_execution(5)

    # We use an executor here so as to be able to test this feature within one
    # working process, as activity_worker.start and worker.start will block
    # otherwise
    # forking_executor = ForkingExecutor.new

    # forking_executor.execute { activity_worker.start }
    # class WorkflowWorker
    #   def start
    #     poller = WorkflowTaskPoller.new(@service, @domain, DecisionTaskHandler.new(@workflow_definition_map), @ptask_list)
    #     loop do
    #       poller.poll_and_process_single_task
    #     end
    #   end
    # end

    worker.run_once
    activity_worker.run_once
    worker.run_once
    activity_worker.run_once
    worker.run_once
    # worker.start

    wait_for_execution(workflow_execution)

    workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
  end

  it "ensures that you can use an internal workflow_client without domain/client" do
    general_test(:task_list => "internal_without_domain", :class_name => "InternalWithoutDomain")
    @workflow_class.class_eval do
      def entry_point
        my_workflow_client = workflow_client
        my_workflow_client.class.should == WorkflowClient
      end
    end

    workflow_execution = @my_workflow_client.entry_point
    @worker.run_once
  end

  it "ensures you cannot schedule more than 99 things in one decision" do
    general_test(:task_list => "schedule_more_than_100", :class_name => "Above100TasksScheduled")
    @activity_class.class_eval do
      def run_activity1(arg)
        arg
      end
    end
    @workflow_class.class_eval do
      def entry_point
        101.times do |i|
          activity.send_async(:run_activity1, i)
        end
      end
    end
    workflow_execution = @my_workflow_client.entry_point
    @worker.run_once
    workflow_execution.events.map(&:event_type).count("ActivityTaskScheduled").should be 99
    @worker.run_once
    workflow_execution.events.map(&:event_type).count("ActivityTaskScheduled").should be 101
  end


  describe "ensures that you can specify the {workflow_id,execution_method} to be used for an external client" do
    {:workflow_id => ["blah", "workflow_id"] ,
     :execution_method => ["start", "workflow_type.name.split('.').last" ]
    }.each_pair do |method, value_and_method_to_check|
      value, method_to_check = value_and_method_to_check

      it "makes sure that #{method} can be specified correctly" do
        class WorkflowIDWorkflow
          extend Workflows
          workflow :start do
            {
              version: "1.0",
              default_execution_start_to_close_timeout: 600,
              default_task_list: "timeout_test"
            }
          end
          def start; end
        end

        worker = WorkflowWorker.new(@domain.client, @domain, "timeout_test", WorkflowIDWorkflow)
        worker.register
        client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "WorkflowIDWorkflow" } }

        workflow_execution = client.start do {
          method.to_sym => value,
          tag_list: ["stuff"]
        }
        end

        return_value = eval "workflow_execution.#{method_to_check}"
        return_value.should == value
        workflow_execution.tags.should == ["stuff"]
      end
    end
  end
  describe "making sure that timeouts are infrequent" do
    it "is a basic repro case" do
      class TimeoutActivity
        extend AWS::Flow::Activities
        activity :run_activity1 do
          {
            default_task_list: "timeout_test_activity",
            version: "1.0",
            default_task_schedule_to_close_timeout: "60",
            default_task_schedule_to_start_timeout: "30",
            default_task_start_to_close_timeout: "30",
          }
        end
        def run_activity1
          "did some work in run_activity1"
        end
      end
      class TimeoutWorkflow
        extend AWS::Flow::Workflows
        workflow :entry_point do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: "timeout_test_workflow"
          }
        end
        activity_client (:activity) { { from_class: "TimeoutActivity" } }
        def entry_point
          activity.run_activity1
        end
      end
      @worker = WorkflowWorker.new(@domain.client, @domain, "timeout_test_workflow", TimeoutWorkflow)
      @activity_workers = []

      num_tests = 15
      1.upto(num_tests) { @activity_workers << ActivityWorker.new(@domain.client, @domain, "timeout_test_activity", TimeoutActivity) }

      @worker.register
      @activity_workers.first.register

      my_workflow_client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "TimeoutWorkflow" } }

      workflow_executions = []

      @forking_executor  = ForkingExecutor.new(max_workers: 20)
      @forking_executor.execute { @worker.start }
      @activity_workers.each { |x| @forking_executor.execute { x.start } }
      sleep 10

      1.upto(num_tests)  { |i| workflow_executions << my_workflow_client.entry_point }

      workflow_executions.each { |x| wait_for_execution(x) }
      workflow_executions.each{|x| x.events.to_a.last.event_type.should == "WorkflowExecutionCompleted" }
      @forking_executor.shutdown(1)

    end
  end

  describe "makes sure that workflow clients expose the same client api and do the right thing" do
    it "makes sure that send_async works" do
      class SendAsyncChildWorkflow
        extend AWS::Flow::Workflows
        workflow :start do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: "600"
          }
        end
        def start; end
      end
      class SendAsyncParentWorkflow
        extend AWS::Flow::Workflows
        workflow :start do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: "600",
          }
        end
        def start
          domain = get_test_domain
          client = AWS::Flow::workflow_client(domain.client, domain) { { from_class: "SendAsyncChildWorkflow" } }
          client.send_async(:start_execution) { { task_list: "client_test_async2" } }
          client.send_async(:start_execution) { { task_list: "client_test_async2" } }
        end
      end
      @parent_worker = WorkflowWorker.new(@domain.client, @domain, "client_test_async", SendAsyncParentWorkflow)
      @child_worker = WorkflowWorker.new(@domain.client, @domain, "client_test_async2", SendAsyncChildWorkflow)
      @parent_worker.register
      @child_worker.register
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @parent_worker.start }
      @forking_executor.execute { @child_worker.start }

      my_workflow_client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "SendAsyncParentWorkflow" } }
      workflow_execution = my_workflow_client.start_execution

      wait_for_execution(workflow_execution)

      history_events = workflow_execution.events.map(&:event_type)
      history_events.count("ChildWorkflowExecutionCompleted").should == 2
      history_events.count("WorkflowExecutionCompleted").should == 1
    end

    it "makes sure that retry works" do
      class OtherWorkflow
        extend AWS::Flow::Workflows
        workflow :other_workflow do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 120,
            default_task_start_to_close_timeout: 10
          }

        end
        def other_workflow
          raise "Simulated error"
        end
      end
      class BadWorkflow
        extend AWS::Flow::Workflows 
        workflow :bad_workflow do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_start_to_close_timeout: 30
          }
        end

        def bad_workflow
          domain = get_test_domain
          child_client = AWS::Flow::workflow_client(domain.client, domain) { { from_class: "OtherWorkflow" } } 
          child_client.exponential_retry(:start_execution) { { maximum_attempts: 1 }}
        end
      end
      @parent_worker = WorkflowWorker.new(@domain.client, @domain, "client_test_retry", BadWorkflow)
      @child_worker = WorkflowWorker.new(@domain.client, @domain, "client_test_retry2", OtherWorkflow)
      @parent_worker.register
      @child_worker.register

      forking_executor = ForkingExecutor.new(:max_workers => 3)
      forking_executor.execute { @parent_worker.start }
      forking_executor.execute { @child_worker.start }

      parent_client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "BadWorkflow" } }
      workflow_execution = parent_client.start_execution

      wait_for_execution(workflow_execution)
      history_events = workflow_execution.events.map(&:event_type)
      history_events.count("ChildWorkflowExecutionFailed").should == 2
      history_events.count("WorkflowExecutionFailed").should == 1
    end

    it "ensures that activity task timed out is not a terminal exception, and that it can use the new option style" do
      general_test(:task_list => "activity_task_timed_out", :class_name => "ActivityTaskTimedOut")
      @workflow_class.class_eval do
        def entry_point
          activity.exponential_retry(:run_activity1) do
            {
              :retries_per_exception => {
                ActivityTaskTimedOutException => Float::INFINITY,
                ActivityTaskFailedException => 3
              }
            }
          end
        end
      end

      workflow_execution = @my_workflow_client.entry_point
      @worker.run_once
      sleep 20
      @worker.run_once
      @worker.run_once
      workflow_execution.events.map(&:event_type).last.should == "ActivityTaskScheduled"
    end

    it "ensures that with_retry does synchronous blocking by default" do
      general_test(:task_list => "with_retry_synch", :class_name => "WithRetrySynchronous")
      @workflow_class.class_eval do
        def entry_point
          foo = with_retry do
            activity.run_activity1
          end
          activity.run_activity2
        end
      end
      workflow_execution = @my_workflow_client.entry_point
      @worker.run_once
      # WFExecutionStarted, DecisionTaskScheduled, DecisionTaskStarted, DecisionTaskCompleted, ActivityTaskScheduled(only 1!)
      workflow_execution.events.to_a.length.should be 5
    end

    it "ensures that with_retry does asynchronous blocking correctly" do
      general_test(:task_list => "with_retry_synch", :class_name => "WithRetryAsynchronous")
      @workflow_class.class_eval do
        def entry_point
          with_retry do
            activity.send_async(:run_activity1)
            activity.send_async(:run_activity2)
          end
        end
      end
      workflow_execution = @my_workflow_client.entry_point
      @worker.run_once
      # WFExecutionStarted, DecisionTaskScheduled, DecisionTaskStarted, DecisionTaskCompleted, ActivityTaskScheduled(only 1!)
      workflow_execution.events.to_a.length.should be 6
    end


    it "makes sure that option inheritance doesn't override set values" do
      class InheritanceOptionsWorkflow
        extend Workflows
        workflow :entry_point do
          {
            version: "1.0",
          }
        end
        def entry_point ; end
      end
      worker = WorkflowWorker.new(@swf.client, @domain, "client_test_inheritance", InheritanceOptionsWorkflow)
      worker.register
      my_workflow_factory = workflow_factory @swf.client, @domain do |options|
        options.workflow_name = "InheritanceOptionsWorkflow"
        options.execution_start_to_close_timeout = 600
        options.task_start_to_close_timeout = 10
        options.child_policy = :REQUEST_CANCEL
        options.task_list = "client_test_inheritance"
      end
      workflow_execution = my_workflow_factory.get_client.entry_point
      workflow_execution.terminate
      workflow_execution.child_policy.should == :request_cancel
    end

    it "makes sure that option inheritance gives you defaults" do
      class InheritanceOptionsWorkflow2
        extend Workflows
        workflow :options_workflow do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: "client_test_inheritance"
          }
        end
        def options_workflow ; end
      end
      worker = WorkflowWorker.new(@domain.client, @domain, "client_test_inheritance", InheritanceOptionsWorkflow2)
      worker.register

      client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "InheritanceOptionsWorkflow2", child_policy: "REQUEST_CANCEL" } }

      workflow_execution = client.start_execution
      workflow_execution.terminate
      workflow_execution.child_policy.should == :request_cancel
    end

    it "makes sure that the new option style is supported" do
      class NewOptionsActivity
        extend Activity
        activity :run_activity1 do
          {
            :default_task_list => "options_test", :version => "1",
            :default_task_heartbeat_timeout => "600",
            :default_task_schedule_to_close_timeout => "60",
            :default_task_schedule_to_start_timeout => "60",
            :default_task_start_to_close_timeout => "60",
          }
        end
        def run_activity1
          "did some work in run_activity1"
        end
      end
      class NewOptionsWorkflow
        extend Workflows
        version "1"
        entry_point :entry_point
        activity_client :activity do
          {
            :prefix_name => "NewOptionsActivity", :version => "1"
          }
        end
        def entry_point
          activity.run_activity1
        end
      end
      worker = WorkflowWorker.new(@swf.client, @domain, "options_test", NewOptionsWorkflow)
      worker.register
      activity_worker = ActivityWorker.new(@swf.client, @domain, "options_test", NewOptionsActivity)
      activity_worker.register
      my_workflow_factory = workflow_factory @swf.client, @domain do |options|
        options.workflow_name = "NewOptionsWorkflow"
        options.execution_start_to_close_timeout = 600
        options.task_start_to_close_timeout = 10
        options.child_policy = :REQUEST_CANCEL
        options.task_list = "options_test"
      end
      workflow_execution = my_workflow_factory.get_client.entry_point
      worker.run_once
      activity_worker.run_once
      worker.run_once
      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
    end



    it "makes sure that the with_retry is supported" do
      class WithRetryActivity
        extend Activity
        activity :run_activity1 do
          {
            :default_task_list => "options_test", :version => "1",
            :default_task_heartbeat_timeout => "600",
            :default_task_schedule_to_close_timeout => "60",
            :default_task_schedule_to_start_timeout => "60",
            :default_task_start_to_close_timeout => "60",
          }
        end
        def run_activity1
          raise "simulated error"
        end
      end
      class WithRetryWorkflow
        extend Workflows
        version "1"
        entry_point :entry_point
        activity_client :activity do
          {
            :prefix_name => "WithRetryActivity", :version => "1"
          }
        end
        def entry_point
          with_retry(:maximum_attempts => 1) { activity.run_activity1 }
        end
      end
      worker = WorkflowWorker.new(@swf.client, @domain, "options_test", WithRetryWorkflow)
      worker.register
      activity_worker = ActivityWorker.new(@swf.client, @domain, "options_test", WithRetryActivity)
      activity_worker.register
      my_workflow_factory = workflow_factory @swf.client, @domain do |options|
        options.workflow_name = "WithRetryWorkflow"
        options.execution_start_to_close_timeout = 600
        options.task_start_to_close_timeout = 10
        options.child_policy = :REQUEST_CANCEL
        options.task_list = "options_test"
      end
      workflow_execution = my_workflow_factory.get_client.entry_point
      worker.run_once
      activity_worker.run_once
      worker.run_once # Sets a timer

      worker.run_once
      activity_worker.run_once
      worker.run_once # Sets a timer

      events = workflow_execution.events.map(&:event_type)
      events.count("ActivityTaskScheduled").should == 2
      events.last.should == "WorkflowExecutionFailed"
    end

    it "makes sure that inheritance of workflows works" do
      class InheritWorkflow
        extend Workflows
        workflow(:test) {{:version => "1"}}
      end
      class ChildWorkflow < InheritWorkflow; end
      ChildWorkflow.workflows.empty?.should == false
    end

    it "makes sure that inheritance of activities works" do
      class InheritActivity
        extend Activities
        activity :test
      end
      class ChildActivity < InheritActivity; end
      ChildActivity.activities.empty?.should == false
    end

    it "makes sure that you can set the activity_name" do

      class OptionsActivity
        extend Activity
        activity :run_activity1 do |options|
          options.default_task_list = "options_test"
          options.version = "1"
          options.default_task_heartbeat_timeout = "600"
          options.default_task_schedule_to_close_timeout = "60"
          options.default_task_schedule_to_start_timeout = "60"
          options.default_task_start_to_close_timeout = "60"
        end
        def run_activity1
          "did some work in run_activity1"
        end
      end
      class OptionsWorkflow
        extend Workflows
        version "1"
        entry_point :entry_point
        activity_client :activity do
          {
            :prefix_name => "OptionsActivity", :version => "1"
          }
        end
        def entry_point
          activity.run_activity1
        end
      end
      worker = WorkflowWorker.new(@swf.client, @domain, "options_test", OptionsWorkflow)
      worker.register
      activity_worker = ActivityWorker.new(@swf.client, @domain, "options_test", OptionsActivity)
      activity_worker.register
      my_workflow_factory = workflow_factory @swf.client, @domain do |options|
        options.workflow_name = "OptionsWorkflow"
        options.execution_start_to_close_timeout = 600
        options.task_start_to_close_timeout = 10
        options.child_policy = :REQUEST_CANCEL
        options.task_list = "options_test"
      end
      workflow_execution = my_workflow_factory.get_client.entry_point
      worker.run_once
      activity_worker.run_once
      worker.run_once
    end
  end

  it "makes sure that you can create a workflow in the new way" do
    class WorkflowWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "test"} }
      def entry_point; ;end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "test", WorkflowWorkflow)
    worker.register
    client = workflow_client(@swf.client, @domain) { {:from_class => "WorkflowWorkflow"} }
    workflow_execution = client.start_execution
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end
  it "makes sure that you can use with_opts with workflow_client" do
    class WorkflowWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "test"} }
      def entry_point; ;end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "Foobarbaz", WorkflowWorkflow)
    worker.register
    client = workflow_client(@swf.client, @domain) { {:from_class => "WorkflowWorkflow"} }
    workflow_execution = client.with_opts(:task_list => "Foobarbaz").start_execution
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "makes sure you can use with_opts with activity_client" do
    class ActivityActivity
      extend Activity
      activity(:run_activity1) do
        {
          :version => 1,
          :default_task_list => "options_test",
          :default_task_heartbeat_timeout => "600",
          :default_task_schedule_to_close_timeout => "60",
          :default_task_schedule_to_start_timeout => "60",
          :default_task_start_to_close_timeout => "60",
        }
      end
    end
    class WorkflowWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "test"} }

      def entry_point; ;end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "Foobarbaz", WorkflowWorkflow)
    worker.register
    client = workflow_client(@swf.client, @domain) { {:from_class => "WorkflowWorkflow"} }
    workflow_execution = client.with_opts(:task_list => "Foobarbaz").start_execution
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "makes sure that workflow errors out on schedule_activity_task_failed" do
    class BadActivityActivity
      extend Activity
      activity(:run_activity1) do
        {
          :version => 1
        }
      end
    end
    class WorkflowWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "test"} }
      activity_client(:client) { {:version => "1", :from_class => "BadActivityActivity"} }
      def entry_point; client.run_activity1; end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "Foobarbaz", WorkflowWorkflow)
    worker.register
    client = workflow_client(@swf.client, @domain) { {:from_class => "WorkflowWorkflow"} }
    workflow_execution = client.with_opts(:task_list => "Foobarbaz").start_execution
    worker.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
  end

  it "makes sure that you can have arbitrary activity names with from_class" do
    general_test(:task_list => "arbitrary_with_from_class", :class_name => "ArbitraryWithFromClass")
    @activity_class.class_eval do
      activity :test do
        {
          :default_task_heartbeat_timeout => "600",
          :default_task_list => task_list,
          :default_task_schedule_to_close_timeout => "20",
          :default_task_schedule_to_start_timeout => "20",
          :default_task_start_to_close_timeout => "20",
          :version => "1",
          :prefix_name => "ArbitraryName",
        }
      end
      def test; end
    end
    $activity_class = @activity_class
    workflow_execution = @my_workflow_client.start_execution
    @activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_with_from_class", @activity_class)
    @activity_worker.register
    @workflow_class.class_eval do
      activity_client(:test) { {:from_class => $activity_class} }
      def entry_point
        test.test
      end
    end
    @worker.run_once
    @activity_worker.run_once
    @worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "makes sure that you can have arbitrary activity names" do
    class ArbitraryActivity
      extend Activity
      def foo
      end
      activity :foo do
        {
          :default_task_list => "arbitrary_test",
          :version => "1",
          :default_task_heartbeat_timeout => "600",
          :default_task_schedule_to_close_timeout => "60",
          :default_task_schedule_to_start_timeout => "60",
          :default_task_start_to_close_timeout => "60",
          :prefix_name => "Foo"
        }
      end
    end
    class ArbitraryWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1" }}
      activity_client(:client) { {:version => "1", :prefix_name => "Foo"} }
      def entry_point
        client.foo
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryWorkflow)
    worker.register
    activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryActivity)
    activity_worker.register
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ArbitraryWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_start_to_close_timeout = 10
      options.child_policy = :REQUEST_CANCEL
      options.task_list = "arbitrary_test"
    end
    workflow_execution = my_workflow_factory.get_client.start_execution
    worker.run_once
    activity_worker.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end
  it "makes sure that exponential_retry's max_attempts works correctly" do
    general_test(:task_list => "exponential_retry_test_max_attempts", :class_name => "ExponentialRetryMaxAttempts")
    @activity_class.class_eval do
      def run_activity1
        raise "error"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        activity.exponential_retry(:run_activity1) do |o|
          o.maximum_attempts = 2
        end
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once

    # first failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    #second failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    # Finally, fail
    @worker.run_once

    wait_for_execution(workflow_execution)
    events = workflow_execution.events.map(&:event_type)
    events.count("WorkflowExecutionFailed").should == 1
    (events.count("ActivityTaskFailed") + events.count("ActivityTaskTimedOut")).should >= 3
  end

  it "makes sure that exponential_retry's max_attempts works correctly from a configured client" do
    general_test(:task_list => "exponential_retry_test_with_configure", :class_name => "ExponentialRetryMaxAttemptsConfigure")
    @activity_class.class_eval do
      def run_activity1
        raise "error"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        activity.reconfigure(:run_activity1) {  {:exponential_retry => {:maximum_attempts => 2}} }

        activity.run_activity1
      end
    end
    workflow_execution = @my_workflow_client.start_execution

    @worker.run_once
    @activity_worker.run_once

    # first failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    #second failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    # Finally, fail, catch, and succeed

    @worker.run_once

    wait_for_execution(workflow_execution)
    events = workflow_execution.events.map(&:event_type)
    events.count("WorkflowExecutionFailed").should == 1
    (events.count("ActivityTaskFailed") + events.count("ActivityTaskTimedOut")).should >= 3
  end

  it "makes sure that exponential_retry allows you to capture the error with configure" do
    general_test(:task_list => "exponential_retry_test_capture_with_configure", :class_name => "ExponentialRetryMaxAttemptsCaptureConfigure")
    @activity_class.class_eval do
      def run_activity1
        raise "error"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        activity.reconfigure(:run_activity1) {  {:exponential_retry => {:maximum_attempts => 2}} }
        begin
          activity.run_activity1
        rescue Exception => e
          # just making sure I can rescue
        end
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once

    # first failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    #second failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    # Finally, fail, catch, and succeed
    @worker.run_once

    wait_for_execution(workflow_execution)
    events = workflow_execution.events.map(&:event_type)
    events.count("WorkflowExecutionCompleted").should == 1
    (events.count("ActivityTaskFailed") + events.count("ActivityTaskTimedOut")).should >= 3
  end

  it "ensures that you can change options at the call site" do
    general_test(:task_list => "basic_options", :class_name => "BasicOptions")
    @workflow_class.class_eval do
      def entry_point
        activity.run_activity1 { {:start_to_close_timeout => 120 } }
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once
    @worker.run_once
    wait_for_execution(workflow_execution)
    # The default registered is 20, we want to make sure we overrode it
    workflow_execution.events.to_a[4].attributes[:start_to_close_timeout].should == 120
  end



  it "ensures that heartbeats work" do
    general_test(:task_list => "basic_heartbeat", :class_name => "BasicHeartbeat")

    @activity_class.class_eval do
      def run_activity1
        6.times do
          sleep 5
          record_activity_heartbeat("test!")
        end
      end
    end
    @workflow_class.class_eval do
      def entry_point
        activity.run_activity1 { {:heartbeat_timeout => 10, :start_to_close_timeout => 120, :schedule_to_close_timeout => 120} }
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once
    @worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "ensures that you can use heartbeats to request cancel" do
    general_test(:task_list => "heartbeat_request_cancel", :class_name => "HeartbeatRequestCancel")
    @activity_class.class_eval do
      def run_activity1
        6.times do
          sleep 5
          record_activity_heartbeat("test!")
        end
        raise "If we got here, the test failed, as we should have cancelled the activity"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        error_handler do |t|
          t.begin do
            future = activity.run_activity1 { {:heartbeat_timeout => 10, :start_to_close_timeout => 120, :schedule_to_close_timeout => 120, :return_on_start => true} }
            create_timer(5)
            activity.request_cancel_activity_task(future)
          end
          t.rescue(CancellationException) { |e| }
        end
      end
    end

    workflow_execution = @my_workflow_client.start_execution
    forking_executor = ForkingExecutor.new(:max_workers => 1)
    @worker.run_once
    forking_executor.execute { @activity_worker.start }
    sleep 10
    @worker.run_once
    sleep 10
    @worker.run_once
    wait_for_execution(workflow_execution)
    forking_executor.shutdown(1)
    # If we didn't cancel, the activity would fail
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "ensures you can use manual completion" do
    general_test(:task_list => "manual_completion", :class_name => "ManualCompletion")

    activity_worker = ActivityWorker.new(@swf.client, @domain, "manual_completion", @activity_class)
    activity_worker.register
    @workflow_class.class_eval do
      activity_client(:activity1) { {
        from_class: self.activity_class,
        manual_completion: true
      } }
      def entry_point
        activity.run_activity2
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    activity_worker.run_once
    workflow_execution.events.map(&:event_type).should include("ActivityTaskStarted")
  end

  it "makes sure that exponential_retry allows you to capture the error" do
    general_test(:task_list => "exponential_retry_test_capture", :class_name => "ExponentialRetryMaxAttemptsCapture")
    @activity_class.class_eval do
      def run_activity1
        raise "error"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        begin
          activity.exponential_retry(:run_activity1) do |o|
            o.maximum_attempts = 2
          end
        rescue Exception => e
          # Just making sure I can rescue
        end
      end
    end
    workflow_execution = @my_workflow_client.start_execution

    @worker.run_once
    @activity_worker.run_once

    # first failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    #second failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once


    # Finally, fail
    @worker.run_once

    wait_for_execution(workflow_execution)
    events = workflow_execution.events.map(&:event_type)
    events.count("WorkflowExecutionCompleted").should == 1
    (events.count("ActivityTaskFailed") + events.count("ActivityTaskTimedOut")).should >= 3
  end

  it "makes sure that you can use extend Activities" do
    class ActivitiesActivity
      extend Activities
      def foo
      end
      activity :foo do
        {
          :default_task_list => "arbitrary_test",
          :version => "1",
          :default_task_heartbeat_timeout => "600",
          :default_task_schedule_to_close_timeout => "60",
          :default_task_schedule_to_start_timeout => "60",
          :default_task_start_to_close_timeout => "60",
          :prefix_name => "Foo"
        }
      end
    end
    class ActivitiesWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1" }}
      activity_client(:client) { {:version => "1", :prefix_name => "Foo"} }
      def entry_point
        client.foo
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary_test", ActivitiesWorkflow)
    worker.register
    activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", ActivitiesActivity)
    activity_worker.register
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ActivitiesWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_start_to_close_timeout = 10
      options.child_policy = :REQUEST_CANCEL
      options.task_list = "arbitrary_test"
    end
    workflow_execution = my_workflow_factory.get_client.start_execution
    worker.run_once
    activity_worker.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "makes sure that you can't have a '.' in prefix name" do
    class ArbitraryWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1" }}
      activity_client(:client) { {:version => "1", :prefix_name => "Foo.this"} }
      def entry_point
        client.foo
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryWorkflow)
    worker.register
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ArbitraryWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_start_to_close_timeout = 10
      options.child_policy = :REQUEST_CANCEL
      options.task_list = "arbitrary_test"
    end
    workflow_execution = my_workflow_factory.get_client.start_execution
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
  end

  it "ensures that reregistering with different values without changing the version will alert you" do
    class RegisterActivity
      extend Activity
      activity :foo do |opt|
        opt.version = "1"
        opt.default_task_start_to_close_timeout = "60"
      end
    end
    activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", RegisterActivity)
    activity_worker.register
    class RegisterBadActivity
      extend Activity
      activity :foo do |opt|
        opt.version = "1"
        opt.default_task_start_to_close_timeout = 30
        opt.prefix_name = "RegisterActivity"
      end
    end
    activity_worker2 = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", RegisterBadActivity)
    expect { activity_worker2.register }.to raise_error RuntimeError
  end

  it "makes sure that you can have arbitrary activity names with the old style options" do
    class ArbitraryActivity
      extend Activity
      def foo
      end
      activity :foo do |opt|
        opt.default_task_list = "arbitrary_test"
        opt.version = "1"
        opt.default_task_heartbeat_timeout = "600"
        opt.default_task_schedule_to_close_timeout = "60"
        opt.default_task_schedule_to_start_timeout = "60"
        opt.default_task_start_to_close_timeout = "60"
        opt.prefix_name = "Foo"
      end
    end
    class ArbitraryWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1" }}
      activity_client(:client) { {:version => "1", :prefix_name => "Foo"} }
      def entry_point
        client.foo
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryWorkflow)
    worker.register
    activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryActivity)
    activity_worker.register
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ArbitraryWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_start_to_close_timeout = 10
      options.child_policy = :REQUEST_CANCEL
      options.task_list = "arbitrary_test"
    end
    workflow_execution = my_workflow_factory.get_client.start_execution
    worker.run_once
    activity_worker.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  describe "Miscellaneous tests" do
    it "will test whether the service client uses the correct user-agent-prefix" do

      domain = get_test_domain
      domain.client.config.user_agent_prefix.should == "ruby-flow"

      response = domain.client.list_domains({:registration_status => "REGISTERED"})
      result = response.http_request.headers["user-agent"]

      result.should match(/^ruby-flow/)
    end

    it "will test whether from_class can take in non-strings" do
      domain = get_test_domain

      class ActivityActivity
        extend Activity
        activity(:activity1) do
          {
            :version => 1
          }
        end
      end
      class WorkflowWorkflow
        extend Workflows
        workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "test"} }
        activity_client(:activity) { {:version => "1", :from_class => ActivityActivity} }
        def entry_point
          activity.activity1
        end
      end

      client = workflow_client(domain.client, domain) { {:from_class => WorkflowWorkflow} }
      client.is_execution_method(:entry_point).should == true
    end
    it "tests whether a forking executor will not accept work when it has no free workers" do
      domain = get_test_domain

      class ForkingTestActivity
        extend Activity
        activity(:activity1) do
          {
            :version => 1,
            :default_task_list => "forking_executor_test",
            :default_task_schedule_to_start_timeout => 120,
            :default_task_start_to_close_timeout => 120,
            :default_task_heartbeat_timeout => "600"
          }
        end
        def activity1; sleep 10; end
      end
      class ForkingTestWorkflow
        extend Workflows
        workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "forking_executor_test"} }
        activity_client(:activity) { {:version => "1", :from_class => ForkingTestActivity} }
        def entry_point
          3.times { activity.send_async(:activity1) }
        end
      end

      worker = WorkflowWorker.new(domain.client, domain, "forking_executor_test", ForkingTestWorkflow)
      worker.register

      activity_worker = ActivityWorker.new(domain.client, domain, "forking_executor_test", ForkingTestActivity) { { :execution_workers => 1 } }
      activity_worker.register

      client = workflow_client(domain.client, domain) { {:from_class => ForkingTestWorkflow} }

      workflow_execution = client.start_execution
      forking_executor  = ForkingExecutor.new(:max_workers => 3)
      forking_executor.execute { worker.start }
      forking_executor.execute { activity_worker.start }
      wait_for_execution(workflow_execution)
      history = workflow_execution.events.map(&:event_type)
      current_depth = 0
      0.upto(history.length) do |i|
        current_depth += 1 if history[i] == "ActivityTaskStarted"
        current_depth -= 1 if (history[i] =~ /ActivityTask(Completed|TimedOut|Failed)/)
        if current_depth > 1
          raise "We had two started's in a row, which indicates the possibility of starving(since the worker should only process one activity at a time) and thus causing a task timeout"
        end
      end

    end

  end
end

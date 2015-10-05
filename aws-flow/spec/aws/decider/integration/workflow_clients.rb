require_relative 'setup'

describe "RubyFlowDecider" do
  include_context "setup integration tests"
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
end

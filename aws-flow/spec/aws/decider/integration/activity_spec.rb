require_relative 'setup'

describe Activities do
  before(:all) do
    @swf, @domain = setup_swf
  end

  it "ensures that a real activity will get scheduled" do
    task_list = "activity_task_list"
    class Blah
      extend AWS::Flow::Activities
    end
    class BasicActivity
      extend AWS::Flow::Activities

      activity :run_activity1 do
        {
          default_task_heartbeat_timeout: 60,
          version: "1",
          default_task_list: "activity_task_list",
          default_task_schedule_to_close_timeout: 60,
          default_task_schedule_to_start_timeout: 30,
          default_task_start_to_close_timeout: 30,
        }
      end
      def run_activity1; end
    end
    class BasicWorkflow
      extend AWS::Flow::Workflows
      workflow :start do
        {
          version: "1.0",
          default_task_start_to_close_timeout: 30,
          default_execution_start_to_close_timeout: 300,
          default_child_policy: "REQUEST_CANCEL",
          default_task_list: "activity_task_list"
        }
      end
      activity_client(:activity) { { from_class: "BasicActivity" } }
      def start
        activity.run_activity1
      end
    end

    worker = WorkflowWorker.new(@domain.client, @domain, task_list, BasicWorkflow)
    activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, BasicActivity)
    workflow_type_name = "BasicWorkflow.start"
    worker.register
    activity_worker.register
    workflow_type, _ = @domain.workflow_types.page(:per_page => 1000).select {|x| x.name == workflow_type_name}

    workflow_execution = workflow_type.start_execution

    worker.run_once
    activity_worker.run_once
    worker.run_once
    wait_for_execution(workflow_execution)

    workflow_execution.events.map(&:event_type).should ==
      ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
  end

  it "tests to see what two activities look like" do

    class DoubleActivity
      extend AWS::Flow::Activities
      activity :run_activity1, :run_activity2 do
        {
          default_task_heartbeat_timeout: 60,
          version: "1.0",
          default_task_list: "double_activity_task_list",
          default_task_schedule_to_close_timeout: 60,
          default_task_schedule_to_start_timeout: 30,
          default_task_start_to_close_timeout: 30,
          exponential_retry: { 
            retries_per_exception: { 
              ActivityTaskTimedOutException: Float::INFINITY 
            }
          }
        }
      end
      def run_activity1; end
      def run_activity2; end
    end

    class DoubleWorkflow
      extend AWS::Flow::Workflows
      workflow(:start) do
        {
          version: "1.0",
          default_task_start_to_close_timeout: 30,
          default_execution_start_to_close_timeout: 300,
          default_child_policy: "REQUEST_CANCEL",
          default_task_list: "double_activity_task_list"
        }
      end
      activity_client(:activity) { { from_class: "DoubleActivity" } }
      def start
        activity.send_async(:run_activity1)
        activity.run_activity2
      end
    end

    task_list = "double_activity_task_list"

    worker = WorkflowWorker.new(@domain.client, @domain, task_list, DoubleWorkflow)
    activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, DoubleActivity)
    workflow_type_name = "DoubleWorkflow.start"
    worker.register
    activity_worker.register
    workflow_id = "basic_activity_workflow"

    run_id = @swf.client.start_workflow_execution(
      :workflow_type => {
        :name => workflow_type_name,
        :version => "1.0"
      },
      :workflow_id => workflow_id,
      :domain => @domain.name.to_s
    )
    workflow_execution = AWS::SimpleWorkflow::WorkflowExecution.new(@domain, workflow_id, run_id["runId"])
    @forking_executor = ForkingExecutor.new(:max_workers => 3)
    @forking_executor.execute { worker.start }
    @forking_executor.execute { activity_worker.start }
    wait_for_execution(workflow_execution)

    workflow_history = workflow_execution.events.map(&:event_type)
    workflow_history.count("ActivityTaskCompleted").should == 2
    workflow_history.count("WorkflowExecutionCompleted").should == 1
  end

  it "tests to see that two subsequent activities are supported" do
    task_list = "subsequent_activity_task_list"
    workflow_tasklist = "subsequent_workflow_task_list"
    class SubsequentActivity
      extend AWS::Flow::Activities

      activity :run_activity1, :run_activity2 do
        {
          default_task_heartbeat_timeout: 300,
          version: "1.2",
          default_task_list: "subsequent_activity_task_list",
          default_task_schedule_to_close_timeout: 60,
          default_task_schedule_to_start_timeout: 30,
          default_task_start_to_close_timeout: 30,
        }
      end
      def run_activity1; end
      def run_activity2; end
    end
    class SubsequentWorkflow
      extend AWS::Flow::Workflows
      workflow :start do
        {
          version: "1.2",
          default_task_start_to_close_timeout: 30,
          default_execution_start_to_close_timeout: 300,
          default_child_policy: "REQUEST_CANCEL",
          default_task_list: "subsequent_workflow_task_list"
        }
      end
      activity_client(:activity) { { from_class: "SubsequentActivity" } }
      def start
        activity.run_activity1
        activity.run_activity2
      end
    end

    worker = WorkflowWorker.new(@domain.client, @domain, workflow_tasklist, SubsequentWorkflow)
    activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, SubsequentActivity)
    worker.register
    activity_worker.register

    client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "SubsequentWorkflow" } }

    workflow_execution = client.start_execution

    @forking_executor = ForkingExecutor.new(:max_workers => 3)
    @forking_executor.execute { worker.start }
    @forking_executor.execute { activity_worker.start }

    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
  end

  it "tests a much larger workflow" do
    class LargeActivity
      extend AWS::Flow::Activities

      activity :run_activity1, :run_activity2, :run_activity3, :run_activity4 do
        {
          version: "1.0",
          default_task_list: "large_activity_task_list",
          default_task_schedule_to_close_timeout: 60,
          default_task_schedule_to_start_timeout: 30,
          default_task_start_to_close_timeout: 30,
          exponential_retry: {
            retries_per_exception: {
              ActivityTaskTimedOutException => Float::INFINITY,
            }
          }
        }
      end
      def run_activity1
        "My name is Ozymandias - 1"
      end
      def run_activity2
        "King of Kings! - 2 "
      end
      def run_activity3
        "Look on my works, ye mighty - 3"
      end
      def run_activity4
        "And Despair! - 4"
      end
    end

    class LargeWorkflow
      extend AWS::Flow::Workflows
      workflow :start do
        {
          version: "1.0",
          default_task_start_to_close_timeout: 30,
          default_execution_start_to_close_timeout: 300,
          default_child_policy: "REQUEST_CANCEL",
          default_task_list: "large_activity_task_list"
        }
      end
      activity_client(:activity) { { from_class: "LargeActivity" } }
      def start
        activity.send_async(:run_activity1)
        activity.send_async(:run_activity2)
        activity.send_async(:run_activity3)
        activity.run_activity4
      end
    end

    task_list = "large_activity_task_list"
    worker = WorkflowWorker.new(@swf.client, @domain, task_list, LargeWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list, LargeActivity)
    worker.register
    activity_worker.register

    workflow_type_name = "LargeWorkflow.start"
    workflow_type, _ = @domain.workflow_types.page(:per_page => 1000).select {|x| x.name == workflow_type_name}

    workflow_execution = workflow_type.start_execution

    @forking_executor = ForkingExecutor.new(:max_workers => 5)
    @forking_executor.execute { activity_worker.start }

    @forking_executor.execute { worker.start }

    wait_for_execution(workflow_execution)

    @forking_executor.shutdown(1)
    workflow_history = workflow_execution.events.map(&:event_type)
    workflow_history.count("WorkflowExecutionCompleted").should == 1
    workflow_history.count("ActivityTaskCompleted").should == 4
  end

  context "Github issue 57" do

    before(:all) do

      class GithubIssue57TestActivity
        extend AWS::Flow::Activities
        activity :not_retryable do
          {
            version: 1.0,
            default_task_list: "github_57_activity_tasklist",
            default_task_schedule_to_start_timeout: 30,
            default_task_start_to_close_timeout: 30,
          }
        end

        activity :retryable do
          {
            version: 1.0,
            default_task_list: "github_57_activity_tasklist",
            default_task_schedule_to_start_timeout: 30,
            default_task_start_to_close_timeout: 30,
            exponential_retry: {
              maximum_attempts: 3,
            },
          }
        end

        def not_retryable
          raise 'blah'
        end
        def retryable
          raise 'asdf'
        end
      end

      class GithubIssue57TestWorkflow
        extend AWS::Flow::Workflows

        workflow :test do
          {
            version: 1.0,
            default_task_list: "github_57_workflow_tasklist",
            default_execution_start_to_close_timeout: 300,
            default_task_start_to_close_timeout: 30
          }
        end

        activity_client(:client) { { from_class: "GithubIssue57TestActivity" } }

        def test
          client.not_retryable
        end
      end
    end


    it "ensures _options method returns an array of type ActivityType" do
      GithubIssue57TestActivity._options.size == 2
      GithubIssue57TestActivity._options.each { |x| x.should be_an ActivityType }
    end

    it "ensures the activity gets set with the right options" do
      worker = WorkflowWorker.new(@domain.client, @domain, "github_57_workflow_tasklist", GithubIssue57TestWorkflow)
      activity_worker = ActivityWorker.new(@domain.client, @domain, "github_57_activity_tasklist", GithubIssue57TestActivity)
      worker.register
      activity_worker.register
      client = workflow_client(@domain.client, @domain) { { from_class: GithubIssue57TestWorkflow } }

      workflow_execution = client.start_execution

      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { worker.start }
      @forking_executor.execute { activity_worker.start }

      wait_for_execution(workflow_execution)

      history_events = workflow_execution.events.map(&:event_type)
      history_events.last.should == "WorkflowExecutionFailed"
      history_events.count("ActivityTaskFailed").should == 1
    end
  end
end


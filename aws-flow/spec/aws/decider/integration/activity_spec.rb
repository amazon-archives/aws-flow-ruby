require_relative 'setup'

describe Activities do
  before(:all) do
    @swf, @domain = setup_swf
  end

  it "ensures that a real activity will get scheduled" do
    task_list = "activity_task_list"
    class Blah
      extend Activity
    end
    class BasicActivity
      extend Activity

      activity :run_activity1 do |o|
        o.default_task_heartbeat_timeout = "3600"
        o.default_task_list = "activity_task_list"
        o.default_task_schedule_to_close_timeout = "3600"
        o.default_task_schedule_to_start_timeout = "3600"
        o.default_task_start_to_close_timeout = "3600"
        o.version = "1"
      end
      def run_activity1
        "first regular activity"
      end
    end
    class BasicWorkflow
      extend Decider

      entry_point :entry_point
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "BasicActivity"
      end
      def entry_point
        activity.run_activity1
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(BasicWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(BasicActivity)
    workflow_type_name = "BasicWorkflow.entry_point"
    worker.register
    activity_worker.register
    sleep 3
    workflow_type, _ = @domain.workflow_types.page(:per_page => 1000).select {|x| x.name == workflow_type_name}

    workflow_execution = workflow_type.start_execution(
      :execution_start_to_close_timeout => 3600,
      :task_list => task_list,
      :task_start_to_close_timeout => 3600,
      :child_policy => :request_cancel
    )
    worker.run_once
    activity_worker.run_once
    worker.run_once
    workflow_execution.events.map(&:event_type).should ==
      ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
  end

  it "tests to see what two activities look like" do
    task_list = "double_activity_task_list"
    class DoubleActivity
      extend Activity
      activity :run_activity1, :run_activity2 do |o|
        o.version = "1"
        o.default_task_heartbeat_timeout = "3600"
        o.default_task_list = "double_activity_task_list"
        o.default_task_schedule_to_close_timeout = "3600"
        o.default_task_schedule_to_start_timeout = "10"
        o.default_task_start_to_close_timeout = "10"
        o.exponential_retry do |retry_options|
          retry_options.retries_per_exception = {
            ActivityTaskTimedOutException => Float::INFINITY,
          }
        end
      end
      def run_activity1
        "first parallel activity"
      end
      def run_activity2
        "second parallel activity"
      end
    end
    class DoubleWorkflow
      extend Decider
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "DoubleActivity"
      end
      entry_point :entry_point
      def entry_point
        activity.send_async(:run_activity1)
        activity.run_activity2
      end
    end

    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(DoubleWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(DoubleActivity)
    workflow_type_name = "DoubleWorkflow.entry_point"
    worker.register
    activity_worker.register
    sleep 3
    workflow_id = "basic_activity_workflow"
    run_id = @swf.client.start_workflow_execution(
      :execution_start_to_close_timeout => "3600",
      :task_list => {:name => task_list},
      :task_start_to_close_timeout => "3600",
      :child_policy => "REQUEST_CANCEL",
      :workflow_type => {
        :name => workflow_type_name,
        :version => "1"
      },
      :workflow_id => workflow_id,
      :domain => @domain.name.to_s
    )
    workflow_execution = AWS::SimpleWorkflow::WorkflowExecution.new(@domain, workflow_id, run_id["runId"])
    @forking_executor = ForkingExecutor.new(:max_workers => 3)
    @forking_executor.execute { worker.start }
    sleep 5
    @forking_executor.execute { activity_worker.start }
    sleep 20
    @forking_executor.shutdown(1)

    workflow_history = workflow_execution.events.map(&:event_type)
    workflow_history.count("ActivityTaskCompleted").should == 2
    workflow_history.count("WorkflowExecutionCompleted").should == 1
  end

  it "tests to see that two subsequent activities are supported" do
    task_list = "subsequent_activity_task_list"
    class SubsequentActivity
      extend Activity
      activity :run_activity1, :run_activity2 do |o|
        o.default_task_heartbeat_timeout = "3600"
        o.version = "1"
        o.default_task_list = "subsequent_activity_task_list"
        o.default_task_schedule_to_close_timeout = "3600"
        o.default_task_schedule_to_start_timeout = "20"
        o.default_task_start_to_close_timeout = "20"
      end
      def run_activity1
        "First subsequent activity"
      end
      def run_activity2
        "Second subsequent activity"
      end
    end
    class SubsequentWorkflow
      extend Workflows
      workflow :entry_point do
        {
          :version => "1",
        }
      end
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "SubsequentActivity"
      end
      def entry_point
        activity.run_activity1
        activity.run_activity2
      end
    end

    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(SubsequentWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(SubsequentActivity)
    workflow_type_name = "SubsequentWorkflow.entry_point"
    worker.register
    activity_worker.register
    sleep 3


    my_workflow_client = workflow_client(@swf.client, @domain) do
      {
        :from_class => "SubsequentWorkflow",
        :execution_start_to_close_timeout => "3600",
        :task_list => task_list,
        :task_start_to_close_timeout => "3600",
        :child_policy => "REQUEST_CANCEL",
      }
    end
    workflow_execution = my_workflow_client.start_execution
    worker.run_once
    activity_worker.run_once
    worker.run_once
    activity_worker.run_once
    worker.run_once
    workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
  end

  it "tests a much larger workflow" do
    task_list = "large_activity_task_list"
    class LargeActivity
      extend Activity
      activity :run_activity1, :run_activity2, :run_activity3, :run_activity4 do |o|
        o.default_task_heartbeat_timeout = "3600"
        o.default_task_list = "large_activity_task_list"
        o.default_task_schedule_to_close_timeout = "3600"
        o.default_task_schedule_to_start_timeout = "5"
        o.default_task_start_to_close_timeout = "5"
        o.version = "1"
        o.exponential_retry do |retry_options|
          retry_options.retries_per_exception = {
            ActivityTaskTimedOutException => Float::INFINITY,
          }
        end
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
      extend Decider
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "LargeActivity"
      end
      entry_point :entry_point
      def entry_point
        activity.send_async(:run_activity1)
        activity.send_async(:run_activity2)
        activity.send_async(:run_activity3)
        activity.run_activity4()
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(LargeWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(LargeActivity)
    worker.register
    activity_worker.register
    sleep 3

    workflow_type_name = "LargeWorkflow.entry_point"
    workflow_type, _ = @domain.workflow_types.page(:per_page => 1000).select {|x| x.name == workflow_type_name}

    workflow_execution = workflow_type.start_execution(
      :execution_start_to_close_timeout => 3600,
      :task_list => task_list,
      :task_start_to_close_timeout => 15,
      :child_policy => :request_cancel
    )
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
            :version => 1.0,
            :default_task_list => "github_57_activity_tasklist",
            :default_task_schedule_to_start_timeout => 30,
            :default_task_start_to_close_timeout => 30,
          }
        end

        activity :retryable do
          {
            :version => 1.0,
            :default_task_list => "github_57_activity_tasklist",
            :default_task_schedule_to_start_timeout => 30,
            :default_task_start_to_close_timeout => 30,
            :exponential_retry => {
              :maximum_attempts => 3,
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
            default_execution_start_to_close_timeout: 3600,
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
      worker = WorkflowWorker.new(@swf.client, @domain, "github_57_workflow_tasklist")
      worker.add_workflow_implementation(GithubIssue57TestWorkflow)
      activity_worker = ActivityWorker.new(@swf.client, @domain, "github_57_activity_tasklist")
      activity_worker.add_activities_implementation(GithubIssue57TestActivity)
      worker.register
      activity_worker.register
      client = workflow_client(@swf.client, @domain) { { :from_class => GithubIssue57TestWorkflow } }
      workflow_execution = client.start_execution
      worker.run_once
      activity_worker.run_once
      worker.run_once
      wait_for_execution(workflow_execution)

      history_events = workflow_execution.events.map(&:event_type)
      history_events.last.should == "WorkflowExecutionFailed"
      history_events.count("ActivityTaskFailed").should == 1
    end
  end
end


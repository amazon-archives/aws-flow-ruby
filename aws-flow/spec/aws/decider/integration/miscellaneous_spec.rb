require_relative 'setup'
describe "Miscellaneous tests" do
  include_context "setup integration tests"
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

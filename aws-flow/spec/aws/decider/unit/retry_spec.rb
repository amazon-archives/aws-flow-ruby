require_relative 'setup'

$error = "--- !ruby/exception:ArgumentError\nmessage: asdfasdf\n---\n- helloworld_activity.rb:21:in `say_hello'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/activity_definition.rb:69:in\n  `execute'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/task_poller.rb:144:in\n  `execute'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/task_poller.rb:287:in\n  `process_single_task'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/task_poller.rb:344:in\n  `block in poll_and_process_single_task'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/executor.rb:75:in\n  `call'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/executor.rb:75:in\n  `block in execute'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/executor.rb:68:in\n  `fork'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/executor.rb:68:in\n  `execute'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/task_poller.rb:344:in\n  `poll_and_process_single_task'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/worker.rb:439:in\n  `run_once'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/worker.rb:412:in\n  `block in start'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/worker.rb:411:in\n  `loop'\n- /home/paritom/.rvm/gems/ruby-1.9.3-p429/gems/aws-flow-1.2.0/lib/aws/decider/worker.rb:411:in\n  `start'\n- helloworld_activity.rb:27:in `<main>'\n"

def get_history_array 
  [
    "WorkflowExecutionStarted",
    "DecisionTaskScheduled",
    "DecisionTaskStarted",
    "DecisionTaskCompleted",
    ["ActivityTaskScheduled", {activity_id: "Activity1"}],
    "ActivityTaskStarted",
    ["ActivityTaskFailed", {scheduled_event_id: 5, :activity_id => "Activity1", cause: AWS::Flow::ActivityFailureException, details: $error } ],
    "DecisionTaskScheduled",
    "DecisionTaskStarted",
  ]
end


describe "ExponentialRetry" do

  before(:all) do
    @bucket = ENV['AWS_SWF_BUCKET_NAME']
    ENV['AWS_SWF_BUCKET_NAME'] = nil
  end
  after(:all) do
    ENV['AWS_SWF_BUCKET_NAME'] = @bucket
  end

  context "ActivityRetry" do
    # The following tests for github issue # 57
    before(:all) do
      class RetryTestActivity
        extend AWS::Flow::Activities
        activity :retryable do
          {
            :version => "1.0",
            :default_task_list => 'default',
            :default_task_schedule_to_start_timeout => 20,
            :default_task_start_to_close_timeout => 20,
            :exponential_retry => {
              :maximum_attempts => 3,
              :retry_expiration_interval_seconds => 10,
              :backoff_coefficient => 4,
              :should_jitter => false
            },
          }
        end
        def retryable
          raise ArgumentError.new("TEST SIMULATION")
        end
      end

      class RetryTestWorkflow
        extend AWS::Flow::Workflows
        workflow(:start) { { version: "1.0" } }
        activity_client(:client) { { from_class: "RetryTestActivity" } }
        def start
          client.retryable
        end
      end
    end

    
    it "tests part 1 - timer gets scheduled after activity fails" do

      class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
        def get_decision_task
          fake_workflow_type =  FakeWorkflowType.new(nil, "RetryTestWorkflow.start", "1.0")
          TestHistoryWrapper.new(fake_workflow_type, FakeWorkflowExecution.new(nil, nil), FakeEvents.new(get_history_array))
        end
      end

      workflow_type_object = FakeWorkflowType.new(nil, "RetryTestWorkflow.start", "1.0")
      domain = FakeDomain.new(workflow_type_object)

      swf_client = FakeServiceClient.new
      task_list = "default"
      my_workflow_factory = workflow_factory(swf_client, domain) do |options|
        options.workflow_name = "RetryTestWorkflow"
        options.execution_start_to_close_timeout = 120
        options.task_list = "default"
        options.task_start_to_close_timeout = 30
      end

      worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, RetryTestWorkflow)
      my_workflow = my_workflow_factory.get_client
      workflow_execution = my_workflow.start_execution
      worker.start
      swf_client.trace.first[:decisions].first[:decision_type].should == "StartTimer"
      swf_client.trace.first[:decisions].first[:start_timer_decision_attributes].should == {:timer_id=>"Timer1", :start_to_fire_timeout=>"2"}
    end

    it "tests part 2 - activity gets retried the first time after timer fires" do

      class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
        def get_decision_task
          fake_workflow_type =  FakeWorkflowType.new(nil, "RetryTestWorkflow.start", "1.0")
          TestHistoryWrapper.new(fake_workflow_type, FakeWorkflowExecution.new(nil, nil),
                                 FakeEvents.new(get_history_array().push(*[
                                   "DecisionTaskCompleted",
                                   ["TimerStarted", {decision_task_completed_event_id: 10, timer_id: "Timer1", start_to_fire_timeout: 2 }],
                                   ["TimerFired", {timer_id: "Timer1", started_event_id: 11}],
                                   "DecisionTaskScheduled",
                                   "DecisionTaskStarted",
          ])))
        end
      end

      workflow_type_object = FakeWorkflowType.new(nil, "RetryTestWorkflow.start", "1.0")
      domain = FakeDomain.new(workflow_type_object)

      swf_client = FakeServiceClient.new
      task_list = "default"
      my_workflow_factory = workflow_factory(swf_client, domain) do |options|
        options.workflow_name = "RetryTestWorkflow"
        options.execution_start_to_close_timeout = 120
        options.task_list = "default"
        options.task_start_to_close_timeout = 30
      end

      worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, RetryTestWorkflow)
      my_workflow = my_workflow_factory.get_client
      workflow_execution = my_workflow.start_execution
      worker.start
      swf_client.trace.first[:decisions].first[:decision_type].should == "ScheduleActivityTask"
      swf_client.trace.first[:decisions].first[:schedule_activity_task_decision_attributes][:activity_id] == "Activity2"
    end

    it "tests part 3 - timer gets scheduled for retry after activity fails again" do

      class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
        def get_decision_task
          fake_workflow_type =  FakeWorkflowType.new(nil, "RetryTestWorkflow.start", "1.0")
          TestHistoryWrapper.new(fake_workflow_type, FakeWorkflowExecution.new(nil, nil),
                                 FakeEvents.new(get_history_array().push(*[
                                   "DecisionTaskCompleted",
                                   ["TimerStarted", {decision_task_completed_event_id: 10, timer_id: "Timer1", start_to_fire_timeout: 2 }],
                                   ["TimerFired", {timer_id: "Timer1", started_event_id: 11}],
                                   "DecisionTaskScheduled",
                                   "DecisionTaskStarted",
                                   "DecisionTaskCompleted",
                                   ["ActivityTaskScheduled", {activity_id: "Activity2"}],
                                   "ActivityTaskStarted",
                                   ["ActivityTaskFailed", {scheduled_event_id: 16, :activity_id => "Activity2", cause: ActivityTaskFailedException, details: $error} ],
                                   "DecisionTaskScheduled",
                                   "DecisionTaskStarted",
          ])))
        end
      end

      workflow_type_object = FakeWorkflowType.new(nil, "RetryTestWorkflow.start", "1.0")
      domain = FakeDomain.new(workflow_type_object)

      swf_client = FakeServiceClient.new
      task_list = "default"
      my_workflow_factory = workflow_factory(swf_client, domain) do |options|
        options.workflow_name = "RetryTestWorkflow"
        options.execution_start_to_close_timeout = 120
        options.task_list = "default"
        options.task_start_to_close_timeout = 30
      end

      worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, RetryTestWorkflow)
      my_workflow = my_workflow_factory.get_client
      workflow_execution = my_workflow.start_execution
      worker.start
      swf_client.trace.first[:decisions].first[:decision_type].should == "StartTimer"
      swf_client.trace.first[:decisions].first[:start_timer_decision_attributes].should == {:timer_id=>"Timer2", :start_to_fire_timeout=>"8"}
    end

    it "tests part 4 - the workflow fails because retry_expiration_interval_seconds is breached by exponential retry" do

      class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
        def get_decision_task
          fake_workflow_type =  FakeWorkflowType.new(nil, "RetryTestWorkflow.start", "1.0")
          TestHistoryWrapper.new(fake_workflow_type, FakeWorkflowExecution.new(nil, nil),
                                 FakeEvents.new(get_history_array().push(*[
                                   "DecisionTaskCompleted",
                                   ["TimerStarted", {decision_task_completed_event_id: 10, timer_id: "Timer1", start_to_fire_timeout: 2 }],
                                   ["TimerFired", {timer_id: "Timer1", started_event_id: 11}],
                                   "DecisionTaskScheduled",
                                   "DecisionTaskStarted",
                                   "DecisionTaskCompleted",
                                   ["ActivityTaskScheduled", {activity_id: "Activity2"}],
                                   "ActivityTaskStarted",
                                   ["ActivityTaskFailed", {scheduled_event_id: 16, :activity_id => "Activity2", cause: AWS::Flow::ActivityFailureException, details: $error } ],
                                   "DecisionTaskScheduled",
                                   "DecisionTaskStarted",
                                   "DecisionTaskCompleted",
                                   ["TimerStarted", {decision_task_completed_event_id: 21, timer_id: "Timer2", start_to_fire_timeout: 8 }],
                                   ["TimerFired", {timer_id: "Timer2", started_event_id: 22}],
                                   "DecisionTaskScheduled",
                                   "DecisionTaskStarted",
                                   "DecisionTaskCompleted",
                                   ["ActivityTaskScheduled", {activity_id: "Activity3"}],
                                   "ActivityTaskStarted",
                                   ["ActivityTaskFailed", {scheduled_event_id: 27, :activity_id => "Activity3", cause: ActivityTaskFailedException, details: $error } ],
                                   "DecisionTaskScheduled",
                                   "DecisionTaskStarted",
          ])))
        end
      end

      workflow_type_object = FakeWorkflowType.new(nil, "RetryTestWorkflow.start", "1.0")
      domain = FakeDomain.new(workflow_type_object)

      swf_client = FakeServiceClient.new
      task_list = "default"
      my_workflow_factory = workflow_factory(swf_client, domain) do |options|
        options.workflow_name = "RetryTestWorkflow"
        options.execution_start_to_close_timeout = 120
        options.task_list = "default"
        options.task_start_to_close_timeout = 30
      end

      worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, RetryTestWorkflow)
      my_workflow = my_workflow_factory.get_client
      workflow_execution = my_workflow.start_execution
      worker.start
      swf_client.trace.first[:decisions].first[:decision_type].should == "FailWorkflowExecution"
      swf_client.trace.first[:decisions].first[:fail_workflow_execution_decision_attributes][:details].should =~ /ArgumentError/
    end
  end
end

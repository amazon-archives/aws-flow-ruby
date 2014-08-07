require_relative 'setup'

describe Activity do
  let(:trace) { [] }
  let(:client) { client = GenericActivityClient.new(DecisionHelper.new, ActivityOptions.new) }

  it "ensures that schedule_activity gets set up from the activity_client" do
    client.reconfigure(:test) { |o| o.version = "blah" }
    class GenericActivityClient
      alias :old_schedule_activity :schedule_activity
      def schedule_activity(name, activity_type, input, options)
        :scheduled_activity
      end
    end
    trace << client.test
    trace.should == [:scheduled_activity]
    class GenericActivityClient
      alias :schedule_activity :old_schedule_activity
    end
  end

  describe "multiple activities" do
    it "ensures that you can schedule multiple activities with the same activity call" do
      class GenericActivityClient
        alias :old_schedule_activity :schedule_activity
        def schedule_activity(name, activity_type, input, options)
          "scheduled_activity_#{name}".to_sym
        end
      end
      client = GenericActivityClient.new(DecisionHelper.new, ActivityOptions.new)
      client.reconfigure(:test, :test2) { |o| o.version = "blah" }
      trace << client.test
      trace << client.test2
      class GenericActivityClient
        alias :schedule_activity :old_schedule_activity
      end
      trace.should == [:"scheduled_activity_.test", :"scheduled_activity_.test2"]
    end
  end
  describe GenericActivityClient do

    before(:each) do
      @options = ActivityOptions.new
      @decision_helper = DecisionHelper.new
      @client = GenericActivityClient.new(@decision_helper, @options)
      [:test, :test_nil].each do |method_name|
        @client.reconfigure(method_name) { |o| o.version = 1 }
      end
    end
    it "ensures that activities get generated correctly" do
      scope = AsyncScope.new { @client.test }
      scope.eventLoop
      @decision_helper.decision_map.values.first.
        get_decision[:schedule_activity_task_decision_attributes][:activity_type][:name].should =~
      /test/
    end

    it "ensures that an activity that has no arguments is scheduled with no input value" do
      scope = AsyncScope.new do

        @client.test_nil
      end
      scope.eventLoop
      @decision_helper.decision_map.values.first.
        get_decision[:schedule_activity_task_decision_attributes].keys.should_not include :input
    end

    it "ensures that activities pass multiple activities fine" do
      scope = AsyncScope.new do
        @client.test(1, 2, 3)
      end
      scope.eventLoop
      input = @decision_helper.decision_map.values.first.
        get_decision[:schedule_activity_task_decision_attributes][:input]
      @client.data_converter.load(input).should == [1, 2, 3]
    end

    context "github issue # 57" do
      # The following tests for github issue # 57
      before(:all) do
        class GithubIssue57Activity
          extend AWS::Flow::Activities
          activity :not_retryable do
            {
              :version => "1.0",
              :task_list => 'not_retryable_activity',
              :schedule_to_start_timeout => 10,
              :start_to_close_timeout => 10,
              :heartbeat_timeout => 2
            }
          end

          activity :retryable do
            {
              :version => "2.0",
              :task_list => 'retryable_activity',
              :schedule_to_start_timeout => 20,
              :start_to_close_timeout => 20,
              :exponential_retry => {
                :maximum_attempts => 3,
              },
            }
          end
        end

        class GithubIssue57Workflow
          extend AWS::Flow::Workflows
          workflow(:start) { { version: "1.0" } }
          activity_client(:client) { { from_class: "GithubIssue57Activity" } }
          def start
            client.not_retryable
            client.retryable
          end
        end

      end

      it "tests first activity invocation options" do
        class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
          def get_decision_task
            fake_workflow_type =  FakeWorkflowType.new(nil, "GithubIssue57Workflow.start", "1.0")
            TestHistoryWrapper.new(fake_workflow_type, FakeWorkflowExecution.new(nil, nil),
                                   [
                                     TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                                     TestHistoryEvent.new("DecisionTaskScheduled", 2, {}),
                                     TestHistoryEvent.new("DecisionTaskStarted", 3, {}),
            ])
          end
        end

        workflow_type_object = double("workflow_type", :name => "GithubIssue57Workflow.start", :start_execution => "" )
        domain = FakeDomain.new(workflow_type_object)

        swf_client = FakeServiceClient.new
        task_list = "default"
        my_workflow_factory = workflow_factory(swf_client, domain) do |options|
          options.workflow_name = "GithubIssue57Workflow"
          options.execution_start_to_close_timeout = 120
          options.task_list = task_list
          options.task_start_to_close_timeout = 30
        end

        worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, GithubIssue57Workflow)
        my_workflow = my_workflow_factory.get_client
        expect_any_instance_of(AWS::Flow::GenericClient).to_not receive(:_retry_with_options)
        workflow_execution = my_workflow.start_execution
        worker.start

        swf_client.trace.first[:decisions].first[:decision_type].should == "ScheduleActivityTask"
        attributes = swf_client.trace.first[:decisions].first[:schedule_activity_task_decision_attributes]
        attributes[:activity_type].should == { name: "GithubIssue57Activity.not_retryable", version: "1.0" }
        attributes[:heartbeat_timeout].should == "2"
        attributes[:schedule_to_start_timeout].should == "10"
        attributes[:start_to_close_timeout].should == "10"
        attributes[:task_list].should == { name: "not_retryable_activity" }
      end

      it "tests second activity invocation options" do

        class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
          def get_decision_task
            fake_workflow_type =  FakeWorkflowType.new(nil, "GithubIssue57Workflow.start", "1.0")
            TestHistoryWrapper.new(fake_workflow_type, FakeWorkflowExecution.new(nil, nil),
                                   FakeEvents.new(["WorkflowExecutionStarted",
                                                   "DecisionTaskScheduled",
                                                   "DecisionTaskStarted",
                                                   "DecisionTaskCompleted",
                                                   ["ActivityTaskScheduled", {activity_id: "Activity1"}],
                                                   "ActivityTaskStarted",
                                                   ["ActivityTaskCompleted", {scheduled_event_id: 5}],
                                                   "DecisionTaskScheduled",
                                                   "DecisionTaskStarted"
            ]))
          end
        end

        workflow_type_object = double("workflow_type", :name => "GithubIssue57Workflow.start", :start_execution => "" )
        domain = FakeDomain.new(workflow_type_object)

        swf_client = FakeServiceClient.new
        task_list = "default"
        my_workflow_factory = workflow_factory(swf_client, domain) do |options|
          options.workflow_name = "GithubIssue57Workflow"
          options.execution_start_to_close_timeout = 120
          options.task_list = "default"
          options.task_start_to_close_timeout = 30
        end

        worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, GithubIssue57Workflow)
        my_workflow = my_workflow_factory.get_client
        #expect_any_instance_of(AWS::Flow::GenericClient).to receive(:_retry_with_options)
        workflow_execution = my_workflow.start_execution
        worker.start
        binding.pry

        swf_client.trace.first[:decisions].first[:decision_type].should == "ScheduleActivityTask"
        attributes = swf_client.trace.first[:decisions].first[:schedule_activity_task_decision_attributes]
        attributes[:activity_type].should == { name: "GithubIssue57Activity.retryable", version: "2.0" }
        attributes[:heartbeat_timeout].nil?.should == true
        attributes[:schedule_to_start_timeout].should == "20"
        attributes[:start_to_close_timeout].should == "20"
        attributes[:task_list].should == { name: "retryable_activity" }

      end
      it "tests second activity invocation for exponential retry option" do

        class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
          def get_decision_task
            fake_workflow_type =  FakeWorkflowType.new(nil, "GithubIssue57Workflow.start", "1.0")
            TestHistoryWrapper.new(fake_workflow_type, FakeWorkflowExecution.new(nil, nil),
                                   FakeEvents.new(["WorkflowExecutionStarted",
                                                   "DecisionTaskScheduled",
                                                   "DecisionTaskStarted",
                                                   "DecisionTaskCompleted",
                                                   ["ActivityTaskScheduled", {activity_id: "Activity1"}],
                                                   "ActivityTaskStarted",
                                                   ["ActivityTaskCompleted", {scheduled_event_id: 5}],
                                                   "DecisionTaskScheduled",
                                                   "DecisionTaskStarted"
            ]))
          end
        end

        workflow_type_object = double("workflow_type", :name => "GithubIssue57Workflow.start", :start_execution => "" )
        domain = FakeDomain.new(workflow_type_object)

        swf_client = FakeServiceClient.new
        task_list = "default"
        my_workflow_factory = workflow_factory(swf_client, domain) do |options|
          options.workflow_name = "GithubIssue57Workflow"
          options.execution_start_to_close_timeout = 120
          options.task_list = "default"
          options.task_start_to_close_timeout = 30
        end

        worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, GithubIssue57Workflow)
        my_workflow = my_workflow_factory.get_client
        expect_any_instance_of(AWS::Flow::GenericClient).to receive(:_retry_with_options)
        workflow_execution = my_workflow.start_execution
        worker.start
      end
    end
  end
end

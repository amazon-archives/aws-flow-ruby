require_relative 'setup'

describe "Miscellaneous" do
  it "ensures with_retry can be called without including AWS::Flow" do

    class TestWorkflow
      extend AWS::Flow::Workflows
      workflow (:start) { {:version => "1"} }

      def start
        AWS::Flow::with_retry do
          return "This is the entry point"
        end
      end
    end

    class SynchronousWorkflowTaskPoller < AWS::Flow::WorkflowTaskPoller
      def get_decision_task
        workflow_type = FakeWorkflowType.new(nil, "TestWorkflow.start", "1")
        TestHistoryWrapper.new(workflow_type,
                               [TestHistoryEvent.new("WorkflowExecutionStarted", 1, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tasklist"}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tastlist"}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {:scheduled_event_id=>2, :identity=>"some_identity"}),
        ])

      end
    end

    workflow_type_object = double("workflow_type", :name => "TestWorkflow.start", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)
    swf_client = FakeServiceClient.new
    task_list = "TestWorkflow_tasklist"

    workflow_worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    workflow_worker.add_workflow_implementation(TestWorkflow)

    workflow_client = AWS::Flow::workflow_client(swf_client, domain) { { from_class: "TestWorkflow" } }
    workflow_client.start_execution

    workflow_worker.start
  end

  it "ensures decision_context can be called without including AWS::Flow" do

    class TestWorkflow
      extend AWS::Flow::Workflows
      workflow (:start) { {:version => "1"} }

      def start
        return AWS::Flow::decision_context.workflow_clock.current_time
      end
    end

    class SynchronousWorkflowTaskPoller < AWS::Flow::WorkflowTaskPoller
      def get_decision_task
        workflow_type = FakeWorkflowType.new(nil, "TestWorkflow.start", "1")
        TestHistoryWrapper.new(workflow_type,
                               [TestHistoryEvent.new("WorkflowExecutionStarted", 1, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tasklist"}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tastlist"}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {:scheduled_event_id=>2, :identity=>"some_identity"}),
        ])

      end
    end

    workflow_type_object = double("workflow_type", :name => "TestWorkflow.start", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)
    swf_client = FakeServiceClient.new
    task_list = "TestWorkflow_tasklist"

    workflow_worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    workflow_worker.add_workflow_implementation(TestWorkflow)

    workflow_client = AWS::Flow::workflow_client(swf_client, domain) { { from_class: "TestWorkflow" } }
    workflow_client.start_execution

    workflow_worker.start
  end

  it "makes sure we can remove depedency on UUIDTools" do
    class TestWorkflow
      extend AWS::Flow::Workflows
      workflow (:start) { { version: "1.0" } }
      def start; end
    end

    require "securerandom"
    # first check if SecureRandom.uuid returns uuid in the right format
    regex = /[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}/
    SecureRandom.uuid.should match(regex)

    # Now check if the uuid is correctly set in start_external_workflow method
    workflow_type = WorkflowType.new(nil, "TestWorkflow.start", "1")
    client = AWS::Flow::WorkflowClient.new(FakeServiceClient.new, FakeDomain.new(workflow_type), TestWorkflow, WorkflowOptions.new)
    workflow = client.start_execution
    workflow.workflow_id.should match(regex)
  end

end



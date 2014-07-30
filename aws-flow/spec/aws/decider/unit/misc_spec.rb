require_relative 'setup'

describe "Miscellaneous" do
  it "ensures with_retry can be called without including AWS::Flow" do

    class TestWorkflow
      extend AWS::Flow::Workflows
      workflow (:entry_point) { {:version => "1"} }

      def entry_point
        AWS::Flow::with_retry do
          return "This is the entry point"
        end
      end
    end

    class SynchronousWorkflowTaskPoller < AWS::Flow::WorkflowTaskPoller
      def get_decision_task
        workflow_type = FakeWorkflowType.new(nil, "TestWorkflow.entry_point", "1")
        TestHistoryWrapper.new(workflow_type,
                               [TestHistoryEvent.new("WorkflowExecutionStarted", 1, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tasklist"}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tastlist"}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {:scheduled_event_id=>2, :identity=>"some_identity"}),
        ])

      end
    end

    workflow_type_object = double("workflow_type", :name => "TestWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)
    swf_client = FakeServiceClient.new
    task_list = "TestWorkflow_tasklist"

    workflow_worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    workflow_worker.add_workflow_implementation(TestWorkflow)

    workflow_client = AWS::Flow::WorkflowClient.new(swf_client, domain, TestWorkflow, AWS::Flow::StartWorkflowOptions.new)
    workflow_client.start_execution

    workflow_worker.start
  end

  it "ensures decision_context can be called without including AWS::Flow" do

    class TestWorkflow
      extend AWS::Flow::Workflows
      workflow (:entry_point) { {:version => "1"} }

      def entry_point
        return AWS::Flow::decision_context.workflow_clock.current_time
      end
    end

    class SynchronousWorkflowTaskPoller < AWS::Flow::WorkflowTaskPoller
      def get_decision_task
        workflow_type = FakeWorkflowType.new(nil, "TestWorkflow.entry_point", "1")
        TestHistoryWrapper.new(workflow_type,
                               [TestHistoryEvent.new("WorkflowExecutionStarted", 1, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tasklist"}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tastlist"}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {:scheduled_event_id=>2, :identity=>"some_identity"}),
        ])

      end
    end

    workflow_type_object = double("workflow_type", :name => "TestWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)
    swf_client = FakeServiceClient.new
    task_list = "TestWorkflow_tasklist"

    workflow_worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    workflow_worker.add_workflow_implementation(TestWorkflow)

    workflow_client = AWS::Flow::WorkflowClient.new(swf_client, domain, TestWorkflow, AWS::Flow::StartWorkflowOptions.new)
    workflow_client.start_execution

    workflow_worker.start
  end

end



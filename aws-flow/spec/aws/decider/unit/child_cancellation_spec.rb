require_relative 'setup'

def get_child_cancellation_history_array
  [
      "WorkflowExecutionStarted",
      "DecisionTaskScheduled",
      "DecisionTaskStarted",
  ]
end

describe "TestChildCancellationNotInitiatedFromParent" do

  context "ChildCancellation" do
    # The following tests for swf-issue-2031
    before(:all) do
      class ParentWorkflow
        extend AWS::Flow::Workflows
        workflow :parent, :child do
          {
              version: "1.0",
              task_list: "default",
          }
        end

        def parent
          $client.child { {workflow_id: "child_workflow_test"}}
        end

        def child; end
      end

    end

    it "tests part 1 - child gets scheduled in the first decision" do

      class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
        def get_decision_task
          TestHistoryWrapper.new($type, FakeWorkflowExecution.new(nil, nil), FakeEvents.new(get_child_cancellation_history_array))
        end
      end

      $type = FakeWorkflowType.new(nil, "ParentWorkflow.parent", "1.0")

      domain = FakeDomain.new($type)
      swf_client = FakeServiceClient.new

      task_list = "default"

      $client = AWS::Flow::workflow_client(swf_client, domain) { { from_class: "ParentWorkflow" } }

      $client.start_execution

      worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, ParentWorkflow)

      worker.start

      swf_client.trace.first[:decisions].first[:decision_type].should == "StartChildWorkflowExecution"
    end

    it "tests part 2 - the workflow fails because child workflow is cancelled" do

      class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
        def get_decision_task
          TestHistoryWrapper.new($type, FakeWorkflowExecution.new(nil, nil),
                                 FakeEvents.new(get_child_cancellation_history_array().push(*[
                                     "DecisionTaskCompleted",
                                     ["StartChildWorkflowExecutionInitiated", {:workflow_id => "child_workflow_test"}],
                                     ["ChildWorkflowExecutionStarted", {:workflow_execution => FakeWorkflowExecution.new("1", "child_workflow_test"), :workflow_id => "child_workflow_test"}],
                                     ["ChildWorkflowExecutionCanceled",{:workflow_execution => FakeWorkflowExecution.new("1", "child_workflow_test")}],
                                     "DecisionTaskScheduled",
                                     "DecisionTaskStarted",
                                 ])))
        end
      end

      $type = FakeWorkflowType.new(nil, "ParentWorkflow.parent", "1.0")

      domain = FakeDomain.new($type)
      swf_client = FakeServiceClient.new

      task_list = "default"

      $client = AWS::Flow::workflow_client(swf_client, domain) { { from_class: "ParentWorkflow" } }

      $client.start_execution

      worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, ParentWorkflow)

      worker.start

      swf_client.trace.first[:decisions].first[:decision_type].should == "FailWorkflowExecution"
    end
  end
end

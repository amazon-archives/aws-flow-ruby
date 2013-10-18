
describe "will test a patch that makes sure with_retry and decision_context can be called without importing AWS::Flow module" do
  before(:all) do
    class FakeDomain
      def initialize(workflow_type_object)
        @workflow_type_object = workflow_type_object
      end
      def page; FakePage.new(@workflow_type_object); end
      def workflow_executions; FakeWorkflowExecutionCollecton.new; end
      def name; "fake_domain"; end
    end

    class FakeWorkflowExecutionCollecton
      def at(workflow_id, run_id); "Workflow_execution"; end
    end

    # A fake service client used to mock out calls to the Simple Workflow Service
    class FakeServiceClient
      attr_accessor :trace
      def respond_decision_task_completed(task_completed_request)
        @trace ||= []
        @trace << task_completed_request
      end
      def start_workflow_execution(options)
        @trace ||= []
        @trace << options
        {"runId" => "blah"}
      end
      def register_workflow_type(options)
      end
    end

    class SynchronousWorkflowWorker < AWS::Flow::WorkflowWorker
      def start
        poller = SynchronousWorkflowTaskPoller.new(@service, nil, AWS::Flow::DecisionTaskHandler.new(@workflow_definition_map), @task_list)
        poller.poll_and_process_single_task
      end
    end

    class FakeWorkflowType < AWS::Flow::WorkflowType
      attr_accessor :domain, :name, :version
      def initialize(domain, name, version)
        @domain = domain
        @name = name
        @version = version
      end
    end

    class TestHistoryWrapper
      def initialize(workflow_type, events)
        @workflow_type = workflow_type
        @events = events
      end
      def workflow_execution
        FakeWorkflowExecution.new
      end
      def task_token
        "1"
      end
      def previous_started_event_id
        1
      end
      attr_reader :events, :workflow_type
    end

    class FakeWorkflowExecution
      def run_id
        "1"
      end
    end

    class TestHistoryEvent < AWS::SimpleWorkflow::HistoryEvent
      def initialize(event_type, event_id, attributes)
        @event_type = event_type
        @attributes = attributes
        @event_id = event_id
        @created_at = Time.now
      end
    end

    class SynchronousWorkflowTaskPoller < AWS::Flow::WorkflowTaskPoller
      def get_decision_tasks
        workflow_type = FakeWorkflowType.new(nil, "TestWorkflow.entry_point", "1")
        TestHistoryWrapper.new(workflow_type,
                               [TestHistoryEvent.new("WorkflowExecutionStarted", 1, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tasklist"}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> workflow_type, :task_list=>"TestWorkflow_tastlist"}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {:scheduled_event_id=>2, :identity=>"some_identity"}),
        ])

      end
    end

  end
  it "makes sure that with_retry can be called without including AWS::Flow" do

    class TestWorkflow
      extend AWS::Flow::Workflows
      workflow (:entry_point) { {:version => "1"} }

      def entry_point
        AWS::Flow::with_retry do
          return "This is the entry point"
        end
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

  it "makes sure that decision_context can be called without including AWS::Flow" do

    class TestWorkflow
      extend AWS::Flow::Workflows
      workflow (:entry_point) { {:version => "1"} }

      def entry_point
        return AWS::Flow::decision_context.workflow_clock.current_time
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



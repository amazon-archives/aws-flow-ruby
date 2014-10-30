require_relative 'setup'


describe WorkflowClient do

  class TestWorkflow
    extend AWS::Flow::Workflows

    workflow :start
    def start
      return "This is the entry point"
    end
  end
  before(:each) do
    workflow_type_object = double("workflow_type", :name => "TestWorkflow.start", :start_execution => "" )
    @client = WorkflowClient.new(FakeServiceClient.new, FakeDomain.new(workflow_type_object), TestWorkflow, StartWorkflowOptions.new)
  end
  it "makes sure that configure works correctly" do
    @client.reconfigure(:start) {{ :task_list => "This nonsense" }}
    @client.start
  end

  context "#start_workflow" do

    before(:all) do
      class WorkflowClientTestWorkflow
        extend AWS::Flow::Workflows
        workflow :workflow_a, :workflow_b do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: "tasklist_a"
          }
        end
        workflow :workflow_c do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 300,
            default_task_list: "tasklist_c"
          }
        end
      end
    end

    context "multiple workflow definitions in a single workflow class" do

      it "ensures we can have multiple workflows in one class - by calling the first workflow" do
        type = FakeWorkflowType.new("domain", "WorkflowClientTestWorkflow.workflow_a", "1.0")
        domain = FakeDomain.new(type)
        swf = AWS::SimpleWorkflow.new
        client = AWS::Flow::workflow_client(swf.client, domain) { { from_class: "WorkflowClientTestWorkflow" } }
        expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:start_workflow_execution).twice { |options| options[:workflow_type][:name].should == "WorkflowClientTestWorkflow.workflow_a"; { run_id: "run_id" } }
        client.start_execution
        client.workflow_a
      end

      it "ensures we can have multiple workflows in one class - by calling the second workflow" do
        type = FakeWorkflowType.new("domain", "WorkflowClientTestWorkflow.workflow_c", "1.0")
        domain = FakeDomain.new(type)
        swf = AWS::SimpleWorkflow.new
        client = AWS::Flow::workflow_client(swf.client, domain) { { from_class: "WorkflowClientTestWorkflow" } }
        expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:start_workflow_execution) { |options| options[:workflow_type][:name].should == "WorkflowClientTestWorkflow.workflow_c"; { run_id: "run_id" } }
        client.workflow_c
      end

      it "ensures we can define multiple workflows with same options" do
     
        type = FakeWorkflowType.new("domain", "WorkflowClientTestWorkflow.workflow_a", "1.0")
        domain = FakeDomain.new(type)
        swf = AWS::SimpleWorkflow.new
        client = AWS::Flow::workflow_client(swf.client, domain) { { from_class: "WorkflowClientTestWorkflow" } }
        expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:start_workflow_execution) { |options| options[:workflow_type][:name].should == "WorkflowClientTestWorkflow.workflow_a"; { run_id: "run_id" } }
        expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:start_workflow_execution) { |options| options[:workflow_type][:name].should == "WorkflowClientTestWorkflow.workflow_b"; { run_id: "run_id" } }
        client.workflow_a
        client.workflow_b
      end

      it "ensures workflow client uses user supplied data_converter" do
        class FooWorkflow
          extend AWS::Flow::Workflows
          workflow :foo_workflow do
            { version: "1.0" }
          end
        end
        class FooDataConverter; end

        swf = double(AWS::SimpleWorkflow)
        domain = double(AWS::SimpleWorkflow::Domain)

        swf.stub(:start_workflow_execution).and_return({"runId" => "111"})
        domain.stub(:name)
        array = []
        domain.stub(:workflow_executions).and_return(array)
        array.stub(:at)

        client = AWS::Flow::workflow_client(swf, domain) { { from_class: "WorkflowClientTestWorkflow" } }
        expect_any_instance_of(FooDataConverter).to receive(:dump)
        client.start_execution(:foo_workflow, "some_input") { { data_converter: FooDataConverter.new } }
      end

    end

  end
end



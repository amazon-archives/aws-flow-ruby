require "spec_helper"

describe Replayer do

  describe DecisionTaskProvider do

    context "#get_decision_task" do

      it "constructs the correct DecisionTask object" do
        task_provider = DecisionTaskProvider.new

        allow(task_provider).to receive(:get_history).and_return(
          ["event_1", "event_2", "event_3"]
        )

        AWS::SimpleWorkflow::HistoryEvent.stub(:new) { |x,y| y }

        allow(task_provider).to receive(:get_execution_info).and_return({
          'execution' => {
            'workflowId' => 'workflow_id',
            'runId' => 'run_id'
          },
          'workflowType' => {
            'name' => 'FooWorkflow',
            'version' => '1.0'
          }
        })

        task = task_provider.get_decision_task
        (task.is_a? AWS::SimpleWorkflow::DecisionTask).should be_true
        task.workflow_execution.workflow_id.should == 'workflow_id'
        task.workflow_execution.run_id.should == 'run_id'
        task.workflow_type.name.should == 'FooWorkflow'
        task.workflow_type.version.should == '1.0'
        task.next_token.should be_nil
        task.task_token.should be_nil
        task.events.to_a.should == ["event_1", "event_2", "event_3"]

      end

      it "returns nil if event list is empty" do
        task_provider = DecisionTaskProvider.new
        allow(task_provider).to receive(:get_history_page).and_return([])
        allow(task_provider).to receive(:get_execution_info).and_return({
          'execution'=> {
            workflow_id: '',
            run_id: ''
          },
          'nextPageToken'=>'foo'
        })
        task = task_provider.get_decision_task
        task.should be_nil
      end

      it "truncates history correctly" do
        task_provider = DecisionTaskProvider.new

        allow(task_provider).to receive(:get_history).and_return(
          [ { "eventId" => 1 }, { "eventId" => 2 }, { "eventId" => 3 } ]
        )

        AWS::SimpleWorkflow::HistoryEvent.stub(:new) { |x,y| y }

        allow(task_provider).to receive(:get_execution_info).and_return({
          'execution' => {
            'workflowId' => 'workflow_id',
            'runId' => 'run_id'
          },
          'workflowType' => {
            'name' => 'FooWorkflow',
            'version' => '1.0'
          }
        })

        task = task_provider.get_decision_task(2)
        task.events.to_a.should == [ { "eventId" => 1 }, { "eventId" => 2 } ]

        task = task_provider.get_decision_task(5)
        task.events.to_a.should == [ { "eventId" => 1 }, { "eventId" => 2 }, { "eventId" => 3 } ]

        task = task_provider.get_decision_task(0)
        task.should be_nil

        task = task_provider.get_decision_task(-1)
        task.should be_nil
      end

    end

  end

  describe ServiceDecisionTaskProvider do

    context "#get_history" do

      it "concatenates paginated history correctly" do
        task_provider = ServiceDecisionTaskProvider.new(
          domain: 'foo',
          execution: 'bar'
        )

        allow(task_provider).to receive(:get_history_page).and_return(
          { 'events' => ["event_1", "event_2", "event_3"], 'nextPageToken' => "foo" },
        )
        allow(task_provider).to receive(:get_history_page).with("foo").and_return(
          { 'events' => ["event_4", "event_5", "event_6"], 'nextPageToken' => nil },
        )

        history = task_provider.get_history
        history.should == ["event_1", "event_2", "event_3", "event_4", "event_5", "event_6"]

      end

      it "returns nil if event list is empty" do
        task_provider = ServiceDecisionTaskProvider.new(
          domain: 'foo',
          execution: 'bar'
        )

        allow(task_provider).to receive(:get_history_page).and_return(
          {'events'=>[], 'nextPageToken'=>nil}
        )

        history = task_provider.get_history
        history.should be_empty
      end

    end

    context "#get_history_page" do

      it "calls the service with the correct parameters" do
        expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125)
        .to receive(:get_workflow_execution_history).once.with(
          domain: 'foo',
          execution: 'bar',
        )
        expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125)
        .to receive(:get_workflow_execution_history).once.with(
          domain: 'foo',
          execution: 'bar',
          next_page_token: 'next_page'
        )

        task_provider = ServiceDecisionTaskProvider.new(
          domain: 'foo',
          execution: 'bar'
        )

        task_provider.get_history_page
        task_provider.get_history_page("next_page")
      end

    end

    context "#get_execution_info" do

      it "calls the service with the correct parameters" do
        expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125)
        .to receive(:describe_workflow_execution).once.with(
          domain: 'foo',
          execution: 'bar',
        ).and_return( { "executionInfo" => "foo" } )

        task_provider = ServiceDecisionTaskProvider.new(
          domain: 'foo',
          execution: 'bar'
        )

        task_provider.get_execution_info
      end

    end

  end

  describe WorkflowReplayer do

    context "#initialize" do

      it "initializes WorkflowReplayer with the correct options" do
        expect{WorkflowReplayer.new(nil)}.to raise_error(ArgumentError)
        expect{WorkflowReplayer.new({})}.to raise_error(ArgumentError)
      end

      it "initializes :task_handler correctly" do
        class ReplayerTestWorkflowClass
          extend AWS::Flow::Workflows
          workflow :a, :b, :c do
            {
              version: "1.0",
            }
          end
        end

        expect_any_instance_of(AWS::SimpleWorkflow).to receive(:client).and_return(nil)
        WorkflowDefinitionFactory.stub(:new).and_return("MyWorkflowDefinitionFactory")

        replayer = WorkflowReplayer.new(
          domain: 'Foo',
          execution: 'Bar',
          workflow_class: ReplayerTestWorkflowClass
        )
        (replayer.task_handler.is_a? DecisionTaskHandler).should be_true

        definitons = replayer.task_handler.workflow_definition_map.map do |key, value|
          [key.name, value]
        end
        definitons.should == [
          ["ReplayerTestWorkflowClass.a", "MyWorkflowDefinitionFactory"],
          ["ReplayerTestWorkflowClass.b", "MyWorkflowDefinitionFactory"],
          ["ReplayerTestWorkflowClass.c", "MyWorkflowDefinitionFactory"],
        ]

      end

      it "initializes :task_provider correctly" do
        expect_any_instance_of(AWS::SimpleWorkflow).to receive(:client).and_return(nil)

        DecisionTaskHandler.stub(:from_workflow_class)

        replayer = WorkflowReplayer.new(
          domain: 'Foo',
          execution: 'Bar',
          workflow_class: ReplayerTestWorkflowClass
        )
        (replayer.task_provider.is_a? ServiceDecisionTaskProvider).should be_true
        replayer.task_provider.domain.should == 'Foo'
        replayer.task_provider.execution.should == 'Bar'
        replayer.task_provider.swf.should be_nil

      end

    end

    context "#replay" do

      before(:all) do
        class ReplayerTestWorkflowClass
          extend AWS::Flow::Workflows
          workflow :a do
            {
              version: "1.0",
            }
          end
        end
      end

      it "uses correct logic" do
        expect_any_instance_of(AWS::SimpleWorkflow).to receive(:client).and_return(nil)
        expect_any_instance_of(ServiceDecisionTaskProvider).to receive(:get_decision_task).and_return("foo")
        expect_any_instance_of(DecisionTaskHandler).to receive(:handle_decision_task).with("foo")

        WorkflowDefinitionFactory.stub(:new).and_return("MyWorkflowDefinitionFactory")

        replayer = WorkflowReplayer.new(
          domain: 'Foo',
          execution: 'Bar',
          workflow_class: ReplayerTestWorkflowClass
        )

        replayer.replay
      end

      it "can replay multiple times for same execution" do
        expect_any_instance_of(AWS::SimpleWorkflow).to receive(:client).and_return(nil)
        expect_any_instance_of(ServiceDecisionTaskProvider).to receive(:get_decision_task).with(nil).and_return("foo")
        expect_any_instance_of(ServiceDecisionTaskProvider).to receive(:get_decision_task).with(1).and_return("foo")
        expect_any_instance_of(ServiceDecisionTaskProvider).to receive(:get_decision_task).with(10).and_return("foo")
        expect_any_instance_of(DecisionTaskHandler).to receive(:handle_decision_task).with("foo").exactly(3).times

        WorkflowDefinitionFactory.stub(:new).and_return("MyWorkflowDefinitionFactory")

        replayer = WorkflowReplayer.new(
          domain: 'Foo',
          execution: 'Bar',
          workflow_class: ReplayerTestWorkflowClass
        )

        replayer.replay
        replayer.replay(1)
        replayer.replay(10)

      end

    end

  end

end

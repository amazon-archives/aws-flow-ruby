require "spec_helper"
include Test::Integ

describe Replayer do
  before(:all) do

    @swf, @domain = setup_swf

    class ReplayerTestActivity
      extend Activities
      activity :activity_a do
        {
          version: "1.0"
        }
      end
      def activity_a; end
    end

    class ReplayerTestWorkflow
      extend Workflows
      workflow :small, :large do
        {
          version: "1.0",
          default_execution_start_to_close_timeout: 600
        }
      end
      activity_client(:client) { { from_class: "ReplayerTestActivity" } }
      def small
        client.activity_a
      end
      def large
        25.times { client.send_async(:activity_a) }
        create_timer(1)
      end
    end

    class ReplayerIntegTest
      class << self
        def run(domain, method)
          workflow_worker = AWS::Flow::WorkflowWorker.new(domain.client, domain, "replayer_wf_tasklist", ReplayerTestWorkflow)
          activity_worker = AWS::Flow::ActivityWorker.new(domain.client, domain, "replayer_act_tasklist", ReplayerTestActivity)
          workflow_worker.register
          activity_worker.register

          executor = AWS::Flow::ForkingExecutor.new(max_workers: 5)
          executor.execute { activity_worker.start }
          executor.execute { workflow_worker.start }

          client = AWS::Flow::workflow_client(domain.client, domain) { { from_class: "ReplayerTestWorkflow" } }
          execution = client.send(method)
          wait_for_execution(execution)
          executor.shutdown(1)
          execution
        end
      end
    end
  end

  it "tests a small workflow history as a sanity test" do
    execution = ReplayerIntegTest.run(@domain, :small)
    replayer = WorkflowReplayer.new(
      domain: @domain.name,
      execution: {
        workflow_id: execution.workflow_id,
        run_id: execution.run_id
      },
      workflow_class: ReplayerTestWorkflow
    )

    replayer.replay(32)[:decisions].first[:decision_type].should == "CompleteWorkflowExecution"
  end

  it "tests a large workflow history to ensure paged histories are loaded correctly" do
    execution = ReplayerIntegTest.run(@domain, :large)
    replayer = WorkflowReplayer.new(
      domain: @domain.name,
      execution: {
        workflow_id: execution.workflow_id,
        run_id: execution.run_id
      },
      workflow_class: ReplayerTestWorkflow
    )

    decisions = replayer.replay(3)
    decisions = decisions[:decisions].map{ |x| x[:decision_type] }
    decisions.select { |x| x == "ScheduleActivityTask" }.size.should == 25
    decisions.select { |x| x == "StartTimer" }.size.should == 1

  end

end

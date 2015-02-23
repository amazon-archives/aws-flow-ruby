require_relative 'setup'

describe "task_priority" do

  before(:all) do
    @swf, @domain = setup_swf
    kill_executors
  end

  context "activities and workflows" do

    before(:all) do

      class ActivityForTaskPriority
        extend AWS::Flow::Activities
        activity :run_1 do
          {
            version: "1.0",
            default_task_priority: 20
          }
        end
        activity :run_2 do
          {
            version: "1.0"
          }
        end
        def run_1; end
      end

      class WorkflowForTaskPriority
        extend AWS::Flow::Workflows
        workflow :start do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 300,
            default_task_priority: "10",
          }
        end
        activity_client(:activity) { { :from_class => "ActivityForTaskPriority"} }
        def start
          activity.run_1
          activity.run_2
        end
      end

    end

    it "ensures that default_task_priority values are assigned when workflow is started and when activity is scheduled" do
      task_list = "activity_task_list1"

      worker = WorkflowWorker.new(@domain.client, @domain, task_list, WorkflowForTaskPriority)
      activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, ActivityForTaskPriority)
      worker.register
      activity_worker.register

      type = @domain.workflow_types.to_a.find { |x| x.name == "WorkflowForTaskPriority.start" }
      type.default_task_priority.should == 10

      type = @domain.activity_types.to_a.find { |x| x.name == "ActivityForTaskPriority.run_1" }
      type.default_task_priority.should == 20

      type = @domain.activity_types.to_a.find { |x| x.name == "ActivityForTaskPriority.run_2" }
      type.default_task_priority.should == 0

      client = AWS::Flow::workflow_client(@domain.client, @domain) { {from_class: "WorkflowForTaskPriority"} }
      execution = client.start_execution

      worker.run_once
      activity_worker.run_once
      worker.run_once
      activity_worker.run_once
      worker.run_once
      wait_for_execution(execution)

      event = execution.events.select { |x| x.event_type =~ /ActivityTaskScheduled/ }
      event.first.attributes[:task_priority].should == 20
      event.last.attributes[:task_priority].should == 0
      events = execution.events.select { |x| x.event_type =~ /DecisionTaskScheduled/ }
      events.first.attributes[:taskPriority].should == 10
    end

    it "ensures that overriden values of task priority are assigned when workflow is started and when activity is scheduled" do
      task_list = "activity_task_list1"
      class WorkflowForTaskPriority
        def start
          activity.run_1 { { task_priority: "200" } }
        end
      end
      worker = WorkflowWorker.new(@domain.client, @domain, task_list, WorkflowForTaskPriority)
      activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, ActivityForTaskPriority)
      worker.register
      activity_worker.register
      type = @domain.workflow_types.to_a.find { |x| x.name == "WorkflowForTaskPriority.start" }
      type.default_task_priority.should == 10

      client = AWS::Flow::workflow_client(@domain.client, @domain) { {from_class: "WorkflowForTaskPriority"} }
      execution = client.start_execution { { task_priority: "100" } }

      worker.run_once
      activity_worker.run_once
      worker.run_once

      wait_for_execution(execution)

      event = execution.events.select { |x| x.event_type =~ /ActivityTaskScheduled/ }
      event.first.attributes[:task_priority].should == 200
      events = execution.events.select { |x| x.event_type =~ /DecisionTaskScheduled/ }
      events.first.attributes[:taskPriority].should == 100
    end

  end

  context "continue_as_new" do
    before (:all) do
      class ContinueAsNewPriorityWorkflow
        extend AWS::Flow::Workflows
        workflow :entry_point do
          {
            version: "1.0",
            default_task_priority: 50,
            default_execution_start_to_close_timeout: 60,
            default_tag_list: ["continue_task_priority_test"]
          }
        end
        def entry_point
          continue_as_new { { task_list: "continue_as_new_foo" } }
        end
      end

    end

    it "makes sure that continue_as_new takes parent's task priority" do
      worker = AWS::Flow::WorkflowWorker.new(@domain.client, @domain, "continue_as_new", ContinueAsNewPriorityWorkflow)
      worker.register
      client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "ContinueAsNewPriorityWorkflow" } }

      execution = client.entry_point { { task_list: "continue_as_new" } }
      worker.run_once
      sleep 1
      wait_for_execution(execution)
      execution.events.map(&:event_type).last.should == "WorkflowExecutionContinuedAsNew"
      execution.status.should == :continued_as_new
      events = execution.events.select { |x| x.event_type == "WorkflowExecutionContinuedAsNew" }
      events.first.attributes.task_priority.should == 50
      @domain.workflow_executions.tagged("continue_task_priority_test").each { |x| x.terminate }
    end


    it "makes sure continue_as_new overrides parent's task priority" do
      class ContinueAsNewPriorityWorkflow
        def entry_point
          continue_as_new { { task_priority: 100 } }
        end
      end

      worker = AWS::Flow::WorkflowWorker.new(@domain.client, @domain, "continue_as_new_1", ContinueAsNewPriorityWorkflow)
      worker.register
      client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "ContinueAsNewPriorityWorkflow" } }

      execution = client.entry_point { { task_list: "continue_as_new_1" } }
      worker.run_once
      sleep 1
      wait_for_execution(execution)
      execution.events.map(&:event_type).last.should == "WorkflowExecutionContinuedAsNew"
      execution.status.should == :continued_as_new
      events = execution.events.select { |x| x.event_type == "WorkflowExecutionContinuedAsNew" }
      events.first.attributes.task_priority.should == 100
      @domain.workflow_executions.tagged("continue_task_priority_test").each { |x| x.terminate }
    end
  end

  context "child_workflows" do

    it "test whether task priority is overridden for child workflow" do
      class ChildWorkflowsTaskPriorityTestChildWorkflow
        extend AWS::Flow::Workflows
        workflow :child do
          {
            version: "1.0",
            default_task_priority: 100,
            default_execution_start_to_close_timeout: 600,
            default_task_start_to_close_timeout:10,
          }
        end
        def child; sleep 1; end
      end

      class ChildWorkflowsTaskPriorityTestParentWorkflow
        extend AWS::Flow::Workflows
        workflow :parent do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: "test"
          }
        end
        def parent
          domain = get_test_domain
          client = AWS::Flow::workflow_client(domain.client, domain) { { from_class: "ChildWorkflowsTaskPriorityTestChildWorkflow", task_list: "test2" } }
          client.start_execution
        end
      end

      parent_client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "ChildWorkflowsTaskPriorityTestParentWorkflow" } }
      @child_worker = WorkflowWorker.new(@domain.client, @domain, "test2", ChildWorkflowsTaskPriorityTestChildWorkflow)
      @parent_worker = WorkflowWorker.new(@domain.client, @domain, "test", ChildWorkflowsTaskPriorityTestParentWorkflow)
      @child_worker.register
      @parent_worker.register

      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @parent_worker.start }
      @forking_executor.execute { @child_worker.start }
      @forking_executor.execute { @child_worker.start }

      workflow_execution = parent_client.start_execution
      wait_for_execution(workflow_execution)

      events = workflow_execution.events.map(&:event_type)
      events.should include("ChildWorkflowExecutionStarted", "ChildWorkflowExecutionCompleted", "WorkflowExecutionCompleted")
      events = workflow_execution.events.select { |x| x.event_type =~ /ChildWorkflowExecutionStarted/ }
      events[0].attributes.workflow_execution.task_priority.should == 100
      @forking_executor.shutdown 0
    end

  end

  after(:all) do
    Test::Integ.kill_executors
  end

end

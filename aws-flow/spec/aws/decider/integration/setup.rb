require 'spec_helper'
require 'yaml'
require 'aws-sdk-v1'
require 'logger'

include Test::Integ

class SimpleTestHistoryEvent
  def initialize(id); @id = id; end
  def attributes; TestHistoryAttributes.new(@id); end
end
class TestHistoryAttributes
  def initialize(id); @id = id; end
  [:activity_id, :workflow_id, :timer_id].each do |method|
    define_method(method) { @id }
  end
end

class ParentActivity
  class << self
    attr_accessor :task_list
  end
end
class ParentWorkflow
  class << self
    attr_accessor :task_list, :activity_class
  end
end

class GeneralActivity
  class << self; attr_accessor :task_list; end
end
class MyWorkflow
  class << self; attr_accessor :task_list; end
end

def general_test(attributes, &block)
  task_list = attributes[:task_list] || "general_task_list"
  class_name = attributes[:class_name] || "General"

  new_activity_class = Class.new(ParentActivity) do
    extend AWS::Flow::Activities
    activity :run_activity1, :run_activity2 do
      {
        default_task_list: task_list,
        default_task_schedule_to_start_timeout: "60",
        default_task_start_to_close_timeout: "60",
        version: "1.0",
        prefix_name: "#{class_name}Activity",
      }
    end
    def run_activity1; end
    def run_activity2; end
  end
  @activity_class = Object.const_set("#{class_name}Activity", new_activity_class)
  new_workflow_class = Class.new(ParentWorkflow) do
    extend AWS::Flow::Workflows
    workflow(:entry_point) {
      {
        version: "1.0",
        default_execution_start_to_close_timeout: 300,
        default_task_list: task_list,
        prefix_name: "#{class_name}Workflow"
      }
    }
    def entry_point
      activity.run_activity1
    end
  end

  @workflow_class = Object.const_set("#{class_name}Workflow", new_workflow_class)
  @workflow_class.activity_class = @activity_class
  @workflow_class.task_list = task_list
  @activity_class.task_list = task_list
  @workflow_class.class_eval do
    activity_client(:activity) { { from_class: self.activity_class } }
  end
  @worker = WorkflowWorker.new(@domain.client, @domain, task_list, @workflow_class)
  @activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, @activity_class)

  @worker.register
  @activity_worker.register
  @my_workflow_client = workflow_client(@domain.client, @domain) { { from_class: @workflow_class } }
end

shared_context "setup integration tests" do
  before(:all) do
    @bucket = ENV['AWS_SWF_BUCKET_NAME']
    ENV['AWS_SWF_BUCKET_NAME'] = nil

    class MyWorkflow
      extend AWS::Flow::Workflows
      version "1"
      # TODO more of the stuff from the proposal
    end

    @swf, @domain = setup_swf
    if @domain.workflow_executions.with_status(:open).count.count > 0
      @domain.workflow_executions.with_status(:open).each { |wf| wf.terminate }
    end
  end
  before(:each) do
    kill_executors
    kill_executors
  end
  after(:each) do
    kill_executors
    kill_executors
  end
  after(:all) do
    ENV['AWS_SWF_BUCKET_NAME'] = @bucket
  end
end

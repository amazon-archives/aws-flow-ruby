##
# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
##

require 'yaml'

require 'aws/decider'
include AWS::Flow


$RUBYFLOW_DECIDER_DOMAIN = "rubyflow_decider_domain_06-12-2012"
$RUBYFLOW_DECIDER_TASK_LIST = 'test_task_list'
class TrivialConverter
  def dump(x)
    x
  end
  def load(x)
    x
  end
end

class FakePage
  def initialize(object); @object = object; end
  def page; @object; end
end

class FakeWorkflowExecution
  def run_id
    "1"
  end
end

class FakeWorkflowExecutionCollecton
  def at(workflow_id, run_id); "Workflow_execution"; end
end

class FakeDomain
  def initialize(workflow_type_object)
    @workflow_type_object = workflow_type_object
  end
  def page; FakePage.new(@workflow_type_object); end
  def workflow_executions; FakeWorkflowExecutionCollecton.new; end
  def name; "fake_domain"; end
end


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
  end
end

describe WorkflowClient do
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
  class TestWorkflow
    extend Decider

    entry_point :entry_point
    def entry_point
      return "This is the entry point"
    end
  end
  before(:each) do
    workflow_type_object = double("workflow_type", :name => "TestWorkflow.entry_point", :start_execution => "" )
    @client = WorkflowClient.new(FakeServiceClient.new, FakeDomain.new(workflow_type_object), TestWorkflow, StartWorkflowOptions.new)
  end
  it "makes sure that configure works correctly" do
    @client.reconfigure(:entry_point) {{ :task_list => "This nonsense" }}
    @client.entry_point

  end
end

describe ActivityDefinition do
  class MyActivity
    extend Activity
    def test_three_arguments(a, b, c)
      a + b + c
    end
    def test_no_arguments()
      :no_arguments
    end
    def test_one_argument(arg)
      arg
    end
    def test_getting_context
      self.activity_execution_context
    end
    activity :test_three_arguments, :test_no_arguments, :test_one_argument
  end
  it "ensures that an activity definition can handle one argument" do
    activity_definition = ActivityDefinition.new(MyActivity.new, :test_one_argument, nil , nil, TrivialConverter.new)
    activity_definition.execute(5, nil).should == 5
  end
  it "ensures that you can get the activity context " do
    activity_definition = ActivityDefinition.new(MyActivity.new, :test_getting_context, nil , nil, TrivialConverter.new)
    (activity_definition.execute(nil, ActivityExecutionContext.new(nil, nil, nil)).is_a? ActivityExecutionContext).should == true
  end
  it "ensures that the activity context gets unset after the execute" do
    activity_definition = ActivityDefinition.new(MyActivity.new, :test_getting_context, nil , nil, TrivialConverter.new)
    activity_definition.execute(nil, ActivityExecutionContext.new(nil, nil, nil))
    begin
      activity_definition.execute(nil, nil)
    rescue Exception => e
      e.backtrace.should include "No activity execution context"
    end
  end
  it "ensures that an activity definition can handle multiple arguments" do
    activity_definition = ActivityDefinition.new(MyActivity.new, :test_three_arguments, nil , nil, TrivialConverter.new)
    activity_definition.execute([1,2,3], nil).should == 6
  end
  it "ensures that an activity definition can handle no arguments" do
    activity_definition = ActivityDefinition.new(MyActivity.new, :test_no_arguments, nil , nil, TrivialConverter.new)
    activity_definition.execute(nil, nil).should == :no_arguments
  end
end

describe WorkflowDefinitionFactory do
  before(:each) do
    class MyWorkflow
      extend Decider
      version "1"
      def no_arguments
        :no_arguments
      end
      def one_argument(arg)
        arg
      end
      def multiple_arguments(arg1, arg2, arg3)
        arg3
      end
    end
    class WorkflowDefinition
      attr_accessor :decision_helper, :workflow_method, :converter
    end
  end
  let(:fake_decision_context) { stub(:decision_helper => nil) }
  let(:workflow_definition) do
    FlowFiber.stub(:current) { Hash.new(Hash.new) }
    WorkflowDefinitionFactory.new(MyWorkflow, nil, nil, nil, nil, nil, nil).get_workflow_definition(fake_decision_context) end
  it "makes sure that workflowDefinitionFactory#get_workflow_definition returns different instances" do
    FlowFiber.stub(:current) { Hash.new(Hash.new) }
    workflow_factory = WorkflowDefinitionFactory.new(MyWorkflow, nil, nil, nil, nil, nil ,nil)
    first_definition = workflow_factory.get_workflow_definition(fake_decision_context)
    second_definition = workflow_factory.get_workflow_definition(fake_decision_context)
    (first_definition.object_id == second_definition.object_id).should == false
  end
  describe "Testing the input/output" do
    before(:each) do
      workflow_definition.converter = TrivialConverter.new
    end
    it "ensures that a workflow definition can handle multiple arguments" do
      workflow_definition.workflow_method = :multiple_arguments
      AsyncScope.new do
        workflow_definition.execute([1, 2, 3]).get
      end.eventLoop
    end
    it "ensures that a workflow definition can handle no arguments" do
      workflow_definition.workflow_method = :no_arguments
      AsyncScope.new do
        workflow_definition.execute(nil).get.should == :no_arguments
      end.eventLoop
    end
    it "ensures that a workflow definition can handle one argument" do
      workflow_definition.workflow_method = :one_argument
      AsyncScope.new do
        workflow_definition.execute(5).get.should == 5
      end.eventLoop
    end
  end
end
p
describe ForkingExecutor do
  it "makes sure that forking executors basic execute works" do
    test_file_name = "ForkingExecutorTestFile"
    begin
      forking_executor = ForkingExecutor.new
      File.exists?(test_file_name).should == false
      forking_executor.execute do
        File.new(test_file_name, 'w')
      end
      sleep 3
      File.exists?(test_file_name).should == true
    ensure

      File.unlink(test_file_name)
    end
  end

  it "ensures that you cannot execute more tasks on a shutdown executor" do
    forking_executor = ForkingExecutor.new
    forking_executor.execute do
    end
    forking_executor.execute do
    end
    forking_executor.shutdown(1)
    expect { forking_executor.execute { "yay" } }.to raise_error
    RejectedExecutionException
  end

end

describe AsyncDecider do
  before(:each) do
    @decision_helper = DecisionHelper.new
    @history_helper = double(HistoryHelper)
  end

end
describe YAMLDataConverter do
  let(:converter) {YAMLDataConverter.new}
  describe "ensures that x == load(dump(x)) is true" do
    {
      Fixnum => 5,
      String => "Hello World",
      Hash => {:test => "good"},
      Array => ["Hello", "World", 5],
      Symbol => :test,
      NilClass => nil
    }.each_pair do |klass, exemplar|
      it "tests #{klass}" do
        1.upto(10).each do |i|
          converted_exemplar = exemplar
          i.times {converted_exemplar = converter.dump converted_exemplar}
          i.times {converted_exemplar = converter.load converted_exemplar}
          converted_exemplar.should == exemplar
        end
      end
    end
  end
end

describe WorkflowFactory do
  it "ensures that you can create a workflow_client without access to the Workflow definition" do
    workflow_type_object = double("workflow_type", :name => "NonExistantWorkflow.some_entry_method", :start_execution => "" )
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
    domain = FakeDomain.new(workflow_type_object)
    swf_client = FakeServiceClient.new
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "NonExistantWorkflow"
      options.execution_method = "some_entry_method"
    end
    # We want to make sure that we get to trying to start the execution on the
    # workflow_type. The workflow_type will be nil, since we return an empty
    # array in the domain.
    my_workflow_factory.get_client.start_execution
  end
end

describe "FakeHistory" do
  before(:all) do
    class WorkflowClock
      alias_method :old_current_time, :current_time
      def current_time
        Time.now
      end
    end

    class SynchronousWorkflowWorker < WorkflowWorker
      def start
        poller = SynchronousWorkflowTaskPoller.new(@service, nil, DecisionTaskHandler.new(@workflow_definition_map), @task_list)
        poller.poll_and_process_single_task
      end
    end

    class FakeServiceClient
      attr_accessor :trace
      def respond_decision_task_completed(task_completed_request)
        @trace ||= []
        @trace << task_completed_request
      end
      def start_workflow_execution(options)
        {"runId" => "blah"}
      end
    end

    class Hash
      def to_h; self; end
    end

    class TestHistoryEvent < AWS::SimpleWorkflow::HistoryEvent
      def initialize(event_type, event_id, attributes)
        @event_type = event_type
        @attributes = attributes
        @event_id = event_id
        @created_at = Time.now
      end
    end

    class FakeWorkflowType < WorkflowType
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
  end
  after(:all) do
    class WorkflowClock
      alias_method :current_time, :old_current_time
    end

  end


  it "reproduces a bug found by a customer" do
    class BadWorkflow
      class << self
        attr_accessor :task_list
      end
      extend Decider

      version "1"
      entry_point :entry_point
      def entry_point
        # pass
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)


    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
      options.task_start_to_close_timeout = 10
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution(5)


    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [TestHistoryEvent.new("WorkflowExecutionStarted", 1, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> fake_workflow_type, :task_list=>"BadWorkflow"}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {:parent_initiated_event_id=>0, :child_policy=>:request_cancel, :execution_start_to_close_timeout=>3600, :task_start_to_close_timeout=>5, :workflow_type=> fake_workflow_type, :task_list=>"BadWorkflow"}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {:scheduled_event_id=>2, :identity=>"some_identity"}),
                                TestHistoryEvent.new("DecisionTaskTimedOut", 4, {:scheduled_event_id=>2, :timeout_type=>"START_TO_CLOSE", :started_event_id=>3})
                               ])

      end
    end
    worker.start
    # @forking_executor.execute { activity_worker.start }

    # debugger
    # worker.start
    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "CompleteWorkflowExecution"
  end

  it "reproduces the ActivityTaskTimedOut problem" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type =  FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {}),
                                TestHistoryEvent.new("DecisionTaskCompleted", 4, {}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 5, {:activity_id => "Activity1"}),
                                TestHistoryEvent.new("ActivityTaskStarted", 6, {}),
                                TestHistoryEvent.new("ActivityTaskTimedOut", 7, {:scheduled_event_id => 5, :timeout_type => "START_TO_CLOSE"}),
                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list
      end
      extend Decider
      version "1"
      entry_point :entry_point
      activity_client :activity do |options|
        options.prefix_name = "BadActivity"
        options.version = "1"
        options.default_task_heartbeat_timeout = "3600"
        options.default_task_list = "BadWorkflow"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "10"
      end
      def entry_point
        activity.run_activity1
        activity.run_activity2
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)


    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
      options.task_start_to_close_timeout = 10
      options.child_policy = :request_cancel
    end

    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution(5)
    worker.start

    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "FailWorkflowExecution"
    swf_client.trace.first[:decisions].first[:fail_workflow_execution_decision_attributes][:details].should =~
      /AWS::Flow::ActivityTaskTimedOutException/
  end

  it "makes sure that exponential retry can take arguments" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list
      end
      extend Decider
      version "1"
      entry_point :entry_point

      activity_client :activity do |options|
        options.prefix_name = "BadActivity"
        options.version = "1"
        options.default_task_heartbeat_timeout = "3600"
        options.default_task_list = "BadWorkflow"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "10"
      end
      def entry_point
        activity.exponential_retry(:run_activity1, 5) do |o|
          o.maximum_attempts = 3
        end
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
      options.task_start_to_close_timeout = 10
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start
    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "ScheduleActivityTask"
  end

  it "makes sure that overriding works correctly" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list
      end
      extend Decider
      version "1"
      entry_point :entry_point
      activity_client :activity do |options|
        options.prefix_name = "BadActivity"
        options.version = "1"
        options.default_task_heartbeat_timeout = "3600"
        options.default_task_list = "BadWorkflow"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "10"
      end
      def entry_point
        activity.exponential_retry(:run_activity1, 5) do |o|
          o.maximum_attempts = 3
        end
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
      options.task_start_to_close_timeout = 10
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start
    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "ScheduleActivityTask"
  end

  it "makes sure that exponential_retry blocks correctly" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list, :trace
      end
      @trace = []
      extend Decider
      version "1"
      entry_point :entry_point
      activity_client :activity do |options|
        options.prefix_name = "BadActivity"
        options.version = "1"
        options.default_task_heartbeat_timeout = "3600"
        options.default_task_list = "BadWorkflow"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "10"
      end
      def entry_point
        BadWorkflow.trace << :start
        activity.exponential_retry(:run_activity1, 5) do |o|
          o.maximum_attempts = 3
        end
        BadWorkflow.trace << :middle
        activity.exponential_retry(:run_activity2, 5) do |o|
          o.maximum_attempts = 3
        end
        activity.run_activity1
        BadWorkflow.trace << :end
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
      options.task_start_to_close_timeout = 10
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start
    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "ScheduleActivityTask"
    BadWorkflow.trace.should == [:start]
  end

  it "makes sure that exponential_retry blocks correctly when done through configure" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {}),
                                TestHistoryEvent.new("DecisionTaskCompleted", 4, {}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 5, {:activity_id => "Activity1"}),
                                TestHistoryEvent.new("ActivityTaskStarted", 6, {}),
                                TestHistoryEvent.new("ActivityTaskTimedOut", 7, {:scheduled_event_id => 5, :timeout_type => "START_TO_CLOSE"}),
                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list, :trace
      end
      @trace = []
      extend Decider
      version "1"
      entry_point :entry_point
      activity_client :activity do |options|
        options.prefix_name = "BadActivity"
        options.version = "1"
        options.default_task_heartbeat_timeout = "3600"
        options.default_task_list = "BadWorkflow"
        options.default_task_schedule_to_close_timeout = "90"
        options.default_task_schedule_to_start_timeout = "90"
        options.default_task_start_to_close_timeout = "90"
      end
      def entry_point
        BadWorkflow.trace << :start

        activity.reconfigure(:run_activity1) do |o|
          o.exponential_retry do |retry_options|
            retry_options.maximum_attempts = 3
          end
        end
        activity.run_activity1
        BadWorkflow.trace << :middle
        activity.run_activity1
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
      options.task_start_to_close_timeout = 30
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start
    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "StartTimer"
    BadWorkflow.trace.should == [:start]
  end

  it "makes sure that exponential_retry blocks correctly when done through the activity_client" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [

                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {:created_at => Time.now}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {:created_at => Time.now}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {:created_at => Time.now}),
                                TestHistoryEvent.new("DecisionTaskCompleted", 4, {:created_at => Time.now}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 5, {:activity_id => "Activity1", :created_at => Time.now}),
                                TestHistoryEvent.new("ActivityTaskStarted", 6, {:created_at => Time.now}),
                                TestHistoryEvent.new("ActivityTaskTimedOut", 7, {:scheduled_event_id => 5, :timeout_type => "START_TO_CLOSE", :created_at => Time.now}),
                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list, :trace
      end
      @trace = []
      extend Decider
      version "1"
      entry_point :entry_point
      activity_client :activity do |options|
        options.prefix_name = "BadActivity"
        options.version = "1"
        options.default_task_heartbeat_timeout = "3600"
        options.default_task_list = "BadWorkflow"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "30"
        options.exponential_retry do |retry_options|
          retry_options.maximum_attempts = 3
        end
      end
      def entry_point
        BadWorkflow.trace << :start
        activity.run_activity1
        BadWorkflow.trace << :middle
        activity.run_activity1

      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
      options.task_start_to_close_timeout = 30
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start

    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "StartTimer"
    BadWorkflow.trace.should == [:start]
  end
  it "makes sure that multiple schedules followed by a timeout work" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {}),
                                TestHistoryEvent.new("DecisionTaskCompleted", 4, {}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 5, {:activity_id => "Activity1"}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 6, {:activity_id => "Activity2"}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 7, {:activity_id => "Activity3"}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 8, {:activity_id => "Activity4"}),
                                TestHistoryEvent.new("ActivityTaskTimedOut", 9, {:scheduled_event_id => 5, :timeout_type => "START_TO_CLOSE"}),
                                TestHistoryEvent.new("ActivityTaskTimedOut", 10, {:scheduled_event_id => 6, :timeout_type => "START_TO_CLOSE"}),
                                TestHistoryEvent.new("ActivityTaskTimedOut", 11, {:scheduled_event_id => 7, :timeout_type => "START_TO_CLOSE"}),
                                TestHistoryEvent.new("ActivityTaskTimedOut", 12, {:scheduled_event_id => 8, :timeout_type => "START_TO_CLOSE"}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 13, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 14, {}),

                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list, :trace
      end
      @trace = []
      extend Decider
      version "1"
      entry_point :entry_point
      activity_client :activity do |options|
        options.prefix_name = "BadActivity"
        options.version = "1"
        options.default_task_heartbeat_timeout = "3600"
        options.default_task_list = "BadWorkflow"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "30"
        options.exponential_retry do |retry_options|
          retry_options.maximum_attempts = 3
        end
      end
      def entry_point
        BadWorkflow.trace << :start
        [:run_activity1, :run_activity2, :run_activity3, :run_activity4].each do |act|
          activity.send_async(act)
        end
        BadWorkflow.trace << :middle
        activity.run_activity3
        BadWorkflow.trace << :end
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
      options.task_start_to_close_timeout = 30
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start
    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "StartTimer"
    swf_client.trace.first[:decisions].length.should == 4
    BadWorkflow.trace.should == [:start, :middle]
  end

  it "makes sure that timeout followed by success is handled correctly" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {}),
                                TestHistoryEvent.new("DecisionTaskCompleted", 4, {}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 5, {:activity_id => "Activity1"}),
                                TestHistoryEvent.new("ActivityTaskTimedOut", 6, {:scheduled_event_id => 5, :timeout_type => "START_TO_CLOSE"}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 7, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 8, {}),
                                TestHistoryEvent.new("DecisionTaskCompleted", 10, {}),
                                TestHistoryEvent.new("TimerStarted", 11, {:decision_task_completed_event_id => 10, :timer_id => "Timer1", :start_to_fire_timeout => 1}),
                                TestHistoryEvent.new("TimerFired", 12, {:timer_id => "Timer1", :started_event_id => 11}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 13, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 14, {}),
                                TestHistoryEvent.new("DecisionTaskCompleted", 15, {}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 16, {:activity_id => "Activity2"}),
                                TestHistoryEvent.new("ActivityTaskCompleted", 17, {:scheduled_event_id => 16 }),
                                TestHistoryEvent.new("DecisionTaskScheduled", 18, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 19, {}),

                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list, :trace
      end
      @trace = []
      extend Decider
      version "1"
      entry_point :entry_point
      activity_client :activity do |options|
        options.prefix_name = "BadActivity"
        options.version = "1"
        options.default_task_heartbeat_timeout = "3600"
        options.default_task_list = "BadWorkflow"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "30"
        options.exponential_retry do |retry_options|
          retry_options.maximum_attempts = 3
        end
      end
      def entry_point
        BadWorkflow.trace << :start
        activity.run_activity1
        BadWorkflow.trace << :middle

        BadWorkflow.trace << :end
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
      options.task_start_to_close_timeout = 30
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start

    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "CompleteWorkflowExecution"
    BadWorkflow.trace.should == [:start, :middle, :end]
  end

  it "makes sure that signal works correctly" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {}),
                                TestHistoryEvent.new("DecisionTaskCompleted", 4, {}),
                                TestHistoryEvent.new("WorkflowExecutionSignaled", 5, {:signal_name => "this_signal"}),
                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list, :trace
      end
      @trace = []
      extend Decider
      version "1"
      entry_point :entry_point
      activity_client :activity do |options|
        options.prefix_name = "BadActivity"
        options.version = "1"
        options.default_task_heartbeat_timeout = "3600"
        options.default_task_list = "BadWorkflow"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "30"
        options.exponential_retry do |retry_options|
          retry_options.maximum_attempts = 3
        end
      end
      def this_signal
        @wait.broadcast
      end
      signal :this_signal
      def entry_point
        BadWorkflow.trace << :start
        @wait ||= FiberConditionVariable.new
        @wait.wait
        BadWorkflow.trace << :middle
        BadWorkflow.trace << :end
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    swf_client = FakeServiceClient.new
    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(BadWorkflow)
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start
    swf_client.trace.first[:decisions].first[:decision_type].should ==
      "CompleteWorkflowExecution"
    BadWorkflow.trace.should == [:start, :middle, :end]
  end

  it "makes sure that signal works correctly" do
    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type =  FakeWorkflowType.new(nil, "BadWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {}),
                               ])
      end
    end
    class BadWorkflow
      class << self
        attr_accessor :task_list, :trace
      end
      @trace = []
      extend Decider
      version "1"
      entry_point :entry_point
      def entry_point
        raise "This is an expected error"
      end
    end
    workflow_type_object = double("workflow_type", :name => "BadWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    swf_client = FakeServiceClient.new

    task_list = "BadWorkflow_tasklist"
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list, BadWorkflow)

    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "BadWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
    end
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start
    swf_client.trace.first[:decisions].first[:fail_workflow_execution_decision_attributes][:details].should =~ /This is an expected error/
  end
  it "makes sure that you can do retry with the easier Fixnum semantic"do

    class SynchronousWorkflowTaskPoller < WorkflowTaskPoller
      def get_decision_tasks
        fake_workflow_type = FakeWorkflowType.new(nil, "FixnumWorkflow.entry_point", "1")
        TestHistoryWrapper.new(fake_workflow_type,
                               [
                                TestHistoryEvent.new("WorkflowExecutionStarted", 1, {}),
                                TestHistoryEvent.new("DecisionTaskScheduled", 2, {}),
                                TestHistoryEvent.new("DecisionTaskStarted", 3, {}),
                                TestHistoryEvent.new("DecisionTaskCompleted", 4, {}),
                                TestHistoryEvent.new("ActivityTaskScheduled", 5, {:activity_id => "Activity1"}),
                                TestHistoryEvent.new("ActivityTaskStarted", 6, {}),
                                TestHistoryEvent.new("ActivityTaskTimedOut", 7, {:scheduled_event_id => 5, :timeout_type => "START_TO_CLOSE"}),
                               ])
      end
    end
    workflow_type_object = double("workflow_type", :name => "FixnumWorkflow.entry_point", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)

    class FixnumActivity
      extend Activity
      activity :run_activity1
      def run_activity1; raise StandardError; end
    end
    class FixnumWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1"} }
      activity_client(:activity) { {:version => "1", :prefix_name => "FixnumActivity" } }
      def entry_point

        activity.retry(:run_activity1, 5) {{:maximum_attempts => 5, :should_jitter => false}}
      end
    end
    swf_client = FakeServiceClient.new
    task_list = "FixnumWorkflow_tasklist"
    my_workflow_factory = workflow_factory(swf_client, domain) do |options|
      options.workflow_name = "FixnumWorkflow"
      options.execution_start_to_close_timeout = 3600
      options.task_list = task_list
    end
    worker = SynchronousWorkflowWorker.new(swf_client, domain, task_list)
    worker.add_workflow_implementation(FixnumWorkflow)
    my_workflow = my_workflow_factory.get_client
    workflow_execution = my_workflow.start_execution
    worker.start
    swf_client.trace.first[:decisions].first[:start_timer_decision_attributes][:start_to_fire_timeout].should == "5"
  end

end
describe "Misc tests" do
  it "makes sure that Workflows is equivalent to Decider" do
    class TestDecider
      extend Workflows
    end
    TestDecider.methods.map(&:to_sym).should include :signal
  end
end

describe FlowConstants do

  it "will test the default retry function with regular cases" do
    test_first = [Time.now, Time.now, Time.now]
    test_time_of_failure = [0, 10, 100]
    test_attempts = [{}, {Exception=>1}, {ActivityTaskTimedOutException=>5, Exception=>2}]
    test_output = [0, 1, 64]
    arr = test_first.zip(test_time_of_failure, test_attempts, test_output)
    arr.each do |first, time_of_failure, attempts, output|
      result = FlowConstants.exponential_retry_function.call(first, time_of_failure, attempts)
      (result == output).should == true
    end
  end

  it "will test for exceptions" do
    expect { FlowConstants.exponential_retry_function.call(-1, 1, {}) }.to raise_error(ArgumentError, "first is not an instance of Time")
    expect { FlowConstants.exponential_retry_function.call(Time.now, -1, {}) }.to raise_error(ArgumentError, "time_of_failure can't be negative")
    expect { FlowConstants.exponential_retry_function.call(Time.now, 1, {Exception=>-1}) }.to raise_error(ArgumentError, "number of attempts can't be negative")
    expect { FlowConstants.exponential_retry_function.call(Time.now, 1, {Exception=>-1, ActivityTaskTimedOutException=>-10}) }.to raise_error(ArgumentError, "number of attempts can't be negative")
    expect { FlowConstants.exponential_retry_function.call(Time.now, 1, {Exception=>2, ActivityTaskTimedOutException=>-10}) }.to raise_error(ArgumentError, "number of attempts can't be negative")
  end

end



describe "testing changing default values in RetryOptions and RetryPolicy" do

  it "will test exponential retry with a new retry function" do
    my_retry_func = lambda do |first, time_of_failure, attempts|
      10
    end
    options = {
      :should_jitter => false
    }
    retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
    result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>10}, Exception, 1)
    result.should == 10
  end

  it "will test the jitter function" do
    my_retry_func = lambda do |first, time_of_failure, attempts|
      10
    end
    options = {
      :should_jitter => true
    }
    retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options, true))
    result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>10}, Exception, 1)
    result.should >= 10 && result.should < 15
  end


  it "will test whether we get the same jitter for a particular execution id" do

   (FlowConstants.jitter_function.call(1, 100)).should equal(FlowConstants.jitter_function.call(1, 100))

  end

  it "will test the default exceptions included for retry" do
    options = RetryOptions.new
    options.exceptions_to_include.should include Exception
  end

  it "will test the default exceptions included for retry" do
    my_retry_func = lambda do |first, time_of_failure, attempts|
      10
    end
    options = {
      :exceptions_to_include => [ActivityTaskTimedOutException],
      :exceptions_to_exclude => [ActivityTaskFailedException]
    }
    retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
    result = retry_policy.isRetryable(ActivityTaskTimedOutException.new("a", "b", "c", "d"))
    result.should == true

    result = retry_policy.isRetryable(ActivityTaskFailedException.new("a", "b", "c", RuntimeError.new))
    result.should == false
  end


  it "will make sure exception is raised if the called exception is there in both included and excluded exceptions" do
    my_retry_func = lambda do |first, time_of_failure, attempts|
      10
    end
    options = {
      :exceptions_to_include => [ActivityTaskFailedException],
      :exceptions_to_exclude => [ActivityTaskFailedException]
    }
    retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
    expect {retry_policy.isRetryable(ActivityTaskFailedException.new("a", "b", "c", ActivityTaskFailedException))}.to raise_error
  end

  it "will test max_attempts" do
    my_retry_func = lambda do |first, time_of_failure, attempts|
      10
    end
    options = {
      :maximum_attempts => 5,
      :should_jitter => false
    }
    retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
    result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>10}, Exception.new, 1)
    result.should == -1

    result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>4}, Exception.new, 1)
    result.should == 10
  end

  it "will test retries_per_exception" do
    my_retry_func = lambda do |first, time_of_failure, attempts|
      10
    end
    options = {
      :retries_per_exception => {Exception => 5},
      :should_jitter => false
    }
    retry_policy = RetryPolicy.new(my_retry_func, RetryOptions.new(options))
    result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>10}, Exception.new, 1)
    result.should == -1

    result = retry_policy.next_retry_delay_seconds(Time.now, 0, {Exception=>4}, Exception.new, 1)
    result.should == 10
  end
end

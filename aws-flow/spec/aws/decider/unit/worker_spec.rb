require_relative 'setup'

class TestWorkflow
  extend Workflows
  workflow :start do
    {
      :default_execution_start_to_close_timeout => 30, :version => "1"
    }
  end
  def start; end
end

class TestActivity
  extend Activity

  activity :run_activity1 do |o|
    o.default_task_heartbeat_timeout = "3600"
    o.default_task_list = "activity_task_list"
    o.default_task_schedule_to_close_timeout = "3600"
    o.default_task_schedule_to_start_timeout = "3600"
    o.default_task_start_to_close_timeout = "3600"
    o.version = "1"
  end
  def run_activity1
    "first regular activity"
  end
  def run_activity2
    "second regular activity"
  end
end

class TestActivityWorker < ActivityWorker

  attr_accessor :executor
  def initialize(service, domain, task_list, forking_executor, *args, &block)
    super(service, domain, task_list, *args, &block)
    @executor = forking_executor
  end
end

class FakeTaskPoller < WorkflowTaskPoller
  def get_decision_task
    nil
  end
end
def dumb_fib(n)
  n < 1 ? 1 : dumb_fib(n - 1) + dumb_fib(n - 2)
end

describe GenericWorker do
  context "#resolve_default_task_list" do
    worker = GenericWorker.new(nil, nil, "worker_task_list")
    worker.resolve_default_task_list("USE_WORKER_TASK_LIST").should == "worker_task_list"
    worker.resolve_default_task_list("passed_in_task_list").should == "passed_in_task_list"
  end
end

describe WorkflowWorker do
  context "#register" do
    it "ensures that worker uses the right task list to register type" do
      class DefaultTasklistTestWorkflow
        extend Workflows
        workflow :workflow do
          {
            version: "1.0",
            default_task_list: "USE_WORKER_TASK_LIST"
          }
        end
        workflow :workflow2 do
          {
            version: "1.0",
            default_task_list: "my_own_task_list"
          }
        end
        workflow :workflow3 do
          {
            version: "1.0",
          }
        end
      end
      workflow_options = {
        default_task_start_to_close_timeout: "30",
        default_child_policy: "TERMINATE",
        default_task_priority: "0",
        domain: "UnitTestDomain",
        name: "DefaultTasklistTestWorkflow.workflow",
        version: "1.0",
        default_task_list: {
          name: "task_list"
        }
      }
      workflow2_options = {
        default_task_start_to_close_timeout: "30",
        default_child_policy: "TERMINATE",
        default_task_priority: "0",
        domain: "UnitTestDomain",
        name: "DefaultTasklistTestWorkflow.workflow2",
        version: "1.0",
        default_task_list: {
          name: "my_own_task_list"
        }
      }
      workflow3_options = {
        default_task_start_to_close_timeout: "30",
        default_task_priority: "0",
        default_child_policy: "TERMINATE",
        domain: "UnitTestDomain",
        name: "DefaultTasklistTestWorkflow.workflow3",
        version: "1.0",
        default_task_list: {
          name: "task_list"
        }
      }
      expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:register_workflow_type).with(workflow_options)
      expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:register_workflow_type).with(workflow2_options)
      expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:register_workflow_type).with(workflow3_options)

      worker = AWS::Flow::WorkflowWorker.new(
        AWS::SimpleWorkflow.new.client,
        AWS::SimpleWorkflow::Domain.new("UnitTestDomain"),
        "task_list",
        DefaultTasklistTestWorkflow
      )

      worker.register
    end
  end

  it "will test whether WorkflowWorker shuts down cleanly when an interrupt is received" do
    task_list = "TestWorkflow_tasklist"
    service = FakeServiceClient.new
    workflow_type_object = double("workflow_type", :name => "TestWorkflow.start", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)
    workflow_worker = WorkflowWorker.new(service, domain, task_list)
    workflow_worker.add_workflow_implementation(TestWorkflow)
    pid = fork do
      loop do
        workflow_worker.run_once(true, FakeTaskPoller.new(service, domain, nil, task_list, nil))
      end
    end
    # Send an interrupt to the child process
    Process.kill("INT", pid)
    # Adding a sleep to let things get setup correctly (not ideal but going with
    # this for now)
    sleep 5
    return_pid, status = Process.wait2(pid, Process::WNOHANG)
    Process.kill("KILL", pid) if return_pid.nil?
    return_pid.should_not be nil
    status.success?.should be_true
  end

  it "will test whether WorkflowWorker dies cleanly when two interrupts are received" do
    class FakeTaskPoller
      def poll_and_process_single_task
        dumb_fib(5000)
      end
    end
    task_list = "TestWorkflow_tasklist"
    service = FakeServiceClient.new
    workflow_type_object = double("workflow_type", :name => "TestWorkflow.start", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)
    workflow_worker = WorkflowWorker.new(service, domain, task_list)
    workflow_worker.add_workflow_implementation(TestWorkflow)
    pid = fork do
      loop do
        workflow_worker.run_once(true, FakeTaskPoller.new(service, domain, nil, task_list, nil))
      end
    end
    # Send an interrupt to the child process
    sleep 3
    2.times { Process.kill("INT", pid); sleep 2 }
    return_pid, status = Process.wait2(pid, Process::WNOHANG)

    Process.kill("KILL", pid) if return_pid.nil?
    return_pid.should_not be nil
    status.success?.should be_false
  end

end

describe ActivityWorker do

  context "#initialize" do

    context "windows platform" do

      before(:all) do
        @platform = RbConfig::CONFIG['host_os'] =~ /mswin|mingw/
        RbConfig::CONFIG['host_os'] = 'mswin'
      end

      after(:all) do
        RbConfig::CONFIG['host_os'] = @platform
      end

      it "turns forking off by default" do
        ActivityWorker.any_instance.stub(:add_implementation)
        worker = AWS::Flow::ActivityWorker.new(nil, nil, "task_list")
        options = worker.instance_variable_get("@options")
        options.use_forking.should be_false
        options.execution_workers.should be_zero
      end

      it "turns forking on if user explicitely specifies execution workers" do
        ActivityWorker.any_instance.stub(:add_implementation)
        worker = AWS::Flow::ActivityWorker.new(nil, nil, "task_list") { { execution_workers: 5 }}
        options = worker.instance_variable_get("@options")
        options.use_forking.should be_true
        options.execution_workers.should == 5
      end
    end

    # This test is to ensure that the default behavior of forking is overriden
    # by setting execution_workers to 0. This is added to ensure that forking
    # can be turned off from the json spec for the runner.
    it "turns forking on if execution_workers is 0 and use_forking is true" do
      worker = AWS::Flow::ActivityWorker.new(nil, nil, "task_list") { { execution_workers: 0, use_forking: true }}
      options = worker.instance_variable_get("@options")
      options.use_forking.should be_false
      options.execution_workers.should be_zero
    end

  end

  context "#register" do
    it "ensures that worker uses the right task list to register type" do
      class DefaultTasklistTestActivity
        extend Activities
        activity :activity do
          {
            version: "1.0",
            default_task_list: "USE_WORKER_TASK_LIST"
          }
        end
        activity :activity2 do
          {
            version: "1.0",
            default_task_list: "my_own_task_list"
          }
        end
        activity :activity3 do
          {
            version: "1.0"
          }
        end
      end
      activity_options = {
        domain: "UnitTestDomain",
        name: "DefaultTasklistTestActivity.activity",
        version: "1.0",
        default_task_heartbeat_timeout: "NONE",
        default_task_schedule_to_close_timeout: "NONE",
        default_task_schedule_to_start_timeout: "NONE",
        default_task_start_to_close_timeout: "NONE",
        default_task_priority: "0",
        default_task_list: {
          name: "task_list"
        }
      }

      activity2_options = {
        domain: "UnitTestDomain",
        name: "DefaultTasklistTestActivity.activity2",
        version: "1.0",
        default_task_heartbeat_timeout: "NONE",
        default_task_schedule_to_close_timeout: "NONE",
        default_task_schedule_to_start_timeout: "NONE",
        default_task_start_to_close_timeout: "NONE",
        default_task_priority: "0",
        default_task_list: {
          name: "my_own_task_list"
        }
      }

      activity3_options = {
        domain: "UnitTestDomain",
        name: "DefaultTasklistTestActivity.activity3",
        version: "1.0",
        default_task_heartbeat_timeout: "NONE",
        default_task_schedule_to_close_timeout: "NONE",
        default_task_schedule_to_start_timeout: "NONE",
        default_task_start_to_close_timeout: "NONE",
        default_task_priority: "0",
        default_task_list: {
          name: "task_list"
        }
      }

      expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:register_activity_type).with(activity_options)
      expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:register_activity_type).with(activity2_options)
      expect_any_instance_of(AWS::SimpleWorkflow::Client::V20120125).to receive(:register_activity_type).with(activity3_options)

      worker = AWS::Flow::ActivityWorker.new(
        AWS::SimpleWorkflow.new.client,
        AWS::SimpleWorkflow::Domain.new("UnitTestDomain"),
        "task_list",
        DefaultTasklistTestActivity
      )

      worker.register
    end
  end

  class FakeDomain
    def activity_tasks
      sleep 30
    end
  end
  it "will test whether the ActivityWorker shuts down cleanly when an interrupt is received" do

    task_list = "TestWorkflow_tasklist"
    service = FakeServiceClient.new
    workflow_type_object = double("workflow_type", :name => "TestWorkflow.start", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)
    forking_executor = ForkingExecutor.new
    activity_worker = TestActivityWorker.new(service, domain, task_list, forking_executor) { {:logger => FakeLogger.new} }
    activity_worker.add_activities_implementation(TestActivity)
    # Starts the activity worker in a forked process. Also, attaches an at_exit
    # handler to the process. When the process exits, the handler checks whether
    # the executor's internal is_shutdown variable is set correctly or not.
    pid = fork do
      at_exit {
        activity_worker.executor.is_shutdown.should == true
      }
      activity_worker.start true
    end
    # Send an interrupt to the child process
    Process.kill("INT", pid)
    # Adding a sleep to let things get setup correctly (not ideal but going with
    # this for now)
    sleep 5
    return_pid, status = Process.wait2(pid, Process::WNOHANG)
    Process.kill("KILL", pid) if return_pid.nil?
    return_pid.should_not be nil

    status.success?.should be_true
  end

  # This method will take a long time to run, allowing us to test our shutdown
  # scenarios


  xit "will test whether the ActivityWorker shuts down immediately if two or more interrupts are received" do
    task_list = "TestWorkflow_tasklist"
    service = FakeServiceClient.new
    workflow_type_object = double("workflow_type", :name => "TestWorkflow.start", :start_execution => "" )
    domain = FakeDomain.new(workflow_type_object)
    forking_executor = ForkingExecutor.new
    activity_worker = TestActivityWorker.new(service, domain, task_list, forking_executor) { {:logger => FakeLogger.new} }

    activity_worker.add_activities_implementation(TestActivity)
    # Starts the activity worker in a forked process. Also, executes a task
    # using the forking executor of the activity worker. The executor will
    # create a child process to run that task. The task (dumb_fib) is
    # purposefully designed to be long running so that we can test our shutdown
    # scenario.
    pid = fork do
      activity_worker.executor.execute {
        dumb_fib(1000)
      }
      activity_worker.start true
    end
    # Adding a sleep to let things get setup correctly (not idea but going with
    # this for now)
    sleep 3
    # Send 2 interrupts to the child process
    2.times { Process.kill("INT", pid); sleep 3 }
    status = Process.waitall
    status[0][1].success?.should be_false
  end

end

describe GenericType do

  let(:type) { GenericType.new("name", "version", {}) }
  let(:another_type) { GenericType.new("name", "version", {}) }

  context "#hash" do

    it "produces correct hash value" do
      type.hash.should == another_type.hash
    end

  end

  context "#eql?" do

    it "correctly checks equality" do
      type.should eql(another_type)
    end

  end

  it "ensures the type can be retrieved from a hash" do
    hash = { type => "foo_type" }
    hash[another_type].should == "foo_type"
  end

  [:ActivityType, :WorkflowType].each do |type|
    describe "#{type}" do
      let(:swf_klass) { AWS::SimpleWorkflow.const_get(type) }
      let(:flow_klass) { AWS::Flow::const_get(type) }

      describe "with same name and version" do
        let(:swf_type) { swf_klass.new("domain", "name", "version") }
        let(:flow_type) { flow_klass.new("name", "version") }

        context "#hash" do
          it "produces same hash value" do
            swf_type.hash.should == flow_type.hash
          end
        end

        context "#eql?" do
          it "ensures the two types are logically equivalent" do
            swf_type.should eql(flow_type)
          end
        end

        it "can be looked up in a hash" do
          hash = { flow_type => "flow_type" }
          hash[swf_type].should == "flow_type"
        end

      end

      describe "with different name and version" do
        let(:swf_type) { swf_klass.new("domain", "name", "version") }
        let(:flow_type) { flow_klass.new("diff_name", "diff_version") }

        context "#hash" do
          it "produces different hash value" do
            swf_type.hash.should_not == flow_type.hash
          end
        end

        context "#eql?" do
          it "ensures the two types are not logically equivalent" do
            swf_type.should_not eql(flow_type)
          end
        end

        it "cannot be looked up in a hash" do
          hash = { flow_type => "flow_type" }
          hash[swf_type].should be_nil
        end

      end
    end
  end

end


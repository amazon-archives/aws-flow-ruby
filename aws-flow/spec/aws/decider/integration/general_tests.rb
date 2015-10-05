require_relative 'setup'

describe "RubyFlowDecider" do
  include_context "setup integration tests"
  describe "General Testing" do
    it "makes sure that you can register a workflow with defaults" do
      general_test(:task_list => "workflow registration", :class_name => "WFRegister")
      @workflow_class.class_eval do
        workflow(:test_method) do
          {
            :version => 1,
            :default_task_list => "foo",
            :default_execution_start_to_close_timeout => 30,
            :default_child_policy => "request_cancel"
          }
        end
      end
      worker = WorkflowWorker.new(@swf.client, @domain, "test", @workflow_class)

      worker.register
      sleep 5
      @domain.workflow_types.to_a.find{|x| x.name == "#{@workflow_class}.test_method"}.should_not be_nil
    end

    it "tests that workflow clock gives the same value over multiple replays" do
      general_test(:task_list => "replaying_test", :class_name => "Replaying_clock")
      @workflow_class.class_eval do
        def entry_point

        end
      end
    end
    it "tests to make sure we set replaying correctly" do
      general_test(:task_list => "is_replaying", :class_name => "Replaying")
      @workflow_class.class_eval do
        def entry_point
          activity.run_activity1
          decision_context.workflow_clock.replaying
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      # TODO Kinda hacky, we should be using the workflow_class's data_converter
      workflow_execution.events.to_a.last.attributes[:result].include? "false"
    end

    it "makes sure that having a workflow with outstanding activities will close if one fails" do
      general_test(:task_list => "outstanding_activity_failure", :class_name => "OutstandingActivityFailure")
      @workflow_class.class_eval do
        def entry_point
          activity.send_async(:run_activity1)
          task do
            activity.run_activity2 {{:task_list => "foo"}}
          end
        end
      end
      @activity_class.class_eval do
        def run_activity1
          raise "simulated error"
        end
        def run_activity2

        end
      end

      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }

      workflow_execution = @my_workflow_client.start_execution

      wait_for_execution(workflow_execution)

      history = workflow_execution.events.map(&:event_type)
      history.last.should == "WorkflowExecutionFailed"

      history.should include "ActivityTaskCancelRequested"
      #@worker.run_once
      #@activity_worker.run_once
      #wait_for_decision(workflow_execution)
      #@worker.run_once
      #wait_for_decision(workflow_execution)
      #@worker.run_once

      #wait_for_execution(workflow_execution)
      #history = workflow_execution.events.map(&:event_type)
      #history.last.should == "WorkflowExecutionFailed"
      ## Should look something like: ["WorkflowExecutionStarted",
      # "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted",
      # "ActivityTaskScheduled", "ActivityTaskScheduled", "ActivityTaskStarted",
      # "ActivityTaskFailed", "DecisionTaskScheduled", "DecisionTaskStarted",
      # "DecisionTaskCompleted", "ActivityTaskCancelRequested",
      # "ActivityTaskCanceled", "DecisionTaskScheduled", "DecisionTaskStarted",
      # "DecisionTaskCompleted", "WorkflowExecutionFailed"]
      #history.should include "ActivityTaskCancelRequested"
    end

    it "makes sure that you can use the :exponential_retry key" do
      general_test(:task_list => "exponential_retry_key", :class_name => "ExponentialRetryKey")
      @workflow_class.class_eval do
        def entry_point
          activity.run_activity1  do
            {
              :exponential_retry => {:maximum_attempts => 1},
              :schedule_to_start_timeout => 1
            }
          end
        end
      end
      worker = WorkflowWorker.new(@domain.client, @domain, "exponential_retry_key", @workflow_class)
      workflow_execution = @my_workflow_client.start_execution
      4.times { worker.run_once }
      wait_for_execution(workflow_execution)
      workflow_execution.events.to_a.last.event_type.should == "WorkflowExecutionFailed"
    end

    it "ensures that you can use an arbitrary logger" do
      testing_file = "/tmp/testing"
      general_test(:task_list => "arbitrary logger", :class_name => "ArbitraryLogger")
      File.delete(testing_file) if File.exists? testing_file
      logger = Logger.new(testing_file)
      logger.level = Logger::DEBUG
      worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary logger", @workflow_class) { {:logger => logger} }
      activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary logger", @activity_class) { { :logger => logger, :execution_workers => 20, :use_forking => false} }
      workflow_execution = @my_workflow_client.start_execution
      worker.run_once
      file = File.open(testing_file)
      # The file should have something in it(i.e., not blank)
      file.read.should_not =~ /""/
      # Clear the file so we can be sure the activity worker works too
      File.open(testing_file, 'w') {}
      file = File.open(testing_file).read.should_not =~ /""/
      activity_worker.run_once
    end
    it "makes sure that raising an exception in the wf definition is fine" do
      general_test(:task_list => "exception in wf", :class_name => "WFException")
      @workflow_class.class_eval do
        def entry_point
          raise Exception
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
    end
    it "makes sure that the return value of an activity is directly useable" do
      general_test(:task_list => "return value activity", :class_name => "ActivityReturn")
      @activity_class.class_eval do
        def run_activity1
          return 5
        end
      end
      @workflow_class.class_eval do
        def entry_point
          x = activity.run_activity1
          x.should == 5
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
    end
    it "makes sure that there is an easy way to get workflow_id" do
      general_test(:task_list => "workflow_id method", :class_name => "WFID")
      @workflow_class.class_eval do
        def entry_point
          workflow_id
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
    end
    it "makes sure that arguments get passed correctly" do
      task_list = "argument_task_list"
      class TaskListProvider
        class << self; attr_accessor :task_list; end
      end
      TaskListProvider.task_list = task_list

      class ArgumentActivity < TaskListProvider
        extend AWS::Flow::Activities
        activity :run_activity1 do
          {
            version: "1.0",
            default_task_list: self.task_list,
            default_task_schedule_to_close_timeout: "120",
            default_task_schedule_to_start_timeout: "60",
            default_task_start_to_close_timeout: "60"
          }
        end

        def run_activity1(arg)
          arg.should == 5
          arg + 1
        end
      end
      class ArgumentWorkflow < TaskListProvider
        extend AWS::Flow::Workflows
        workflow :entry_point do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: self.task_list,
            default_task_start_to_close_timeout: 10,
            default_child_policy: :request_cancel,
          }
        end
        activity_client(:activity) { { from_class: "ArgumentActivity" } }
        def entry_point(arg)
          arg.should == 5
          activity.run_activity1(arg)
        end
      end

      worker = WorkflowWorker.new(@domain.client, @domain, task_list, ArgumentWorkflow)
      activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, ArgumentActivity)
      worker.register
      activity_worker.register
      client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "ArgumentWorkflow" } }

      workflow_execution = client.start_execution(5)
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { worker.start }
      @forking_executor.execute { activity_worker.start }

      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).should ==
        ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
      workflow_execution.events.to_a.last.attributes[:result].should =~ /6/
      @forking_executor.shutdown(1)
    end

    it "makes sure that a standard error works" do
      general_test(:task_list => "regular error raise", :class_name => "StandardError")
      @workflow_class.class_eval do
        def entry_point
          activity.run_activity1
        end
      end

      @activity_class.class_eval do
        def run_activity1
          raise "This is a simulated error"
        end
      end
      workflow_execution = @my_workflow_client.start_execution

      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)

      workflow_execution.events.map(&:event_type).count("WorkflowExecutionFailed").should ==  1
    end


    it "ensures that exceptions to include functions properly" do
      general_test(:task_list => "exceptions_to_include", :class_name => "ExceptionsToInclude")
      @workflow_class.class_eval do
        def entry_point
          activity.exponential_retry(:run_activity1) { {:exceptions_to_exclude => [SecurityError] } }
        end
      end
      @activity_class.class_eval do
        def run_activity1
          raise SecurityError
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
    end
    class YAMLPlusOne
      def dump(obj)
        obj.to_yaml + "1"
      end
      def load(source)
        source = source[0..-2]
        YAML.load source
      end
    end
    it "makes sure you can set a different converter for activities" do
      class DifferentActivityConverterActivity
        extend Activities
        activity :test_converter do
          {
            :data_converter => YAMLPlusOne.new,
            :default_task_list => "different converter activity",
            :version => "1",
            :default_task_heartbeat_timeout => "600",
            :default_task_schedule_to_close_timeout => "60",
            :default_task_schedule_to_start_timeout => "60",
            :default_task_start_to_close_timeout => "60",
          }
        end
        def test_converter
          "this"
        end
      end
      activity_worker = ActivityWorker.new(@swf.client, @domain,"different converter activity", DifferentActivityConverterActivity)
      class DifferentActivityConverterWorkflow
        extend Workflows
        workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "different converter activity"} }
        activity_client(:activity) { { :from_class => DifferentActivityConverterActivity } }
        def entry_point
          activity.test_converter
        end
      end
      worker = WorkflowWorker.new(@swf.client, @domain, "different converter activity", DifferentActivityConverterWorkflow)
      my_workflow_client = workflow_client(@swf.client, @domain) { { :from_class => DifferentActivityConverterWorkflow } }
      worker.register
      activity_worker.register
      workflow_execution = my_workflow_client.start_execution
      worker.run_once
      activity_worker.run_once
      worker.run_once
      activity_completed_index = workflow_execution.events.map(&:event_type).index("ActivityTaskCompleted")
      workflow_execution.events.to_a[activity_completed_index].attributes.result.should =~ /1\z/
    end

    it "makes sure that timers work" do
      general_test(:task_list => "Timer_task_list", :class_name => "Timer")
      @workflow_class.class_eval do
        def entry_point
          create_timer(5)
          activity.run_activity1
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).should ==
        ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "TimerStarted", "TimerFired", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
      @forking_executor.shutdown(1)
    end


    it "makes sure that timers can have a block passed in" do
      general_test(:task_list => "timer_with_block", :class_name => "TimerBlock")
      @workflow_class.class_eval do
        def entry_point
          create_timer(5) { activity.run_activity1 }
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }
      wait_for_execution(workflow_execution)
      @forking_executor.shutdown(1)
      workflow_execution.events.map(&:event_type).should ==
        ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "TimerStarted", "TimerFired", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
    end

    it "makes sure that you can have an asynchronous timer" do
      general_test(:task_list => "async_timer", :class_name => "Async")
      @workflow_class.class_eval do
        def entry_point
          create_timer_async(5)
          activity.run_activity1
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }

      wait_for_execution(workflow_execution)

      @forking_executor.shutdown(1)
      after_first_decision = workflow_execution.events.to_a.slice(4, 2).map(&:event_type)
      after_first_decision.should include "TimerStarted"
      after_first_decision.should include "ActivityTaskScheduled"
    end
    it "makes sure that you can have an asynchronous timer with a block" do
      general_test(:task_list => "async_timer_with_block", :class_name => "AsyncBlock")
      @workflow_class.class_eval do
        def entry_point
          create_timer_async(5) { activity.run_activity1 }
          activity.run_activity2
        end
      end
      @activity_worker = ActivityWorker.new(@swf.client, @domain, "async timer with block", AsyncBlockActivity)
      @activity_worker.register
      workflow_execution = @my_workflow_client.start_execution
      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { @worker.start }
      @forking_executor.execute { @activity_worker.start }
      wait_for_execution(workflow_execution)
      @forking_executor.shutdown(1)
      activity_scheduled = workflow_execution.events.to_a.each_with_index.map{|x, i| i if x.event_type == "ActivityTaskScheduled"}.compact
      history_events = workflow_execution.events.to_a
      history_events[activity_scheduled.first - 1].event_type == "TimerStarted" ||
        history_events[activity_scheduled.first + 1].event_type == "TimerStarted"
      history_events[activity_scheduled.first].attributes[:activity_type].name.should == "AsyncBlockActivity.run_activity2"
      history_events[activity_scheduled.last].attributes[:activity_type].name.should == "AsyncBlockActivity.run_activity1"
    end

    describe "Child Workflows" do

      it "is a basic child workflow test" do

        class ChildWorkflowsTestChildWorkflow
          extend AWS::Flow::Workflows
          workflow :child do
            {
              version: "1.0",
              default_execution_start_to_close_timeout: 600,
              default_task_start_to_close_timeout: 10,
            }
          end
          def child; sleep 1; end
        end

        class ChildWorkflowsTestParentWorkflow
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
            client = AWS::Flow::workflow_client(domain.client, domain) { { from_class: "ChildWorkflowsTestChildWorkflow", task_list: "test2" } }
            client.send_async(:start_execution)
            client.send_async(:start_execution)
          end
        end

        parent_client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "ChildWorkflowsTestParentWorkflow" } }
        @child_worker = WorkflowWorker.new(@domain.client, @domain, "test2", ChildWorkflowsTestChildWorkflow)
        @parent_worker = WorkflowWorker.new(@domain.client, @domain, "test", ChildWorkflowsTestParentWorkflow)

        @forking_executor = ForkingExecutor.new(:max_workers => 3)
        @forking_executor.execute { @parent_worker.start }
        @forking_executor.execute { @child_worker.start }
        @forking_executor.execute { @child_worker.start }
        sleep 2

        workflow_execution = parent_client.start_execution
        wait_for_execution(workflow_execution)

        events = workflow_execution.events.map(&:event_type)
        workflow_execution.events.to_a.last.attributes.result.should_not =~ /secret_access_key/
        events.should include("ChildWorkflowExecutionStarted", "ChildWorkflowExecutionCompleted", "WorkflowExecutionCompleted")
      end

      it "ensures that workflow clock provides at least basic support for current_time_millis" do
        general_test(:task_list => "workflow_clock_basic", :class_name => "WorkflowClockBasic")

        @workflow_class.class_eval do
          class << self
            attr_accessor :time_hash, :replaying_hash
          end
          def entry_point
            def record_point(name)
              self.class.replaying_hash[name] << decision_context.workflow_clock.replaying
              self.class.time_hash[name] << decision_context.workflow_clock.current_time
            end
            record_point(:first)
            create_timer(5)
            record_point(:second)
            create_timer(3)
            record_point(:third)
          end
        end
        @workflow_class.time_hash = Hash.new {|hash, key| hash[key] = []}
        @workflow_class.replaying_hash =  Hash.new {|hash, key| hash[key] = []}
        workflow_execution = @my_workflow_client.start_execution
        3.times { @worker.run_once }
        # Maintain the invariant that you should *not* be replaying only once
        @workflow_class.replaying_hash.values.each {|x| x.count(false).should be 1}
        # Maintain the invariant that at the same point in the code,
        # replay_current_time_millis will return the same value
        @workflow_class.time_hash.values.each do |array|
          array.reduce {|first, second| first if first.should == second}
        end
      end

      it "ensures that a child workflow failing raises a ChildWorkflowExecutionFailed" do
        class FailingChildChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "failing_child_workflow", :default_execution_start_to_close_timeout => 600} }
          def entry_point(arg)
            raise "simulated error"
          end
        end
        class FailingHostChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "failing_parent_workflow", :default_execution_start_to_close_timeout => 600} }
          def other_entry_point
          end

          def entry_point(arg)
            domain = get_test_domain
            client = workflow_client(domain.client, domain) { {:from_class => "FailingChildChildWorkflow"} }
            begin
              client.start_execution(5)
            rescue Exception => e
              #pass
            end
          end
        end
        worker2 = WorkflowWorker.new(@swf.client, @domain, "failing_child_workflow", FailingChildChildWorkflow)
        worker2.register
        worker = WorkflowWorker.new(@swf.client, @domain, "failing_parent_workflow", FailingHostChildWorkflow)
        worker.register
        client = workflow_client(@swf.client, @domain) { {:from_class => "FailingHostChildWorkflow"} }
        workflow_execution = client.entry_point(5)
        worker.run_once
        worker2.run_once
        worker2.run_once
        worker.run_once
        events = workflow_execution.events.map(&:event_type)
        events.should include "ChildWorkflowExecutionFailed"
        events.should include "WorkflowExecutionCompleted"
      end

      it "ensures that a child workflow can use data_converter correctly" do
        class DataConverterChildChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "data_converter_child_workflow", :default_execution_start_to_close_timeout => 600, :data_converter => YAMLPlusOne.new} }
          def entry_point(arg)
            return arg + 1
          end
        end
        class DataConverterHostChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "data_converter_parent_workflow", :default_execution_start_to_close_timeout => 600} }
          def other_entry_point
          end

          def entry_point(arg)
            domain = get_test_domain
            client = workflow_client(domain.client, domain) { {:from_class => "DataConverterChildChildWorkflow"} }
            task { client.start_execution(5) }
          end
        end
        worker2 = WorkflowWorker.new(@swf.client, @domain, "data_converter_child_workflow", DataConverterChildChildWorkflow)
        worker2.register
        worker = WorkflowWorker.new(@swf.client, @domain, "data_converter_parent_workflow", DataConverterHostChildWorkflow)
        worker.register

        client = workflow_client(@swf.client, @domain) { {:from_class => "DataConverterHostChildWorkflow"} }
        workflow_execution = client.entry_point(5)
        worker.run_once
        worker2.run_once
        worker.run_once
        # We have to find the index dynamically, because due to how scheduled/starts work, it isn't necessarily in the same place in our history.
        child_execution_completed_index = workflow_execution.events.map(&:event_type).index("ChildWorkflowExecutionCompleted")

        workflow_execution.events.to_a[child_execution_completed_index].attributes.result.should =~ /1\z/
      end

      it "makes sure that the new way of doing child workflows works" do
        class OtherNewChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "new_child_workflow", :default_execution_start_to_close_timeout => 600} }
          def entry_point(arg)
            sleep 2
          end

        end
        class BadNewChildWorkflow
          extend Workflows
          workflow(:entry_point) { {:version =>  1, :task_list => "new_parent_workflow", :default_execution_start_to_close_timeout => 600} }
          def other_entry_point
          end

          def entry_point(arg)
            domain = get_test_domain
            client = workflow_client(domain.client, domain) { {:from_class => "OtherNewChildWorkflow"} }
            task { client.start_execution(5) }
            task { client.start_execution(5) }
          end
        end
        worker2 = WorkflowWorker.new(@swf.client, @domain, "new_child_workflow", OtherNewChildWorkflow)
        worker2.register
        worker = WorkflowWorker.new(@swf.client, @domain, "new_parent_workflow", BadNewChildWorkflow)
        worker.register
        client = workflow_client(@swf.client, @domain) { {:from_class => "BadNewChildWorkflow"} }
        workflow_execution = client.entry_point(5)
        worker.run_once
        worker2.run_once
        worker2.run_once
        worker.run_once
        worker.run_once if workflow_execution.events.map(&:event_type).last == "DecisionTaskCompleted"
        events = workflow_execution.events.map(&:event_type)
        events.should include "ChildWorkflowExecutionStarted"
        events.should include "ChildWorkflowExecutionCompleted"
        events.should include "WorkflowExecutionCompleted"
      end
    end
    it "makes sure that you can use retries_per_exception" do
      general_test(:task_list => "retries_per_exception", :class_name => "RetriesPerException")
      @activity_class.class_eval do
        def run_activity1
          raise StandardError
        end
      end
      @workflow_class.class_eval do
        activity_client :activity do |options|
          options.default_task_heartbeat_timeout = "600"
          options.default_task_list = self.task_list
          options.default_task_schedule_to_close_timeout = "5"
          options.default_task_schedule_to_start_timeout = "5"
          options.default_task_start_to_close_timeout = "5"
          options.version = "1"
          options.prefix_name = self.activity_class.to_s
        end
        def entry_point
          activity.exponential_retry(:run_activity1) do |o|
            o.retries_per_exception = {
              ActivityTaskTimedOutException => Float::INFINITY,
              ActivityTaskFailedException => 3
            }
          end
        end
      end

      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      @worker.run_once
      @activity_worker.run_once

      @worker.run_once
      @worker.run_once
      @activity_worker.run_once

      @worker.run_once

      wait_for_execution(workflow_execution)
      workflow_history = workflow_execution.events.map(&:event_type)
      workflow_history.count("ActivityTaskFailed").should == 3

      workflow_history.count("WorkflowExecutionFailed").should == 1
    end

    it "makes sure that continueAsNew within a timer works" do
      general_test(:task_list => "continue_as_new_timer", :class_name => "ContinueAsNewTimer")
      @workflow_class.class_eval do
        def entry_point
          create_timer(5) do
            continue_as_new do |options|
              options.execution_start_to_close_timeout = 600
              options.task_list = "continue_as_new_timer"
              options.tag_list = []
              options.version = "1"
            end
          end
        end
      end
      @workflow_execution = @my_workflow_client.entry_point
      @worker.run_once
      @worker.run_once
      @workflow_execution.events.map(&:event_type).last.should ==
        "WorkflowExecutionContinuedAsNew"
      @workflow_execution.status.should ==
        :continued_as_new
    end

    it "ensures that you can write a continue_as_new with less configuration" do
      general_test(:task_list => "continue_as_new_config", :class_name => "ContinueAsNewConfiguration")
      @workflow_class.class_eval do
        def entry_point
          continue_as_new
        end
      end
      @workflow_execution = @my_workflow_client.entry_point
      @worker.run_once
      @workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionContinuedAsNew"
    end

    it "makes sure that basic continueAsNew works" do
      general_test(:task_list => "continue_as_new", :class_name => "ContinueAsNew")
      @workflow_class.class_eval do
        def entry_point
          continue_as_new do |options|
            options.workflow_name = @workflow_class.to_s
            options.execution_method = :entry_point
            options.execution_start_to_close_timeout = 600
            options.task_list = "continue_as_new"
            options.tag_list = []
            options.task_start_to_close_timeout = 30
            options.child_policy = "REQUEST_CANCEL"
            options.version = "1"
          end
        end
      end

      @workflow_execution = @my_workflow_client.entry_point
      @worker.run_once
      @workflow_execution.events.map(&:event_type).last.should ==
        "WorkflowExecutionContinuedAsNew"
      @workflow_execution.status.should ==
        :continued_as_new
    end

    it "makes sure that exponential retry returns values correctly" do
      class ExponentialActivity
        extend AWS::Flow::Activity
        activity :run_activity1 do
          {
            version: "1.0",
            default_task_list: "exponential_test_return_task_list",
            default_task_schedule_to_close_timeout: "30",
            default_task_schedule_to_start_timeout: "15",
            default_task_start_to_close_timeout: "15",
          }
        end
        def run_activity1
          return 5
        end
      end

      class ExponentialWorkflow
        extend AWS::Flow::Workflows
        workflow :start do
          {
            version: "1.0",
            default_task_list: "exponential_test_return_task_list",
            default_execution_start_to_close_timeout: 600,
            default_task_start_to_close_timeout: 60,
            default_child_policy: "REQUEST_CANCEL"
          }
        end
        activity_client(:activity) { { from_class: "ExponentialActivity" } }
        def start
          x = activity.exponential_retry(:run_activity1) {
            {
              retries_per_exception: {
                ActivityTaskTimedOutException => Float::INFINITY,
                ActivityTaskFailedException => 3
              }
            }
          }
          x.should == 5
        end
      end

      task_list = "exponential_test_return_task_list"

      worker = WorkflowWorker.new(@domain.client, @domain, task_list, ExponentialWorkflow)
      activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, ExponentialActivity)
      worker.register
      activity_worker.register
      client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "ExponentialWorkflow" } }
      workflow_execution = client.start_execution
      worker.run_once
      activity_worker.run_once
      activity_worker.run_once unless workflow_execution.events.map(&:event_type).include? "ActivityTaskCompleted"
      worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
    end

    it "makes sure that signals work correctly" do

      class SignalWorkflow
        extend AWS::Flow::Workflows
        workflow :entry_point do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
          }
        end

        signal :this_signal
        def this_signal(input)
          @wait.broadcast
          @input = input
        end

        def entry_point
          @input = "bad_input"
          @wait ||= FiberConditionVariable.new
          @wait.wait
          @input.should =~ /new input!/
        end

      end

      worker = build_worker(SignalWorkflow, "SignalWorkflow_tasklist")
      worker.register
      client = build_client(from_class: "SignalWorkflow")

      workflow_execution = client.start_execution

      worker.run_once
      client.signal_workflow_execution("this_signal", workflow_execution) { {:input => "new input!"}}
      worker.run_once

      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
    end

    it "makes sure that internal signalling works" do

      class SignallingActivity
        extend AWS::Flow::Activities
        activity :run_activity1 do
          {
            version: "1.0",
            default_task_list: "SignalWorker_activity_tasklist",
            default_task_schedule_to_close_timeout: "10",
            default_task_schedule_to_start_timeout: "10",
            default_task_start_to_close_timeout: "8",
          }
        end
        def run_activity1
          return 5
        end
      end

      class SignalInternalWorkflow
        extend AWS::Flow::Workflows

        workflow :entry_point do
          {
            version: "1.0",
            default_task_list: "SignalWorkflow_tasklist",
            default_execution_start_to_close_timeout: 600,
            default_child_policy: :request_cancel,
          }
        end

        activity_client(:activity) { { from_class: "SignallingActivity" } }

        def entry_point
          client = build_client(from_class: "SignaleeWorkflow")
          workflow_future = client.send_async(:start_execution)
          activity.run_activity1
          client.signal_workflow_execution(:this_signal, workflow_future)
        end
      end

      class SignaleeWorkflow
        extend AWS::Flow::Workflows

        workflow :entry_point do
          {
            version: "1.0",
            default_task_list: "WorkflowSignalee_tasklist",
            default_execution_start_to_close_timeout: 600,
            default_child_policy: :request_cancel,
          }
        end
        signal :this_signal

        def entry_point
          @wait ||= FiberConditionVariable.new
          @wait.wait
        end
        def this_signal
          @wait.broadcast
        end
      end

      worker_signalee = build_worker(SignaleeWorkflow, "WorkflowSignalee_tasklist")
      worker_signaler = build_worker(SignalInternalWorkflow, "SignalWorkflow_tasklist")
      activity_worker = build_worker(SignallingActivity, "SignalWorker_activity_tasklist")
      worker_signaler.register
      worker_signalee.register
      activity_worker.register

      client = build_client(from_class: "SignalInternalWorkflow")
      workflow_execution = client.start_execution

      worker_signaler.run_once
      worker_signalee.run_once
      activity_worker.run_once
      wait_for_decision(workflow_execution, "ActivityTaskCompleted")

      worker_signaler.run_once
      wait_for_decision(workflow_execution)

      worker_signalee.run_once
      wait_for_decision(workflow_execution, "ChildWorkflowExecutionCompleted")

      worker_signaler.run_once
      wait_for_execution(workflow_execution)

      workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1

    end
  end
end

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
require 'aws-sdk-v1'
require 'logger'
require_relative 'setup'

describe "RubyFlowDecider" do
  include_context "setup integration tests"

  it "runs an empty workflow, making sure base configuration stuff is correct" do
    target_workflow = @domain.workflow_types.page(:per_page => 1000).select { |x| x.name == "blank_workflow_test"}
    if target_workflow.length == 0
      workflow_type = @domain.workflow_types.create("blank_workflow_test", '1',
                                                    :default_task_list => "initial_test_tasklist",
                                                    :default_child_policy => :request_cancel,
                                                    :default_task_start_to_close_timeout => 3600,
                                                    :default_execution_start_to_close_timeout => 24 * 3600)
    else
      workflow_type = target_workflow.first
    end
    workflow_execution = workflow_type.start_execution :input => "yay"
    workflow_execution.terminate
  end

  describe WorkflowFactory do
    it "makes sure that you can use the basic workflow_factory" do
      task_list = "workflow_factory_task_list"
      class WorkflowFactoryActivity
        extend AWS::Flow::Activities
        activity :run_activity1 do
          {
            version: "1.0",
            default_task_list: "workflow_factory_task_list",
            default_task_schedule_to_close_timeout: 60,
            default_task_schedule_to_start_timeout: 30,
            default_task_start_to_close_timeout: 30,
          }
        end
        def run_activity1(arg)
          "#{arg} is what the activity recieved"
        end
      end

      class WorkflowFactoryWorkflow
        extend AWS::Flow::Workflows
        workflow :entry_point do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: "workflow_factory_task_list",
            default_task_start_to_close_timeout: 120,
            default_child_policy: :request_cancel,
          }
        end
        activity_client(:activity) { { from_class: "WorkflowFactoryActivity" } }
        def entry_point(arg)
          activity.run_activity1("#{arg} recieved as input")
        end
      end

      worker = WorkflowWorker.new(@domain.client, @domain, task_list, WorkflowFactoryWorkflow)
      activity_worker = ActivityWorker.new(@domain.client, @domain, task_list, WorkflowFactoryActivity)
      worker.register
      activity_worker.register

      client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "WorkflowFactoryWorkflow" } }

      workflow_execution = client.start_execution("some input")

      @forking_executor = ForkingExecutor.new(:max_workers => 3)
      @forking_executor.execute { worker.start }
      @forking_executor.execute { activity_worker.start }

      wait_for_execution(workflow_execution)

      workflow_execution.events.map(&:event_type).should == ["WorkflowExecutionStarted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "ActivityTaskScheduled", "ActivityTaskStarted", "ActivityTaskCompleted", "DecisionTaskScheduled", "DecisionTaskStarted", "DecisionTaskCompleted", "WorkflowExecutionCompleted"]
      @forking_executor.shutdown(1)
    end
  end

  it "ensures that activities can be processed with different configurations" do
    class TwoConfigActivity
      extend Activities
      activity :run_activity1 do
        {
          :default_task_heartbeat_timeout => "600",
          :default_task_list => "TwoConfigTaskList",
          :default_task_schedule_to_start_timeout => 120,
          :default_task_start_to_close_timeout => 120,
          :version => "1",
        }
      end
      def run_activity1
      end
    end

    class TwoConfigWorkflow
      extend Workflows
      activity_client(:activity) { { :from_class => TwoConfigActivity }}
      workflow :entry_point do
        {
          :version => 1,
          :default_execution_start_to_close_timeout => 30,
          :default_child_policy => "request_cancel",
          :default_task_list => "TwoConfigTaskList"
        }
      end
      def entry_point
        activity.run_activity1
        activity.run_activity1 { {:task_list => "other_config_task_list"} }
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "TwoConfigTaskList", TwoConfigWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, "TwoConfigTaskList", TwoConfigActivity) {{ :use_forking => false }}
    activity_worker_different_config = ActivityWorker.new(@swf.client, @domain, "other_config_task_list", TwoConfigActivity) {{ :use_forking => false }}
    my_workflow_client = workflow_client(@swf.client, @domain) {{:from_class => TwoConfigWorkflow}}

    worker.register
    activity_worker.register
    workflow_execution = my_workflow_client.start_execution
    worker.run_once
    activity_worker.run_once
    worker.run_once
    activity_worker_different_config.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last == "WorkflowExecutionCompleted"
  end

  it "ensures that not filling in details/reason for activity_task_failed is handled correctly" do
    general_test(:task_list => "ActivityTaskFailedManually", :class_name => "ActivityTaskFailedManually")
    $task_token = nil

    @activity_class.class_eval do
      activity :run_activityManual do
        {
          :default_task_heartbeat_timeout => "600",
          :default_task_list => task_list,
          :default_task_schedule_to_start_timeout => 120,
          :default_task_start_to_close_timeout => 120,
          :version => "1",
          :manual_completion => true
        }
      end
      def run_activityManual
        $task_token = activity_execution_context.task_token
      end
    end

    @workflow_class.class_eval do
      def entry_point
        begin
          activity.run_activityManual
        rescue Exception => e
          #pass
        end
      end
    end

    activity_worker = ActivityWorker.new(@swf.client, @domain, "ActivityTaskFailedManually", @activity_class) {{ :use_forking => false }}
    activity_worker.register

    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    activity_worker.run_once

    @swf.client.respond_activity_task_failed(:task_token => $task_token)

    @worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "ensures that raising inside a with_retry propagates up correctly" do
    general_test(:task_list => "WithRetryPropagation", :class_name => "WithRetryPropagation")
    @workflow_class.class_eval do
      def entry_point
        error = nil
        begin
          with_retry(:maximum_attempts => 1) { activity.run_activity1 }
        rescue ActivityTaskFailedException => e
          error = e
        end
        return error
      end
    end
    @activity_class.class_eval do
      def run_activity1; raise "Error!"; end
    end

    @forking_executor = ForkingExecutor.new(:max_workers => 3)
    @forking_executor.execute { @worker.start }
    @forking_executor.execute { @activity_worker.start }
    sleep 5

    workflow_execution = @my_workflow_client.start_execution

    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
    workflow_execution.events.to_a[-1].attributes.result.should =~ /Error!/

  end

  it "ensures that backtraces are set correctly with yaml" do
    general_test(:task_list => "Backtrace_test", :class_name => "BacktraceTest")
    @workflow_class.class_eval do
      def entry_point
        begin
          activity.run_activity1
        rescue ActivityTaskFailedException => e
          error = e
          e.backtrace.nil?.should == false
        end
        return error.backtrace
      end
    end
    @activity_class.class_eval do
      def run_activity1
        raise "Error!"
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once
    @worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.to_a[-1].attributes.result.should =~ /Error!/
  end

  it "makes sure that an error fails an activity" do
    task_list = "exponential_retry_test"
    GeneralActivity.task_list = task_list
    MyWorkflow.task_list = task_list
    class GeneralActivity
      class << self
        attr_accessor :task_list
      end
      extend Activity
      activity :run_activity1 do |options|
        options.default_task_list = GeneralActivity.task_list
        options.default_task_schedule_to_start_timeout = "600"
        options.default_task_start_to_close_timeout = "600"
        options.version = "1"
      end
      def run_activity1
        raise "error"
      end
    end
    class MyWorkflow
      class << self
        attr_accessor :task_list
      end
      extend AWS::Flow::Workflows
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "GeneralActivity"
        options.version = "1"
        options.default_task_list = MyWorkflow.task_list
        options.default_task_schedule_to_start_timeout = "60"
        options.default_task_start_to_close_timeout = "60"
      end
      entry_point :entry_point
      def entry_point(arg)
        activity.run_activity1
      end
    end

    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(MyWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(GeneralActivity)
    worker.register
    activity_worker.register
    my_workflow_factory = workflow_factory(@swf.client, @domain) do |options|
      options.workflow_name = "MyWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_list = task_list
      options.task_start_to_close_timeout = 120
      options.child_policy = :request_cancel
    end
    my_workflow = my_workflow_factory.get_client


    @forking_executor = ForkingExecutor.new(:max_workers => 3)
    @forking_executor.execute { worker.start }
    @forking_executor.execute { activity_worker.start }
    workflow_execution = my_workflow.start_execution(5)
    wait_for_execution(workflow_execution)
    @forking_executor.shutdown(1)
    workflow_execution.events.map(&:event_type).count("ActivityTaskFailed").should == 1
  end

  it "is a good example of the service" do
    # Definition of the activity
    class AddOneActivity
      extend Activity
      activity :run_activity1 do |options|
        options.default_task_list = "add_one_task_list"
        options.version = "1"
        options.default_task_heartbeat_timeout = "600"
        options.default_task_schedule_to_close_timeout = "30"
        options.default_task_schedule_to_start_timeout = "30"
        options.default_task_start_to_close_timeout = "30"
      end
      def run_activity1(arg)
        arg.should == 5
        arg + 1
      end
    end
    # Definition of the workflow logic
    class MyWorkflow
      extend AWS::Flow::Workflows
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "AddOneActivity"
        # If we had the activity somewhere we couldn't reach it, we would have
        # to have the lines below, but since the have access to the activity, we
        # can simply "peek" at its configuration, and use those

        # options.default_task_heartbeat_timeout = "600"
        # options.default_task_list = "add_one_task_list"
        # options.default_task_schedule_to_close_timeout = "600"
        # options.default_task_schedule_to_start_timeout = "600"
        # options.default_task_start_to_close_timeout = "600"
      end

      # The default place to start the execution of a workflow is "entry_point",
      # but you can specify any entry point you want with the entry_point method
      entry_point :start_my_workflow
      def start_my_workflow(arg)
        # Should is a Rspec assert statement. E.g. "assert that the variable arg
        # is equal to 5"
        arg.should == 5
        # This makes sure that if there is an error, such a time out, then the
        # activity will be rescheduled
        activity.exponential_retry(:run_activity1, arg) do |o|
          o.maximum_attempts = 3
        end
      end
    end

    # Set up the workflow/activity worker
    task_list = "add_one_task_list"
    # @swf and @domain are set beforehand with the aws ruby sdk
    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(MyWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(AddOneActivity)
    worker.register
    activity_worker.register

    # Get a workflow client to start the workflow
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "MyWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_list = task_list
      options.task_start_to_close_timeout = 120
      options.child_policy = :request_cancel
    end
    # Forking executors have some possibility of race conditions, so we will
    # avoid them by putting in a small sleep. There is no plan to fix at current, as
    # we don't expect forking executor to be used by most customers.
    my_workflow_client = my_workflow_factory.get_client
    sleep 5
    workflow_execution = my_workflow_client.start_execution(5)
    # We use an executor here so as to be able to test this feature within one
    # working process, as activity_worker.start and worker.start will block
    # otherwise
    forking_executor = ForkingExecutor.new(:max_workers => 2)
    forking_executor.execute { activity_worker.start }
    forking_executor.execute { worker.start }

    # Sleep to give the threads some time to compute, as we'll run right out of
    # the test before they can run otherwise
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
  end

  it "is an example of joining a parallel split" do
    # Definition of the activity
    class ParallelSplitActivity
      extend Activity
      activity :run_activity1, :run_activity2, :run_activity3 do |options|
        options.default_task_list = "parallel_split_task_list"
        options.version = "1"
        options.default_task_heartbeat_timeout = "600"
        options.default_task_schedule_to_close_timeout = "120"
        options.default_task_schedule_to_start_timeout = "120"
        options.default_task_start_to_close_timeout = "120"
      end
      def run_activity1(arg)
        arg + 1
      end
      def run_activity2(arg)
        arg + 2
      end
      def run_activity3(arg)
        arg + 3
      end
    end
    # Definition of the workflow logic
    class ParallelWorkflow
      extend AWS::Flow::Workflows
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "ParallelSplitActivity"
        # If we had the activity somewhere we couldn't reach it, we would have
        # to have the lines below, but since the have access to the activity, we
        # can simply "peek" at its configuration, and use those

        # options.default_task_heartbeat_timeout = "600"
        # options.default_task_list = "parallel_split_task_list"
        # options.default_task_schedule_to_close_timeout = "120"
        # options.default_task_schedule_to_start_timeout = "120"
        # options.default_task_start_to_close_timeout = "120"
      end

      # The default place to start the execution of a workflow is "entry_point",
      # but you can specify any entry point you want with the entry_point method
      entry_point :start_my_workflow
      def start_my_workflow(arg)
        future_array = []
        [:run_activity1, :run_activity2, :run_activity3].each do |this_activity|
          # send_async will not block here, but will instead return a
          # future. So, at the end of this each loop, future_array will contain
          # 3 promises corresponding to the values that will eventually be
          # returned from calling the activities
          future_array << activity.send_async(this_activity, arg)

          # wait_for_all will block until all the promises in the enumerable
          # collection that it is given are ready. There is also wait_for_any,
          # which will return when any of the promises are ready. In this way,
          # you can join on a parallel split.
        end
        wait_for_all(future_array)
      end
    end

    # Set up the workflow/activity worker
    task_list = "parallel_split_task_list"
    # @swf and @domain are set beforehand with the aws ruby sdk
    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(ParallelWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list) { {:use_forking => false}}
    activity_worker.add_activities_implementation(ParallelSplitActivity)
    worker.register
    activity_worker.register

    # Get a workflow client to start the workflow
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ParallelWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_list = task_list
      options.task_start_to_close_timeout = 10
      options.child_policy = :request_cancel
    end

    my_workflow_client = my_workflow_factory.get_client
    workflow_execution = my_workflow_client.start_execution(5)

    forking_executor = ForkingExecutor.new(:max_workers => 2)
    forking_executor.execute { activity_worker.start }
    forking_executor.execute { worker.start }

    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
  end

  it "is an example of error handling in rubyflow" do
    class ErrorHandlingActivity
      extend Activity
      activity :run_activity1, :run_activity2 do |options|
        options.default_task_list = "error_handling_task_list"
        options.version = "1"
        options.default_task_heartbeat_timeout = "600"
        options.default_task_schedule_to_close_timeout = "10"
        options.default_task_schedule_to_start_timeout = "10"
        options.default_task_start_to_close_timeout = "10"
      end
      def run_activity1(arg)
        raise StandardError, "run_activity1 failed"
      end
      def run_activity2(arg)
        raise StandardError, "run_activity2 failed"
      end
    end
    # Definition of the workflow logic
    class MyWorkflow
      extend AWS::Flow::Workflows
      version "1"
      activity_client :activity do |options|
        options.prefix_name = "ErrorHandlingActivity"
        # If we had the activity somewhere we couldn't reach it, we would have
        # to have the lines below, but since the have access to the activity, we
        # can simply "peek" at its configuration, and use those

        # options.default_task_heartbeat_timeout = "600"
        # options.default_task_list = "error_handling_task_list"
        # options.default_task_schedule_to_close_timeout = "120"
        # options.default_task_schedule_to_start_timeout = "120"
        # options.default_task_start_to_close_timeout = "120"
      end

      # The default place to start the execution of a workflow is "entry_point",
      # but you can specify any entry point you want with the entry_point method
      entry_point :start_my_workflow
      def start_my_workflow(arg)
        # activity.run_activity1(arg) will "block", and so we can use the normal
        # ruby error handler semantics, and if there is a failure, it will
        # propagate here
        error_seen = nil
        begin
          activity.run_activity1(arg)
        rescue Exception => e
          error_seen = e.class
          # Do something with the error
        ensure
          # Should is a Rspec assert statement. E.g. "assert that the variable error_seen
          # is equal to StandardError
          error_seen.should == ActivityTaskFailedException
          # Do something to clean up after
        end
        # Since send_async won't "block" here, but will schedule a task for
        # processing later and evaluate any expressions after the send_async, we
        # should use the asynchronous error handler so as to make sure that
        # exceptions raised by the send_async will be caught hereerror_seen = nil
        error_handler do |t|
          t.begin { activity.send_async(:run_activity2, arg) }
          t.rescue(Exception) do |error|
            error_seen = error.class
            # Do something with the error
          end
          t.ensure do
            error_seen.should == ActivityTaskFailedException
            # Do something to clean up after
          end
          end
        5
      end
    end

    # Set up the workflow/activity worker
    task_list = "error_handling_task_list"
    # @swf and @domain are set beforehand with the aws ruby sdk
    worker = WorkflowWorker.new(@swf.client, @domain, task_list)
    worker.add_workflow_implementation(MyWorkflow)
    activity_worker = ActivityWorker.new(@swf.client, @domain, task_list)
    activity_worker.add_activities_implementation(ErrorHandlingActivity)
    worker.register
    activity_worker.register

    # Get a workflow client to start the workflow
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "MyWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_list = task_list
      options.task_start_to_close_timeout = 20
      options.child_policy = :request_cancel
    end

    my_workflow_client = my_workflow_factory.get_client
    workflow_execution = my_workflow_client.start_execution(5)

    # We use an executor here so as to be able to test this feature within one
    # working process, as activity_worker.start and worker.start will block
    # otherwise
    # forking_executor = ForkingExecutor.new

    # forking_executor.execute { activity_worker.start }
    # class WorkflowWorker
    #   def start
    #     poller = WorkflowTaskPoller.new(@service, @domain, DecisionTaskHandler.new(@workflow_definition_map), @ptask_list)
    #     loop do
    #       poller.poll_and_process_single_task
    #     end
    #   end
    # end

    worker.run_once
    activity_worker.run_once
    worker.run_once
    activity_worker.run_once
    worker.run_once
    # worker.start

    wait_for_execution(workflow_execution)

    workflow_execution.events.map(&:event_type).count("WorkflowExecutionCompleted").should == 1
  end

  it "ensures that you can use an internal workflow_client without domain/client" do
    general_test(:task_list => "internal_without_domain", :class_name => "InternalWithoutDomain")
    @workflow_class.class_eval do
      def entry_point
        my_workflow_client = workflow_client
        my_workflow_client.class.should == WorkflowClient
      end
    end

    workflow_execution = @my_workflow_client.entry_point
    @worker.run_once
  end

  it "ensures you cannot schedule more than 99 things in one decision" do
    general_test(:task_list => "schedule_more_than_100", :class_name => "Above100TasksScheduled")
    @activity_class.class_eval do
      def run_activity1(arg)
        arg
      end
    end
    @workflow_class.class_eval do
      def entry_point
        101.times do |i|
          activity.send_async(:run_activity1, i)
        end
      end
    end
    workflow_execution = @my_workflow_client.entry_point
    @worker.run_once
    workflow_execution.events.map(&:event_type).count("ActivityTaskScheduled").should be 99
    @worker.run_once
    workflow_execution.events.map(&:event_type).count("ActivityTaskScheduled").should be 101
  end


  describe "ensures that you can specify the {workflow_id,execution_method} to be used for an external client" do
    {:workflow_id => ["blah", "workflow_id"] ,
     :execution_method => ["start", "workflow_type.name.split('.').last" ]
    }.each_pair do |method, value_and_method_to_check|
      value, method_to_check = value_and_method_to_check

      it "makes sure that #{method} can be specified correctly" do
        class WorkflowIDWorkflow
          extend Workflows
          workflow :start do
            {
              version: "1.0",
              default_execution_start_to_close_timeout: 600,
              default_task_list: "timeout_test"
            }
          end
          def start; end
        end

        worker = WorkflowWorker.new(@domain.client, @domain, "timeout_test", WorkflowIDWorkflow)
        worker.register
        client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "WorkflowIDWorkflow" } }

        workflow_execution = client.start do {
          method.to_sym => value,
          tag_list: ["stuff"]
        }
        end

        return_value = eval "workflow_execution.#{method_to_check}"
        return_value.should == value
        workflow_execution.tags.should == ["stuff"]
      end
    end
  end
  describe "making sure that timeouts are infrequent" do
    it "is a basic repro case" do
      class TimeoutActivity
        extend AWS::Flow::Activities
        activity :run_activity1 do
          {
            default_task_list: "timeout_test_activity",
            version: "1.0",
            default_task_schedule_to_close_timeout: "60",
            default_task_schedule_to_start_timeout: "30",
            default_task_start_to_close_timeout: "30",
          }
        end
        def run_activity1
          "did some work in run_activity1"
        end
      end
      class TimeoutWorkflow
        extend AWS::Flow::Workflows
        workflow :entry_point do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: "timeout_test_workflow"
          }
        end
        activity_client (:activity) { { from_class: "TimeoutActivity" } }
        def entry_point
          activity.run_activity1
        end
      end
      @worker = WorkflowWorker.new(@domain.client, @domain, "timeout_test_workflow", TimeoutWorkflow)
      @activity_workers = []

      num_tests = 15
      1.upto(num_tests) { @activity_workers << ActivityWorker.new(@domain.client, @domain, "timeout_test_activity", TimeoutActivity) }

      @worker.register
      @activity_workers.first.register

      my_workflow_client = AWS::Flow::workflow_client(@domain.client, @domain) { { from_class: "TimeoutWorkflow" } }

      workflow_executions = []

      @forking_executor  = ForkingExecutor.new(max_workers: 20)
      @forking_executor.execute { @worker.start }
      @activity_workers.each { |x| @forking_executor.execute { x.start } }
      sleep 10

      1.upto(num_tests)  { |i| workflow_executions << my_workflow_client.entry_point }

      workflow_executions.each { |x| wait_for_execution(x) }
      workflow_executions.each{|x| x.events.to_a.last.event_type.should == "WorkflowExecutionCompleted" }
      @forking_executor.shutdown(1)

    end
  end

  it "makes sure that you can create a workflow in the new way" do
    class WorkflowWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "test"} }
      def entry_point; ;end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "test", WorkflowWorkflow)
    worker.register
    client = workflow_client(@swf.client, @domain) { {:from_class => "WorkflowWorkflow"} }
    workflow_execution = client.start_execution
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end
  it "makes sure that you can use with_opts with workflow_client" do
    class WorkflowWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "test"} }
      def entry_point; ;end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "Foobarbaz", WorkflowWorkflow)
    worker.register
    client = workflow_client(@swf.client, @domain) { {:from_class => "WorkflowWorkflow"} }
    workflow_execution = client.with_opts(:task_list => "Foobarbaz").start_execution
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "makes sure you can use with_opts with activity_client" do
    class ActivityActivity
      extend Activity
      activity(:run_activity1) do
        {
          :version => 1,
          :default_task_list => "options_test",
          :default_task_heartbeat_timeout => "600",
          :default_task_schedule_to_close_timeout => "60",
          :default_task_schedule_to_start_timeout => "60",
          :default_task_start_to_close_timeout => "60",
        }
      end
    end
    class WorkflowWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "test"} }

      def entry_point; ;end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "Foobarbaz", WorkflowWorkflow)
    worker.register
    client = workflow_client(@swf.client, @domain) { {:from_class => "WorkflowWorkflow"} }
    workflow_execution = client.with_opts(:task_list => "Foobarbaz").start_execution
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "makes sure that workflow errors out on schedule_activity_task_failed" do
    class BadActivityActivity
      extend Activity
      activity(:run_activity1) do
        {
          :version => 1
        }
      end
    end
    class WorkflowWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1", :default_execution_start_to_close_timeout => 600, :task_list => "test"} }
      activity_client(:client) { {:version => "1", :from_class => "BadActivityActivity"} }
      def entry_point; client.run_activity1; end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "Foobarbaz", WorkflowWorkflow)
    worker.register
    client = workflow_client(@swf.client, @domain) { {:from_class => "WorkflowWorkflow"} }
    workflow_execution = client.with_opts(:task_list => "Foobarbaz").start_execution
    worker.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
  end

  it "makes sure that you can have arbitrary activity names with from_class" do
    general_test(:task_list => "arbitrary_with_from_class", :class_name => "ArbitraryWithFromClass")
    @activity_class.class_eval do
      activity :test do
        {
          :default_task_heartbeat_timeout => "600",
          :default_task_list => task_list,
          :default_task_schedule_to_close_timeout => "20",
          :default_task_schedule_to_start_timeout => "20",
          :default_task_start_to_close_timeout => "20",
          :version => "1",
          :prefix_name => "ArbitraryName",
        }
      end
      def test; end
    end
    $activity_class = @activity_class
    workflow_execution = @my_workflow_client.start_execution
    @activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_with_from_class", @activity_class)
    @activity_worker.register
    @workflow_class.class_eval do
      activity_client(:test) { {:from_class => $activity_class} }
      def entry_point
        test.test
      end
    end
    @worker.run_once
    @activity_worker.run_once
    @worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "makes sure that you can have arbitrary activity names" do
    class ArbitraryActivity
      extend Activity
      def foo
      end
      activity :foo do
        {
          :default_task_list => "arbitrary_test",
          :version => "1",
          :default_task_heartbeat_timeout => "600",
          :default_task_schedule_to_close_timeout => "60",
          :default_task_schedule_to_start_timeout => "60",
          :default_task_start_to_close_timeout => "60",
          :prefix_name => "Foo"
        }
      end
    end
    class ArbitraryWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1" }}
      activity_client(:client) { {:version => "1", :prefix_name => "Foo"} }
      def entry_point
        client.foo
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryWorkflow)
    worker.register
    activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryActivity)
    activity_worker.register
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ArbitraryWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_start_to_close_timeout = 10
      options.child_policy = :REQUEST_CANCEL
      options.task_list = "arbitrary_test"
    end
    workflow_execution = my_workflow_factory.get_client.start_execution
    worker.run_once
    activity_worker.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end
  it "makes sure that exponential_retry's max_attempts works correctly" do
    general_test(:task_list => "exponential_retry_test_max_attempts", :class_name => "ExponentialRetryMaxAttempts")
    @activity_class.class_eval do
      def run_activity1
        raise "error"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        activity.exponential_retry(:run_activity1) do |o|
          o.maximum_attempts = 2
        end
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once

    # first failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    #second failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    # Finally, fail
    @worker.run_once

    wait_for_execution(workflow_execution)
    events = workflow_execution.events.map(&:event_type)
    events.count("WorkflowExecutionFailed").should == 1
    (events.count("ActivityTaskFailed") + events.count("ActivityTaskTimedOut")).should >= 3
  end

  it "makes sure that exponential_retry's max_attempts works correctly from a configured client" do
    general_test(:task_list => "exponential_retry_test_with_configure", :class_name => "ExponentialRetryMaxAttemptsConfigure")
    @activity_class.class_eval do
      def run_activity1
        raise "error"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        activity.reconfigure(:run_activity1) {  {:exponential_retry => {:maximum_attempts => 2}} }

        activity.run_activity1
      end
    end
    workflow_execution = @my_workflow_client.start_execution

    @worker.run_once
    @activity_worker.run_once

    # first failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    #second failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    # Finally, fail, catch, and succeed

    @worker.run_once

    wait_for_execution(workflow_execution)
    events = workflow_execution.events.map(&:event_type)
    events.count("WorkflowExecutionFailed").should == 1
    (events.count("ActivityTaskFailed") + events.count("ActivityTaskTimedOut")).should >= 3
  end

  it "makes sure that exponential_retry allows you to capture the error with configure" do
    general_test(:task_list => "exponential_retry_test_capture_with_configure", :class_name => "ExponentialRetryMaxAttemptsCaptureConfigure")
    @activity_class.class_eval do
      def run_activity1
        raise "error"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        activity.reconfigure(:run_activity1) {  {:exponential_retry => {:maximum_attempts => 2}} }
        begin
          activity.run_activity1
        rescue Exception => e
          # just making sure I can rescue
        end
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once

    # first failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    #second failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    # Finally, fail, catch, and succeed
    @worker.run_once

    wait_for_execution(workflow_execution)
    events = workflow_execution.events.map(&:event_type)
    events.count("WorkflowExecutionCompleted").should == 1
    (events.count("ActivityTaskFailed") + events.count("ActivityTaskTimedOut")).should >= 3
  end

  it "ensures that you can change options at the call site" do
    general_test(:task_list => "basic_options", :class_name => "BasicOptions")
    @workflow_class.class_eval do
      def entry_point
        activity.run_activity1 { {:start_to_close_timeout => 120 } }
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once
    @worker.run_once
    wait_for_execution(workflow_execution)
    # The default registered is 20, we want to make sure we overrode it
    workflow_execution.events.to_a[4].attributes[:start_to_close_timeout].should == 120
  end



  it "ensures that heartbeats work" do
    general_test(:task_list => "basic_heartbeat", :class_name => "BasicHeartbeat")

    @activity_class.class_eval do
      def run_activity1
        6.times do
          sleep 5
          record_activity_heartbeat("test!")
        end
      end
    end
    @workflow_class.class_eval do
      def entry_point
        activity.run_activity1 { {:heartbeat_timeout => 10, :start_to_close_timeout => 120, :schedule_to_close_timeout => 120} }
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    @activity_worker.run_once
    @worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "ensures that you can use heartbeats to request cancel" do
    general_test(:task_list => "heartbeat_request_cancel", :class_name => "HeartbeatRequestCancel")
    @activity_class.class_eval do
      def run_activity1
        6.times do
          sleep 5
          record_activity_heartbeat("test!")
        end
        raise "If we got here, the test failed, as we should have cancelled the activity"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        error_handler do |t|
          t.begin do
            future = activity.run_activity1 { {:heartbeat_timeout => 10, :start_to_close_timeout => 120, :schedule_to_close_timeout => 120, :return_on_start => true} }
            create_timer(5)
            activity.request_cancel_activity_task(future)
          end
          t.rescue(CancellationException) { |e| }
        end
      end
    end

    workflow_execution = @my_workflow_client.start_execution
    forking_executor = ForkingExecutor.new(:max_workers => 1)
    @worker.run_once
    forking_executor.execute { @activity_worker.start }
    sleep 10
    @worker.run_once
    sleep 10
    @worker.run_once
    wait_for_execution(workflow_execution)
    forking_executor.shutdown(1)
    # If we didn't cancel, the activity would fail
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "ensures you can use manual completion" do
    general_test(:task_list => "manual_completion", :class_name => "ManualCompletion")

    activity_worker = ActivityWorker.new(@swf.client, @domain, "manual_completion", @activity_class)
    activity_worker.register
    @workflow_class.class_eval do
      activity_client(:activity1) { {
        from_class: self.activity_class,
        manual_completion: true
      } }
      def entry_point
        activity.run_activity2
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    @worker.run_once
    activity_worker.run_once
    workflow_execution.events.map(&:event_type).should include("ActivityTaskStarted")
  end

  it "makes sure that exponential_retry allows you to capture the error" do
    general_test(:task_list => "exponential_retry_test_capture", :class_name => "ExponentialRetryMaxAttemptsCapture")
    @activity_class.class_eval do
      def run_activity1
        raise "error"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        begin
          activity.exponential_retry(:run_activity1) do |o|
            o.maximum_attempts = 2
          end
        rescue Exception => e
          # Just making sure I can rescue
        end
      end
    end
    workflow_execution = @my_workflow_client.start_execution

    @worker.run_once
    @activity_worker.run_once

    # first failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once

    #second failure
    @worker.run_once
    @worker.run_once
    @activity_worker.run_once


    # Finally, fail
    @worker.run_once

    wait_for_execution(workflow_execution)
    events = workflow_execution.events.map(&:event_type)
    events.count("WorkflowExecutionCompleted").should == 1
    (events.count("ActivityTaskFailed") + events.count("ActivityTaskTimedOut")).should >= 3
  end

  it "makes sure that you can use extend Activities" do
    class ActivitiesActivity
      extend Activities
      def foo
      end
      activity :foo do
        {
          :default_task_list => "arbitrary_test",
          :version => "1",
          :default_task_heartbeat_timeout => "600",
          :default_task_schedule_to_close_timeout => "60",
          :default_task_schedule_to_start_timeout => "60",
          :default_task_start_to_close_timeout => "60",
          :prefix_name => "Foo"
        }
      end
    end
    class ActivitiesWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1" }}
      activity_client(:client) { {:version => "1", :prefix_name => "Foo"} }
      def entry_point
        client.foo
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary_test", ActivitiesWorkflow)
    worker.register
    activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", ActivitiesActivity)
    activity_worker.register
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ActivitiesWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_start_to_close_timeout = 10
      options.child_policy = :REQUEST_CANCEL
      options.task_list = "arbitrary_test"
    end
    workflow_execution = my_workflow_factory.get_client.start_execution
    worker.run_once
    activity_worker.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end

  it "makes sure that you can't have a '.' in prefix name" do
    class ArbitraryWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1" }}
      activity_client(:client) { {:version => "1", :prefix_name => "Foo.this"} }
      def entry_point
        client.foo
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryWorkflow)
    worker.register
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ArbitraryWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_start_to_close_timeout = 10
      options.child_policy = :REQUEST_CANCEL
      options.task_list = "arbitrary_test"
    end
    workflow_execution = my_workflow_factory.get_client.start_execution
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
  end

  it "ensures that reregistering with different values without changing the version will alert you" do
    class RegisterActivity
      extend Activity
      activity :foo do |opt|
        opt.version = "1"
        opt.default_task_start_to_close_timeout = "60"
      end
    end
    activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", RegisterActivity)
    activity_worker.register
    class RegisterBadActivity
      extend Activity
      activity :foo do |opt|
        opt.version = "1"
        opt.default_task_start_to_close_timeout = 30
        opt.prefix_name = "RegisterActivity"
      end
    end
    activity_worker2 = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", RegisterBadActivity)
    expect { activity_worker2.register }.to raise_error RuntimeError
  end

  it "makes sure that you can have arbitrary activity names with the old style options" do
    class ArbitraryActivity
      extend Activity
      def foo
      end
      activity :foo do |opt|
        opt.default_task_list = "arbitrary_test"
        opt.version = "1"
        opt.default_task_heartbeat_timeout = "600"
        opt.default_task_schedule_to_close_timeout = "60"
        opt.default_task_schedule_to_start_timeout = "60"
        opt.default_task_start_to_close_timeout = "60"
        opt.prefix_name = "Foo"
      end
    end
    class ArbitraryWorkflow
      extend Workflows
      workflow(:entry_point) { {:version => "1" }}
      activity_client(:client) { {:version => "1", :prefix_name => "Foo"} }
      def entry_point
        client.foo
      end
    end
    worker = WorkflowWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryWorkflow)
    worker.register
    activity_worker = ActivityWorker.new(@swf.client, @domain, "arbitrary_test", ArbitraryActivity)
    activity_worker.register
    my_workflow_factory = workflow_factory @swf.client, @domain do |options|
      options.workflow_name = "ArbitraryWorkflow"
      options.execution_start_to_close_timeout = 600
      options.task_start_to_close_timeout = 10
      options.child_policy = :REQUEST_CANCEL
      options.task_list = "arbitrary_test"
    end
    workflow_execution = my_workflow_factory.get_client.start_execution
    worker.run_once
    activity_worker.run_once
    worker.run_once
    wait_for_execution(workflow_execution)
    workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"
  end
  it "makes sure that exponential_retry fails if we retry and still get failures", focus: true do
    general_test(:task_list => "exponential_retry_fails_correctly", :class_name => "ExponentialRetryFailsCorrectly")
    @activity_class.class_eval do
      def run_activity1
        raise "This is an error!"
      end
    end
    @workflow_class.class_eval do
      def entry_point
        activity.reconfigure(:run_activity1) {  {:exponential_retry => {:maximum_attempts => 2}} }
        futures = []
        futures << activity.send_async(:run_activity1)
        wait_for_all(futures)
      end
    end
    workflow_execution = @my_workflow_client.start_execution
    forking_executor = ForkingExecutor.new(:max_workers => 2)
    forking_executor.execute { @worker.start }
    forking_executor.execute { @activity_worker.start }

    wait_for_execution(workflow_execution)
    events = workflow_execution.events.map(&:event_type)
    events.count("WorkflowExecutionFailed").should == 1
  end
end

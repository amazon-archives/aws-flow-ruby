require_relative 'setup'
describe "Handle_ tests" do
  include_context "setup integration tests"
    # This also effectively tests "RequestCancelExternalWorkflowExecutionInitiated"

    # TODO: These three tests will sometimes fail, seemingly at random. We need to fix this.
    it "ensures that handle_child_workflow_execution_canceled is correct" do
      class OtherCancellationChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_child_cancelled_workflow", :default_execution_start_to_close_timeout => 600} }
        def entry_point(arg)
          create_timer(20)
        end
      end
      class BadCancellationChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_parent_cancelled_workflow", :default_execution_start_to_close_timeout => 600} }

        def entry_point(arg)
          domain = get_test_domain
          client = workflow_client(domain.client, domain) { {:from_class => "OtherCancellationChildWorkflow"} }
          workflow_future = client.send_async(:start_execution, 5)
          client.request_cancel_workflow_execution(workflow_future)
        end
      end
      child_worker = WorkflowWorker.new(@swf.client, @domain, "new_child_cancelled_workflow", OtherCancellationChildWorkflow)
      child_worker.register
      parent_worker = WorkflowWorker.new(@swf.client, @domain, "new_parent_cancelled_workflow", BadCancellationChildWorkflow)
      parent_worker.register
      client = workflow_client(@swf.client, @domain) { {:from_class => "BadCancellationChildWorkflow"} }
      workflow_execution = client.entry_point(5)

      parent_worker.run_once
      child_worker.run_once
      parent_worker.run_once

      wait_for_decision(workflow_execution)
      workflow_execution.events.map(&:event_type).should include "ExternalWorkflowExecutionCancelRequested"
      child_worker.run_once

      wait_for_decision(workflow_execution, "ChildWorkflowExecutionCanceled")
      workflow_execution.events.map(&:event_type).should include "ChildWorkflowExecutionCanceled"
      parent_worker.run_once

      wait_for_execution(workflow_execution)
      workflow_execution.events.to_a.last.attributes.details.should =~ /AWS::Flow::Core::Cancellation/
    end

    it "ensures that handle_child_workflow_terminated is handled correctly" do
      class OtherTerminationChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_child_terminated_workflow", :default_execution_start_to_close_timeout => 600} }

        def entry_point(arg)
          create_timer(5)
        end

      end
      $workflow_id = nil
      class BadTerminationChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_parent_terminated_workflow", :default_execution_start_to_close_timeout => 600} }
        def other_entry_point
        end

        def entry_point(arg)
          domain = get_test_domain
          client = workflow_client(domain.client, domain) { {:from_class => "OtherTerminationChildWorkflow"} }
          workflow_future = client.send_async(:start_execution, 5)
          $workflow_id = workflow_future.workflow_execution.workflow_id.get
        end
      end
      worker2 = WorkflowWorker.new(@swf.client, @domain, "new_child_terminated_workflow", OtherTerminationChildWorkflow)
      worker2.register
      worker = WorkflowWorker.new(@swf.client, @domain, "new_parent_terminated_workflow", BadTerminationChildWorkflow)
      worker.register
      client = workflow_client(@swf.client, @domain) { {:from_class => "BadTerminationChildWorkflow"} }
      workflow_execution = client.entry_point(5)

      worker.run_once
      worker2.run_once
      wait_for_decision(workflow_execution)
      @swf.client.terminate_workflow_execution({:workflow_id => $workflow_id, :domain => @domain.name})
      wait_for_decision(workflow_execution, "ChildWorkflowExecutionTerminated")
      worker.run_once
      wait_for_execution(workflow_execution)
      validate_execution_failed(workflow_execution)
      workflow_execution.events.to_a.last.attributes.details.should =~ /AWS::Flow::ChildWorkflowTerminatedException/
    end

    it "ensures that handle_child_workflow_timed_out is handled correctly" do
      class OtherTimedOutChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_child_timed_out_workflow", :default_execution_start_to_close_timeout => 5} }

        def entry_point(arg)
          create_timer(5)
        end

      end
      $workflow_id = nil
      class BadTimedOutChildWorkflow
        extend Workflows
        workflow(:entry_point) { {:version =>  1, :task_list => "new_parent_timed_out_workflow", :default_execution_start_to_close_timeout => 600} }
        def other_entry_point
        end

        def entry_point(arg)
          domain = get_test_domain
          client = workflow_client(domain.client, domain) { {:from_class => "OtherTimedOutChildWorkflow"} }
          workflow_future = client.send_async(:start_execution, 5)
          $workflow_id = workflow_future.workflow_execution.workflow_id.get
        end
      end
      worker2 = WorkflowWorker.new(@swf.client, @domain, "new_child_timed_out_workflow", OtherTimedOutChildWorkflow)
      worker2.register
      worker = WorkflowWorker.new(@swf.client, @domain, "new_parent_timed_out_workflow", BadTimedOutChildWorkflow)
      worker.register
      client = workflow_client(@swf.client, @domain) { {:from_class => "BadTimedOutChildWorkflow"} }
      workflow_execution = client.entry_point(5)
      worker.run_once
      sleep 8
      worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.to_a.last.attributes.details.should =~ /AWS::Flow::ChildWorkflowTimedOutException/
    end

    it "ensures that handle_start_child_workflow_execution_failed is fine" do
      general_test(:task_list => "handle_start_child_workflow_execution_failed", :class_name => "HandleStartChildWorkflowExecutionFailed")
      class FooBar
        extend Workflows
        workflow :bad_workflow do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 600,
            default_task_list: "handle_start_child_workflow_execution_failed_child"
          }
        end
        def bad_workflow
          raise "Child workflow died"
        end
      end
      @workflow_class.class_eval do
        def entry_point
          domain = get_test_domain
          wf = AWS::Flow.workflow_client(domain.client, domain) { { from_class: "FooBar" } }
          wf.start_execution
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      child_worker = WorkflowWorker.new(@domain.client, @domain, "handle_start_child_workflow_execution_failed_child", FooBar)
      child_worker.register
      @worker.run_once
      child_worker.run_once
      @worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionFailed"
      # Make sure this is actually caused by a child workflow failed
      workflow_execution.events.to_a.last.attributes.details.should =~ /ChildWorkflowFailed/
    end

    it "ensures that handle_timer_canceled is fine" do
      general_test(:task_list => "handle_timer_canceled", :class_name => "HandleTimerCanceled")
      @workflow_class.class_eval do
        def entry_point
          bre = error_handler do |t|
            t.begin do
              create_timer(100)
            end
            t.rescue(CancellationException) {}
          end
          create_timer(1)
          bre.cancel(CancellationException.new)
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_history = workflow_execution.events.map(&:event_type)
      workflow_history.count("TimerCanceled").should == 1
      workflow_history.count("WorkflowExecutionCompleted").should == 1
    end

    it "ensures that activities under a bre get cancelled" do
      general_test(:task_list => "activite under bre", :class_name => "ActivitiesUnderBRE")
      @workflow_class.class_eval do
        def entry_point
          bre = error_handler do |t|
            t.begin { activity.send_async(:run_activity1) }
          end
          create_timer(1)
          bre.cancel(CancellationException.new)
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @worker.run_once
      workflow_execution.events.map(&:event_type).count("ActivityTaskCancelRequested").should == 1
      @worker.run_once
      wait_for_execution(workflow_execution)
      workflow_execution.events.to_a.last.attributes.reason.should == "AWS::Flow::Core::CancellationException"
    end

    it "ensures that start_timer_failed is handled correctly" do
      general_test(:task_list => "start_timer_failed", :class_name => "StartTimerFailed")
    end

    it "ensures that get_state_method works fine" do
      general_test(:task_list => "get_state_method", :class_name => "GetStateTest")
      @workflow_class.class_eval do
        get_state_method :get_state_test
        def get_state_test
          "This is the workflow state!"
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      worker = WorkflowWorker.new(@swf.client, @domain, "get_state_method", @workflow_class)
      worker.run_once
      workflow_execution.events.to_a[3].attributes.execution_context.should =~ /This is the workflow state!/
    end

    it "ensures that handle_request_cancel_activity_task_failed works" do
      general_test(:task_list => "handle_request_cancel_activity_task_failed", :class_name => "HandleRCActivityTaskFailed")
      class AsyncDecider
        alias_method :old_handle_request_cancel_activity_task_failed, :handle_request_cancel_activity_task_failed
        # We have to replace this method, otherwise we'd fail on handling the
        # error because we can't find the decision in the decision_map. There
        # is similar behavior in javaflow
        def handle_request_cancel_activity_task_failed(event)
          event_double = SimpleTestHistoryEvent.new("Activity1")
          self.send(:old_handle_request_cancel_activity_task_failed, event_double)
        end
      end

      class ActivityDecisionStateMachine
        alias_method :old_create_request_cancel_activity_task_decision, :create_request_cancel_activity_task_decision
        def create_request_cancel_activity_task_decision
          { :decision_type => "RequestCancelActivityTask",
            :request_cancel_activity_task_decision_attributes => {:activity_id => "bad_id"} }
        end
      end

      @workflow_class.class_eval do
        def entry_point
          future = activity.send_async(:run_activity1)
          create_timer(1)
          activity.request_cancel_activity_task(future)
        end
      end


      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @worker.run_once
      @worker.run_once

      # In the future, we might want to verify that it transitions the state
      # machine properly, but at a base, it should not fail the workflow.
      workflow_execution.events.map(&:event_type).last.should == "DecisionTaskCompleted"
      class AsyncDecider
        alias_method :handle_request_cancel_activity_task_failed, :old_handle_request_cancel_activity_task_failed
      end
      class ActivityDecisionStateMachine
        alias_method  :create_request_cancel_activity_task_decision,:old_create_request_cancel_activity_task_decision
      end
    end
  end

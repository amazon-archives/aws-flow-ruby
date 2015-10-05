require_relative 'setup'
describe "Workflow/Activity return values/exceptions" do
  include_context "setup integration tests"
    it "ensures that an activity returning more than 32k data fails the activity" do
      general_test(:task_list => "ActivityTaskLargeOutput", :class_name => "ActivityTaskLargeOutput")
      @activity_class.class_eval do
        def run_activity1
          # Make sure we return something that's over 32k. Note this won't
          # necessarily work with all converters, as it's pretty trivially
          # compressible
          return ":" + "a" * 33000
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      history_events = workflow_execution.events.map(&:event_type)
      # Previously, it would time out, as the failure would include the original
      # large output that killed the completion and failure call. Thus, we need to
      # check that we fail the ActivityTask.
      history_events.should include "ActivityTaskFailed"

      workflow_execution.events.to_a.last.attributes.details.should_not =~ /Psych/
      workflow_execution.events.to_a.last.attributes.reason.should == Utilities.validation_error_string("Activity")
      history_events.last.should == "WorkflowExecutionFailed"
    end

    it "ensures that an activity returning an exception of size more than 32k fails the activity correctly and truncates the message" do
      general_test(:task_list => "ActivityTaskExceptionLargeOutput", :class_name => "ActivityTaskExceptionLargeOutput")
      @activity_class.class_eval do
        def run_activity1
          raise  ":" + "a" * 33000
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      history_events = workflow_execution.events.map(&:event_type)
      # Previously, it would time out, as the failure would include the original
      # large output that killed the completion and failure call. Thus, we need to
      # check that we fail the ActivityTask.
      history_events.should include "ActivityTaskFailed"

      workflow_execution.events.to_a.last.attributes.details.should_not =~ /Psych/
      history_events.last.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should include("[TRUNCATED]")
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.data_converter.load(details)
      exception.class.should == AWS::Flow::ActivityTaskFailedException
    end

    it "ensures that an activity returning a Cancellation Exception of size more than 32k fails the activity" do
      general_test(:task_list => "ActivityTaskCancellationExceptionLargeOutput", :class_name => "ActivityTaskCancellationExceptionLargeOutput")
      @activity_class.class_eval do
        def run_activity1
          raise  CancellationException.new("a" * 33000)
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      @activity_worker.run_once
      @worker.run_once
      wait_for_execution(workflow_execution)
      history_events = workflow_execution.events.map(&:event_type)
      history_events.should include "ActivityTaskFailed"

      history_events.last.should == "WorkflowExecutionFailed"
      event = workflow_execution.events.to_a.select { |x| x.event_type == "ActivityTaskFailed"}
      event.first.attributes.reason.should == Utilities.validation_error_string("Activity")
      event.first.attributes.details.should == "AWS::SimpleWorkflow::Errors::ValidationException"
    end

    it "ensures that a workflow output > 32k fails the workflow" do
      general_test(:task_list => "WorkflowOutputTooLarge", :class_name => "WorkflowOutputTooLarge")
      @workflow_class.class_eval do
        def entry_point
          return ":" + "a" * 33000
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      last_event.attributes.reason.should == Utilities.validation_error_string_partial("Workflow")
    end

    it "ensures that a workflow exception details > 32k fails the workflow correctly and truncates the details" do
      general_test(:task_list => "WorkflowExceptionDetailsTooLarge", :class_name => "WorkflowExceptionDetailsTooLarge")
      @workflow_class.class_eval do
        def entry_point
          e = RuntimeError.new("a")
          e.set_backtrace("a"*25769)
          raise e
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.data_converter.load(details)
      exception.class.should == RuntimeError
      exception.backtrace.first.should include ("[TRUNCATED]")
    end

    it "ensures that a workflow exception message > 256 characters fails the workflow correctly and truncates the message" do
      general_test(:task_list => "WorkflowExceptionMessageTooLarge", :class_name => "WorkflowExceptionMessageTooLarge")
      @workflow_class.class_eval do
        def entry_point
          raise  "a" * 257
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should include("[TRUNCATED]")
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.data_converter.load(details)
      exception.class.should == RuntimeError
    end


    it "ensures that a respond_decision_task_completed call with response > 32k that we can't truncate fails the workflow correctly" do
      class CustomException < FlowException
        def initialize(reason, details)
          @something = "a"*50000
          super(reason, details)
        end
      end
      general_test(:task_list => "CustomWorkflowExceptionTooLarge", :class_name => "CustomWorkflowExceptionTooLarge")
      @workflow_class.class_eval do
        def entry_point
          raise  CustomException.new("asdf", "sdf")
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      @worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should == Utilities.validation_error_string("Workflow")
    end

    it "ensures that an activity input > 32k data fails the workflow" do
      general_test(:task_list => "ActivityTaskLargeInput", :class_name => "ActivityTaskLargeInput")
      @workflow_class.class_eval do
        def entry_point
          activity.run_activity1("A"*50000)
        end
      end
      workflow_execution = @my_workflow_client.start_execution
      worker = WorkflowWorker.new(@domain.client, @domain, "ActivityTaskLargeInput", @workflow_class)
      worker.register
      worker.run_once
      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      last_event.attributes.reason.should == Utilities.validation_error_string("Workflow")
      last_event.attributes.details.should == "AWS::SimpleWorkflow::Errors::ValidationException"
    end


    it "ensures that a child workflow input > 32k fails the workflow" do
      general_test(:task_list => "ChildWorkflowInputTooLarge", :class_name => "ChildWorkflowInputTooLarge")
      @workflow_class.class_eval do
        workflow(:child) do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 300,
            default_task_list: "ChildWorkflowInputTooLarge",
            prefix_name: "ChildWorkflowInputTooLargeWorkflow"
          }
        end
        def entry_point
          child_client = AWS::Flow::workflow_client(nil, nil) { { from_class: "ChildWorkflowInputTooLargeWorkflow" } }
          child_client.child("A"*50000)
        end
        def child(input); end
      end

      worker = WorkflowWorker.new(@domain.client, @domain, "ChildWorkflowInputTooLarge", @workflow_class)
      worker.register
      workflow_execution = @my_workflow_client.start_execution
      worker.run_once

      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should == Utilities.validation_error_string("Workflow")
      workflow_execution.events.to_a.last.attributes.details.should == "AWS::SimpleWorkflow::Errors::ValidationException"
    end



    it "ensures that a child workflow exception > 32k fails the workflow correctly and truncates the stacktrace" do
      general_test(:task_list => "ChildWorkflowExceptionTooLarge", :class_name => "ChildWorkflowExceptionTooLarge")
      @workflow_class.class_eval do
        workflow(:child) do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 300,
            default_task_list: "ChildWorkflowExceptionTooLarge",
            prefix_name: "ChildWorkflowExceptionTooLargeWorkflow"
          }
        end
        def entry_point
          child_client = AWS::Flow::workflow_client(nil, nil) { { from_class: "ChildWorkflowExceptionTooLargeWorkflow" } }
          child_client.child
        end
        def child
          raise  ":" + "a" * 33000
        end
      end

      worker = WorkflowWorker.new(@domain.client, @domain, "ChildWorkflowExceptionTooLarge", @workflow_class)
      worker.register
      workflow_execution = @my_workflow_client.start_execution
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once

      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should include("[TRUNCATED]")
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.data_converter.load(details)
      exception.class.should == AWS::Flow::ChildWorkflowFailedException
      exception.cause.class.should == RuntimeError
    end


    it "ensures that a child child workflow exception > 32k fails the workflow correctly and truncates the stacktrace" do
      general_test(:task_list => "ChildChildWorkflowExceptionTooLarge", :class_name => "ChildChildWorkflowExceptionTooLarge")
      @workflow_class.class_eval do
        workflow(:child, :child_1) do
          {
            version: "1.0",
            default_execution_start_to_close_timeout: 300,
            default_task_list: "ChildChildWorkflowExceptionTooLarge",
            prefix_name: "ChildChildWorkflowExceptionTooLargeWorkflow"
          }
        end
        def entry_point
          child_client = AWS::Flow::workflow_client(nil, nil) { { from_class: "ChildChildWorkflowExceptionTooLargeWorkflow" } }
          child_client.child
        end
        def child
          child_1_client = AWS::Flow::workflow_client(nil, nil) { { from_class: "ChildChildWorkflowExceptionTooLargeWorkflow" } }
          child_1_client.child_1
        end
        def child_1
          raise  ":" + "a" * 33000
        end
      end
      worker = WorkflowWorker.new(@domain.client, @domain, "ChildChildWorkflowExceptionTooLarge", @workflow_class)
      worker.register
      workflow_execution = @my_workflow_client.start_execution
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once
      worker.run_once

      wait_for_execution(workflow_execution)
      last_event = workflow_execution.events.to_a.last
      last_event.event_type.should == "WorkflowExecutionFailed"
      workflow_execution.events.to_a.last.attributes.reason.should include("[TRUNCATED]")
      details = workflow_execution.events.to_a.last.attributes.details
      exception = FlowConstants.data_converter.load(details)
      exception.class.should == AWS::Flow::ChildWorkflowFailedException
      exception.cause.class.should == AWS::Flow::ChildWorkflowFailedException
    end
  end

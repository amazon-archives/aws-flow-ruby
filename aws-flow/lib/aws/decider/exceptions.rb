#--
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
#++

module AWS
  module Flow


        # Exception used to communicate failure during fulfillment of a decision sent to SWF. This exception and all its
    # subclasses are expected to be thrown by the framework.
    class DecisionException < Exception
      attr_accessor :event_id
    end


    # An exception that serves as the base for {ChildWorkflowFailedException}, {FailWorkflowExecutionException}, and
    # {ActivityFailureException}.
    class FlowException < Exception
      # A string containing the reason for the exception.
      attr_accessor :reason

      # A string containing details for the exception.
      attr_accessor :details

      # Creates a new FlowException.
      #
      # @param reason [String]
      #   The reason for the exception. This is made available to the exception receiver through the {#reason}
      #   attribute.
      #
      # @param details [String]
      #   The details of the exception. This is made available to the exception receiver through the {#details}
      #   attribute.
      #
      def initialize(reason = "Something went wrong in Flow",
                     details = "But this indicates that it got corrupted getting out")
        @reason = reason
        @details = details
        details = details.message if details.is_a? Exception
        self.set_backtrace(details)
      end
    end



    class ChildWorkflowException < FlowException
      def detail_termination(message, event_id, workflow_execution, workflow_type)
        "#{message} for workflow_execution #{workflow_execution.to_s} with event_id #{event_id}"
      end
    end

    class ChildWorkflowTerminatedException < ChildWorkflowException
      def initialize(event_id, workflow_execution, workflow_type)
        @reason = "WF exception terminated"
        @detail = detail_termination("Terminated", event_id, workflow_execution, workflow_type)
        # TODO: we'll likely want to provide more info later, but it'll take a bit to plumb it
        # @detail = "Terminate for workflow_execution #{workflow_execution.to_s} of workflow_type #{workflow_type.to_s} with event_id #{event_id}"
      end
    end
    class ChildWorkflowTimedOutException < ChildWorkflowException
      def initialize(event_id, workflow_execution, workflow_type)
        @reason = "WF exception timed out"
        @detail = detail_termination("Timed out", event_id, workflow_execution, workflow_type)

      end
    end
    # Unhandled exceptions in child workflows are reported back to the parent workflow implementation by throwing a
    # `ChildWorkflowFailedException`. The original exception can be retrieved from the {#reason} attribute of this
    # exception. The exception also provides information in the {#details} attribute that is useful for debugging
    # purposes, such as the unique identifiers of the child execution.
    #
    # @abstract An exception raised when the child workflow execution has failed.
    #
    class ChildWorkflowFailedException < FlowException

      attr_accessor :cause, :details
      # Creates a new `ChildWorkflowFailedException`
      #
      # @param event_id
      #   The event id for the exception.
      #
      # @param execution
      #   The child workflow execution that raised the exception.
      #
      # @param workflow_type
      #   The workflow type of the child workflow that raised the exception.
      #
      # @param (see FlowException#initialize)
      #
      def initialize(event_id, execution, workflow_type, reason, details)
        @cause = details
        # TODO This should probably do more with the event_id, execution, workflow_type
        super(reason, details)
      end
    end


    # @abstract An exception raised when the workflow execution has failed.
    class FailWorkflowExecutionException < FlowException
    end


    # This exception is used by the framework internally to communicate activity failure. When an activity fails due to
    # an unhandled exception, it is wrapped in ActivityFailureException and reported to Amazon SWF. You need to deal
    # with this exception only if you use the activity worker extensibility points. Your application code will never
    # need to deal with this exception.
    #
    # @abstract An exception raised when the activity has failed.
    class ActivityFailureException < FlowException
    end
    class WorkflowException < FlowException; end
    class SignalExternalWorkflowException < FlowException
      def initialize(event_id, workflow_execution, cause)
        super("Signalling the external workflow failed", cause)
      end
    end

    class StartChildWorkflowFailedException < FlowException
      def initialize(event_id, workflow_execution, workflow_type, cause)
        super("failed to start child workflow", cause)
      end
    end
    class StartTimerFailedException < FlowException
      def initialize(event_id, timer_id, user_context, cause)
        super("Timerid #{timer_id} got messed up", cause)
      end
    end
    # This exception is thrown if an activity was timed out by Amazon SWF. This could happen if the activity task could
    # not be assigned to the worker within the require time period or could not be completed by the worker in the
    # required time. You can set these timeouts on the activity using the @ActivityRegistrationOptions annotation or
    # using the ActivitySchedulingOptions parameter when calling the activity method.
    #
    # @abstract An exception raised when the activity task has timed out.
    class ActivityTaskTimedOutException < ActivityFailureException

      # Creates a new ActivityTaskTimeoutException
      def initialize(id, activity_id, reason, details)
        @id = id
        @activity_id = activity_id
        super(reason, details)
      end
    end

    class ScheduleActivityTaskFailedException < FlowException
      def initialize(event_id, activity_type, activity_id, cause)
        super("Schedule activity task failed", cause)
      end
    end

    # Unhandled exceptions in activities are reported back to the workflow implementation by throwing an
    # `ActivityTaskFailedException`. The original exception can be retrieved from the reason attribute of this
    # exception. The exception also provides information in the `details` attribute that is useful for debugging
    # purposes, such as the unique activity identifier in the history.
    #
    # @abstract An exception raised when the activity task has failed.
    class ActivityTaskFailedException < ActivityFailureException
      attr_accessor :cause, :details
      # Creates a new ActivityTaskFailedException
      def initialize(id, activity_id, reason, details)
        @id = id
        @activity_id = activity_id
        @cause = details
        super(reason, details)
      end
    end
  end
end

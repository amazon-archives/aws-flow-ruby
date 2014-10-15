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

    # Defines an executable activity.
    #
    # @!attribute [ActivityOptions] execution_options
    #   The {ActivityOptions} for this activity.
    #
    class ActivityDefinition
      attr_accessor :execution_options

      # Creates a new ActivityDefinition instance.
      #
      # @param [Object] instance
      #
      # @param [Symbol] activity_method
      #   The method to run when {#execute} is called.
      #
      # @param [Hash] registration_options
      #
      # @param [Hash] execution_options
      #   The {ActivityOptions} for this activity.
      #
      # @param [Object] converter
      #
      def initialize(instance, activity_method, registration_options, execution_options, converter)
        @instance = instance
        @activity_method = activity_method
        @registration_options = registration_options
        @execution_options = execution_options
        @converter = converter
      end

      # Executes the activity.
      #
      # @param [Object] input
      #   Additional input for the activity execution.
      #
      # @param [ActivityExecutionContext] context
      #   The context for the activity execution.
      #
      def execute(input, context)
        begin
          @instance._activity_execution_context = context
          # Since we encode all the inputs in some converter, and these inputs
          # are not "true" Ruby objects yet, there is no way for that input to
          # be an instance of the NilClass(the only thing that responds true to
          # .nil?) and thus we can be assured that if input.nil?, then the
          # method had no input.
          if input.nil?
            result = @instance.send(@activity_method)
          else
            ruby_input = @converter.load input
            result = @instance.send(@activity_method, *ruby_input)
          end
        rescue Exception => e
          #TODO we need the proper error handling here
          raise e if e.is_a? CancellationException

          converted_failure = @converter.dump(e)
          reason = e.message

          # Check if serialized exception violates the 32k limit
          if converted_failure.to_s.size > FlowConstants::FAILURE_DETAILS
            # The ActivityFailureException takes two inputs - reason and
            # details. We put the reason of the current exception e in the reason field of
            # ActivityFailureException and we put the entire serialized
            # exception in the details field of ActivityFailureException. Hence
            # it is necessary for us to fit the entire serialized exception
            # inside 32k limit.
            new_exception, reason = truncate_exception(e)
            # serialize the new exception
            converted_failure = @converter.dump(new_exception)
          end
          raise ActivityFailureException.new(reason, converted_failure)
        ensure
          @instance._activity_execution_context = nil
        end
        converted_result = @converter.dump(result)
        # We are going to have to convert this object into a string to submit it, and that's where the 32k limit will be enforced, so it's valid to turn the object to a string and check the size of the result
        if converted_result.to_s.size > 32768
          return @converter.dump("The result was too large, so we could not serialize it correctly. You can find the full result in the ActivityTaskPoller logs."), result, true
        end
        return converted_result, result, false
      end

      # @api private
      # This method will take the exception and truncate both the reason and
      # details.
      def truncate_exception(e)


        # Truncation overhead for serializing and other stuff. This value is
        # copied from make_fail_decision in async_decider.rb
        truncation_overhead = 8000
        # We want the entire exception to fit inside 32k since the exception
        # will be a part of ActivityTaskFailure's details field.
        size_diff = FlowConstants::FAILURE_DETAILS - FlowConstants::FAILURE_REASON - truncation_overhead

        # get the details/backtrace of the current exception
        new_details = e.details if e.respond_to? :details
        new_details ||= e.backtrace.join("")

        # truncate the details
        new_details = new_details.slice(0, size_diff)
        new_details += "->->->->->THIS BACKTRACE WAS TRUNCATED"

        # get the reason/message of the current exception
        reason = e.message
        if reason.size > FlowConstants::FAILURE_REASON
          # truncate the reason
          reason = reason.slice(0, FlowConstants::FAILURE_REASON - 10)
          reason += "[TRUNCATED]"
        end

        # create a new exception because we can't rewrite the reason/message of an
        # already existing exception
        new_exception = e.class.new(reason)
        if e.respond_to? :details
          new_exception.details = new_details
        else
          new_exception.set_backtrace(new_details)
        end
        [new_exception, reason]
      end

    end

    # The execution context for an activity task.
    class ActivityExecutionContext
      attr_accessor :service, :domain, :task

      # Initializes a new `ActivityExecutionContext` object.
      #
      # @param [AWS::SimpleWorkflow] service
      #   An instance of [AWS::SimpleWorkflow](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow.html) to
      #   set for the activity execution context.
      #
      # @param [AWS::SimpleWorkflow::Domain] domain
      #   The [AWS::SimpleWorkflow::Domain](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Domain.html)
      #   in which the activity task is running.
      #
      # @param [AWS::SimpleWorkflow::ActivityTask] task
      #   The
      #   [AWS::SimpleWorkflow::ActivityTask](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/ActivityTask.html)
      #   that this execution context is for.
      #
      def initialize(service, domain, task)
        @service = service
        @domain = domain
        @task = task
      end

      # Gets the [task
      # token](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/ActivityTask.html#task_token-instance_method),
      # an opaque string that can be used to uniquely identify this task execution.
      # @return [String] the activity task token.
      def task_token
        @task.task_token
      end

      # Gets the
      # [AWS::SimpleWorkflow::WorkflowExecution](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/WorkflowExecution.html)
      # instance that is the context for this activity execution.
      #
      # @return [AWS::SimpleWorkflow::WorkflowExecution]
      #   The `WorkflowExecution` in this activity execution context.
      #
      def workflow_execution
        @task.workflow_execution
      end

      # Records a heartbeat for the activity, indicating to Amazon SWF that the activity is still making progress.
      #
      # @param [String] details
      #   If specified, contains details about the progress of the activity task. Up to 2048
      #   characters can be provided.
      #
      # @raise [CancellationException]
      #   The activity task has been cancelled.
      #
      def record_activity_heartbeat(details)
        to_send = {:task_token => task_token.to_s, :details => details.to_s }
        response = @service.record_activity_task_heartbeat(to_send)
        # TODO See if cancel requested, throw exception if so
        raise CancellationException if response["cancelRequested"]

      end

    end

  end
end

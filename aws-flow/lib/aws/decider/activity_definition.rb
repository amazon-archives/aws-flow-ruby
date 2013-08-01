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

      # Creates a new ActivityDefinition instance
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

      # Executes the activity
      #
      # === Parameters
      #
      # @param [Object] input
      #   Optional input for the activity execution.
      #
      # @param [ActivityExecutionContext] context
      #   The context for the activity execution.
      #
      def execute(input, context)
        begin
          @instance._activity_execution_context = context
          # Since we encode all the inputs in some converter, and these inputs
          # are not "true" ruby objects yet, there is no way for that input to
          # be an instance of the NilClass(the only thing that responds true to
          # .nil?) and thus we can be assured that if input.nil?, then the
          # method had no input
          if input.nil?
            result = @instance.send(@activity_method)
          else
            ruby_input = @converter.load input
            result = @instance.send(@activity_method, *ruby_input)
          end
        rescue Exception => e
          #TODO we need the proper error handling here
          raise e if e.is_a? CancellationException
          raise ActivityFailureException.new(e.message, @converter.dump(e))
        ensure
          @instance._activity_execution_context = nil
        end
        return @converter.dump result
      end

    end

    class ActivityExecutionContext
      attr_accessor :service, :domain, :task
      def initialize(service, domain, task)
        @service = service
        @domain = domain
        @task = task
      end
      def task_token
        @task.task_token
      end

      def workflow_execution
        @task.workflow_execution
      end

      def record_activity_heartbeat(details)
        to_send = {:task_token => task_token.to_s, :details => details.to_s }
        response = @service.record_activity_task_heartbeat(to_send)
        # TODO See if cancel requested, throw exception if so
        raise CancellationException if response["cancelRequested"]

      end

    end

  end
end

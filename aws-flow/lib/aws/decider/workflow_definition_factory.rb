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

    class WorkflowDefinitionFactory

      class << self
        # Method to create a workflow definition map from a workflow class
        def generate_definition_map(workflow_class)

          unless workflow_class.respond_to?(:workflows)
            raise ArgumentError.new("workflow_class must extend module AWS::Flow::Workflows")
          end

          workflow_definition_map = {}

          workflow_class.workflows.each do |workflow_type|
            options = workflow_type.options
            execution_method = options.execution_method
            registration_options = options.get_registration_options
            get_state_method = workflow_class.get_state_method
            signals = workflow_class.signals

            workflow_definition_map[workflow_type] = self.new(
              workflow_class,
              workflow_type,
              registration_options,
              options,
              execution_method,
              signals,
              get_state_method
            )
          end
          workflow_definition_map
        end
      end

      attr_reader :converter
      def initialize(klass, workflow_type, registration_options, implementation_options, workflow_method, signals, get_state_method)
        @klass = klass
        @workflow_type = workflow_type
        @registration_options = registration_options
        @implementation_options = implementation_options
        @workflow_method = workflow_method
        @signals = signals
        @get_state_method = get_state_method
        if ! implementation_options.nil?
          @converter = implementation_options.data_converter
        end
        @converter ||= FlowConstants.data_converter

      end

      def get_workflow_definition(decision_context)
        FlowFiber.current[:decision_context] = decision_context
        this_instance = @klass.new
        WorkflowDefinition.new(this_instance, @workflow_method, @signals, @get_state_method, @converter)
      end

      def delete_workflow_definition(definition)
        FlowFiber.unset(FlowFiber.current, :decision_context)
        # Indicates to GC that these values are no longer needed.
        FlowFiber.local_variables.each_pair do |key, value|
          value = nil
          FlowFiber.local_variables.delete(key)
        end
      end

    end
  end
end

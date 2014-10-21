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

    # Represents a workflow definition. Every workflow implementation needs to be a subclass of this class.
    #
    # Usually there should be no need to instantiate the class manually. Instead, the @execute method is called to
    # start the workflow. You can think of this class as having factory class methods.
    class WorkflowDefinition

      attr_reader :decision_helper

      attr_reader :converter

      def initialize(instance, workflow_method, signals, get_state_method, converter)
        @instance = instance
        @workflow_method = workflow_method
        @get_state_method = get_state_method
        @signals = signals
        @converter = converter
      end

      def execute(input = nil)
        #TODO Set up all the converter stuff
        result = Future.new
        method_output = Future.new
        error_handler do |t|
          t.begin do
            if input.nil?
              method_output.set(@instance.send(@workflow_method))
            else
              ruby_input = @converter.load input
              # Have to have `ruby_input` in order to be able to handle sending
              # arbitrary arguments correctly, as otherwise it will seem as if
              # @workflow_method will always have an arity of 1.
              method_output.set(@instance.send(@workflow_method, *ruby_input))
            end
          end
          t.rescue(Exception) do |e|

            converted_failure = @converter.dump(e)
            reason = e.message

            # Check if serialized exception violates the 32k limit
            if converted_failure.to_s.size > FlowConstants::DETAILS_LIMIT
              # Truncate the exception to fit in the response
              reason, new_exception = AWS::Flow::Utilities::truncate_exception(e)

              # Serialize the new exception
              converted_failure = @converter.dump(new_exception)
            end

            @failure = WorkflowException.new(reason, converted_failure)
            #TODO error handling stuff
          end
          t.ensure do
            raise @failure if @failure
            # We are going to have to convert this object into a string to submit it, and that's where the 32k limit will be enforced, so it's valid to turn the object to a string and check the size of the result
            output = @converter.dump method_output.get
            if output.to_s.size > 32768
              error_message = "We could not serialize the output of the Workflow correctly since it was too large. Please limit the size of the output to 32768 characters. Please look at the Workflow Worker logs to see the original output."
              raise WorkflowException.new(error_message, "")
            end
            result.set(output)
          end
          end
        return result
      end

      def get_workflow_state
        return nil if @get_state_method.nil?
        converter = @get_state_method.data_converter || @converter
        method = @get_state_method.method_name
        begin
          result = @instance.send(method)
          return converter.dump(result)
        rescue Exception => e
          raise WorkflowException.new(e.message, converter.dump(e))
        end
      end



      def signal_received(signal_name, input)
        method_pair = @signals[signal_name]
        raise "No such signal for #{signal_name}" unless method_pair
        converter = method_pair.data_converter
        method_name = method_pair.method_name
        error_handler do |t|
          parameters = nil
          t.begin do
            if input.class <= NoInput
              @instance.send(method_name)
            else
              parameters = converter.load input
              @instance.send(method_name, *parameters)
            end
          end
          t.rescue(Exception) do |e|
            WorkflowException.new("Got an error while sending #{method_name} with parameters #{parameters}", converter.dump(e))
          end
        end
      end
    end

  end
end

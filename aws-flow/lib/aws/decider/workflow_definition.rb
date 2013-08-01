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

    # Every workflow implementation needs to be a subclass of this class.
    #
    # Usually there should be no need to instantiate the class manually, as instead, the @execute method is called to
    # start the workflow (you can think of ths as having factory class methods).
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
              # Have to have *ruby_input in order to be able to handle sending
              # arbitrary arguments correctly, as otherwise it will seem as if
              # @workflow_method will always have an arity of 1
              method_output.set(@instance.send(@workflow_method, *ruby_input))
            end
          end
          t.rescue(Exception) do |error|
            @failure = WorkflowException.new(error.message, @converter.dump(error))
            #TODO error handling stuff
          end
          t.ensure do
            raise @failure if @failure
            result.set(@converter.dump method_output.get)
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

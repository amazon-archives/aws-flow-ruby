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

# Contains a DFA-like framework , where the transition functions can also perform arbitrary computation before moving to the next state
module AWS
  module Flow
    module Core
      module SimpleDFA
        attr_accessor :transitions, :symbols, :states, :start_state
        def init(start_state)
          include InstanceMethods
          @start_state = start_state
          @symbols = []
          @states = []
          @transitions = {}
          @states << start_state
        end

        def get_start_state
          @start_state
        end

        def get_transitions
          @transitions
        end

        def define_general(state, &block)
          @symbols.each do |symbol|
            if @transitions[[state, symbol]].nil?
              @transitions[[state, symbol]] = block
            end
          end
        end

        def add_transition(state, symbol, &block)
          @symbols << symbol unless @symbols.include? symbol
          @states << state unless @states.include? state
          @transitions[[state, symbol]] = block
        end

        def uncovered_transitions
          @states.product(@symbols) - @transitions.keys
        end

        module InstanceMethods
          attr_accessor :current_state

          def consume(symbol)
            @current_state ||= self.class.get_start_state
            func_to_call = self.class.get_transitions[[@current_state, symbol]]
            raise "This is not a legal transition" unless func_to_call
            func_to_call.call(self)
          end
        end
      end

    end
  end
end

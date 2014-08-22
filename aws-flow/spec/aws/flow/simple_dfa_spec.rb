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

describe SimpleDFA do
  before(:each) do
    class Tester
      attr_accessor :trace

      extend SimpleDFA
      init(:start_state)

      def initialize
        @trace = []
      end

      add_transition(:start_state, :a) do |t|
        t.trace << :start_to_second
        t.current_state = :second_state
      end
      add_transition(:second_state, :b) do |t|
        t.trace << :second_to_third
        t.current_state = :third_state
      end
      add_transition(:third_state, :c) do |t|
        t.trace << :third_to_start
        t.current_state = :start_state
      end
    end
    @this_dfa = Tester.new

  end

  it "ensures that consume works as expected" do
    @this_dfa.consume(:a)
    @this_dfa.trace.should == [:start_to_second]
    @this_dfa.current_state.should == :second_state
  end

  it "ensures that define_general defines general transitions for a state" do
    class Tester
      define_general(:start_state) {|t| t.current_state = :new_state }
    end
    @this_dfa.consume(:c)
    @this_dfa.current_state.should == :new_state
  end

  it "ensures that uncovered_transitions raises on those transitions" do
    expect { @this_dfa.consume(:b) }.to raise_error
  end

end

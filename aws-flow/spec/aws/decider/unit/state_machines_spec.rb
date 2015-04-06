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

require "spec_helper"

describe ChildWorkflowDecisionStateMachine do
  before do
    @this_machine = ChildWorkflowDecisionStateMachine.new('id1',nil)
  end

  it "ensures to handle child cancellation that wasn't initiated from the parent" do
    @this_machine.current_state = :started
    @this_machine.consume(:handle_cancellation_event)
    @this_machine.current_state.should == :completed
  end

  it "ensures to handle StartChildFailed after StartChildInitiated" do
    @this_machine.current_state = :initiated
    @this_machine.consume(:handle_initiation_failed_event)
    @this_machine.current_state.should == :completed
  end

end

describe ActivityDecisionStateMachine do
  before do
    @this_machine = ActivityDecisionStateMachine.new('id2',nil)
  end

  it "ensures to handle ScheduleActivity failed after ScheduleActivity" do
    @this_machine.current_state = :initiated
    @this_machine.consume(:handle_initiation_failed_event)
    @this_machine.current_state.should == :completed
  end

end

describe TimerDecisionStateMachine do
  before do
    @this_machine = TimerDecisionStateMachine.new('id3',nil)
  end

  it "ensures to handle StartTimer failed after StartTimer" do
    @this_machine.current_state = :initiated
    @this_machine.consume(:handle_initiation_failed_event)
    @this_machine.current_state.should == :completed
  end

end
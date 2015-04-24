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

require 'spec_helper'

describe ExternalConditionVariable do
  let(:condition) { ExternalConditionVariable.new }

  it "blocks a thread and wakes it up on signal" do
    t = Thread.new { condition.wait }
    Test::Unit::wait("run", 1, t)
    t.status.should == "sleep"
    condition.signal
    Test::Unit::wait("run", 1, t)
    t.status.should == false
  end

  it "blocks multiple threads and wakes one up on signal" do
    t1 = Thread.new { condition.wait }
    t2 = Thread.new { condition.wait }

    Test::Unit::wait("run", 1, t1, t2)
    t1.status.should == "sleep"
    t2.status.should == "sleep"
    condition.signal

    Test::Unit::wait("run", 1, t1, t2)
    (t1.status == false && t2.status == false).should == false
    condition.signal

    Test::Unit::wait("run", 1, t1, t2)
    (t1.status == false && t2.status == false).should == true
  end

  it "blocks a thread and wakes it up on broadcast" do
    t = Thread.new { condition.wait }

    Test::Unit::wait("run", 1, t)
    sleep 1 if t.status == "run"
    t.status.should == "sleep"
    condition.broadcast

    Test::Unit::wait("run", 1, t)
    sleep 1 if t.status == "run"
    t.status.should == false
  end

  it "blocks multiple threads and wakes them up on broadcast" do
    t1 = Thread.new { condition.wait }
    t2 = Thread.new { condition.wait }

    Test::Unit::wait("run", 1, t1, t2)
    t1.status.should == "sleep"
    t2.status.should == "sleep"
    condition.broadcast

    Test::Unit::wait("run", 1, t1, t2)
    sleep 1 if t1.status == "run" || t2.status == "run"
    (t1.status == false && t2.status == false).should == true
  end

end

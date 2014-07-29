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

require 'aws/decider'
include AWS::Flow

describe ForkingExecutor do

  it "makes sure that forking executors basic execute works" do
    test_file_name = "ForkingExecutorTestFile"
    begin
      forking_executor = ForkingExecutor.new
      File.exists?(test_file_name).should == false
      forking_executor.execute do
        File.new(test_file_name, 'w')
      end
      sleep 3
      File.exists?(test_file_name).should == true
    ensure
      File.unlink(test_file_name)
    end
  end

  it "ensures that you cannot execute more tasks on a shutdown executor" do
    forking_executor = ForkingExecutor.new
    forking_executor.execute do
    end
    forking_executor.execute do
    end
    forking_executor.shutdown(1)
    expect { forking_executor.execute { "yay" } }.to raise_error
    RejectedExecutionException
  end

  context "#remove_completed_pids" do
    context "with block=false" do
      it "should reap all completed child processes" do
        executor = ForkingExecutor.new(max_workers: 3)

        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.execute { sleep 5 }
        executor.pids.size.should == 3
        sleep 2
        executor.send(:remove_completed_pids, false)
        # The two processes that are completed will be reaped.
        executor.pids.size.should == 1
      end
    end
    context "with block=true" do
      it "should wait for atleast one child process to become available and then reap as many as possible" do
        executor = ForkingExecutor.new(max_workers: 3)

        executor.execute { sleep 2 }
        executor.execute { sleep 2 }
        executor.execute { sleep 5 }
        executor.pids.size.should == 3
        executor.send(:remove_completed_pids, true)
        # It will wait for one of the processes to become available and then
        # reap as many as possible
        executor.pids.size.should == 1
      end
    end
  end
end
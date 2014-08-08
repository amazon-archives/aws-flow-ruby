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

require_relative 'setup'

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

  it "ensures that one worker for forking executor will only allow one thing to be processed at a time" do
    executor = ForkingExecutor.new(:max_workers => 1)

    test_file_name = "ForkingExecutorRunOne"
    File.new(test_file_name, "w")
    start_time = Time.now
    executor.execute do
      File.open(test_file_name, "a+") { |f| f.write("First Execution\n")}
      sleep 4
    end
    # Because execute will block if the worker queue is full, we will wait here
    # if we have reached the max number of workers
    executor.execute { 2 + 2 }
    finish_time = Time.now
    # If we waited for the first task to finish, then we will have waited at
    # least 4 seconds; if we didn't, we should not have waited. Thus, if we have
    # waited > 3 seconds, we have likely waited for the first task to finish
    # before doing the second one
    (finish_time - start_time).should > 3
    File.unlink(test_file_name)
  end

  it "ensures that you cannot execute more tasks on a shutdown executor" do
    forking_executor = ForkingExecutor.new
    forking_executor.execute {}
    forking_executor.execute {}
    forking_executor.shutdown(1)
    expect { forking_executor.execute { "yay" } }.to raise_error RejectedExecutionException
  end

  context "#remove_completed_pids" do
    class Status
      def success?; true; end
    end

    context "with block=false" do
      it "should not block if not child process available" do

        # stub out Process.waitpid2 to return only 2 processes
        allow(Process).to receive(:waitpid2).and_return(nil)
        allow_any_instance_of(AWS::Flow::ForkingExecutor).to receive(:fork).and_return(1, 2, 3)
        executor = ForkingExecutor.new(max_workers: 3)

        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.pids.size.should == 3
        executor.send(:remove_completed_pids, false)
        # The two processes that are completed will be reaped.
        executor.pids.size.should == 3
      end

      it "should reap all completed child processes" do

        allow(Process).to receive(:waitpid2).and_return([1, Status.new], [2, Status.new], [3, Status.new] )
        allow_any_instance_of(AWS::Flow::ForkingExecutor).to receive(:fork).and_return(1, 2, 3)
        executor = ForkingExecutor.new(max_workers: 3)

        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.pids.size.should == 3
        executor.send(:remove_completed_pids, false)
        # The two processes that are completed will be reaped.
        executor.pids.size.should == 0
      end
    end
    context "with block=true" do
      it "should wait for atleast one child process to become available and then reap as many as possible" do

        # stub out Process.waitpid2 to return only 2 processes
        allow(Process).to receive(:waitpid2).and_return([1, Status.new], [2, Status.new], nil)
        allow_any_instance_of(AWS::Flow::ForkingExecutor).to receive(:fork).and_return(1, 2, 3)
        executor = ForkingExecutor.new(max_workers: 3)

        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
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

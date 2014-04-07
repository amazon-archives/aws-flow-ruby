require 'spec_helper'

describe AWS::Flow::ActivityOptions do
  context "#defaults" do
    it "should be initialized with NONE" do
      options = AWS::Flow::ActivityOptions.new
      options.default_task_schedule_to_start_timeout.should == "NONE"
      options.default_task_schedule_to_close_timeout.should == "NONE"
      options.default_task_start_to_close_timeout.should == "NONE"
      options.default_task_heartbeat_timeout.should == "NONE"
    end
    it "should change to the value passed in" do
      options = AWS::Flow::ActivityOptions.new({
        default_task_schedule_to_start_timeout: 20,
        default_task_schedule_to_close_timeout: 50,
        default_task_start_to_close_timeout: 30,
        default_task_heartbeat_timeout: 5,
      })
      options.default_task_schedule_to_start_timeout.should == "20"
      options.default_task_schedule_to_close_timeout.should == "50"
      options.default_task_start_to_close_timeout.should == "30"
      options.default_task_heartbeat_timeout.should == "5"
    end
    it "should be overriden when a non default value is set" do
      options = AWS::Flow::ActivityOptions.new
      options.schedule_to_start_timeout = 20
      options.schedule_to_close_timeout = 50
      options.start_to_close_timeout = 30
      options.heartbeat_timeout = 5

      options.schedule_to_start_timeout.should == "20"
      options.schedule_to_close_timeout.should == "50"
      options.start_to_close_timeout.should == "30"
      options.heartbeat_timeout.should == "5"

      options.default_task_schedule_to_start_timeout.should == "NONE"
      options.default_task_schedule_to_close_timeout.should == "NONE"
      options.default_task_start_to_close_timeout.should == "NONE"
      options.default_task_heartbeat_timeout.should == "NONE"
    end
  end
end

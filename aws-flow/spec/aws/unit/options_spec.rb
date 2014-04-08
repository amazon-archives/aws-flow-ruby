require 'spec_helper'

describe AWS::Flow::OptionsMethods do

  class FooDefaults < AWS::Flow::Defaults
    def default_foo; 1; end
    def default_baz; 2; end
    def default_xyz; 3; end
  end
  class FooOptions < AWS::Flow::Options
    include AWS::Flow::OptionsMethods
    property(:default_foo)
    property(:foo)
    property(:bar)
    property(:baz)
    property(:xyz)
    property(:default_baz)
    default_classes << FooDefaults.new

    def default_keys
      [:default_foo, :default_baz, :default_xyz]
    end
    def make_runtime_key(key)
      key.to_s.gsub(/default_/, "").to_sym
    end
  end

  context "#get_full_options" do
    it "should return merged default and regular options" do
      options = FooOptions.new(
        default_foo: 10,
        bar: 10,
        xyz: 10
      )
      options.get_full_options[:foo].should == "10"
      options.get_full_options[:bar].should == "10"
      options.get_full_options[:baz].should == "2"
      options.get_full_options[:xyz].should == "10"

    end
  end

  context "#get_runtime_options" do
    it "should return the runtime values for the default options" do
      options = FooOptions.new(
        default_foo: 10,
        bar: 10
      )
      options.get_runtime_options.has_key?(:foo).should == true
      options.get_runtime_options.has_key?(:baz).should == true
      options.get_runtime_options[:foo].should == "10"
      options.get_runtime_options[:baz].should == "2"
      options.get_runtime_options.has_key?(:bar).should == false
    end
  end
  context "#get_default_options" do
    it "should return the default values for the default options" do
      options = FooOptions.new(
        default_foo: 10,
        baz: 10
      )
      options.get_default_options.has_key?(:default_foo).should == true
      options.get_default_options.has_key?(:default_baz).should == true
      options.get_default_options[:default_foo].should == "10"
      options.get_default_options[:default_baz].should == "2"
    end
  end
end

describe AWS::Flow::ActivityOptions do
  context "#defaults" do
    it "should be initialized with NONE" do
      options = AWS::Flow::ActivityOptions.new
      options.default_task_schedule_to_start_timeout.should == "NONE"
      options.default_task_schedule_to_close_timeout.should == "NONE"
      options.default_task_start_to_close_timeout.should == "NONE"
      options.default_task_heartbeat_timeout.should == "NONE"
      options.data_converter.should ==
        AWS::Flow::FlowConstants.default_data_converter
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

    it "should remain the same when a non default value is set" do
      options = AWS::Flow::ActivityOptions.new
      options.schedule_to_start_timeout = 20
      options.schedule_to_close_timeout = 50
      options.start_to_close_timeout = 30
      options.heartbeat_timeout = 5

      options.default_task_schedule_to_start_timeout.should == "NONE"
      options.default_task_schedule_to_close_timeout.should == "NONE"
      options.default_task_start_to_close_timeout.should == "NONE"
      options.default_task_heartbeat_timeout.should == "NONE"
    end

  end

  context "#default_keys" do
    it "should return the correct set of default keys" do
      AWS::Flow::ActivityOptions.new.default_keys.should == [
        :default_task_heartbeat_timeout,
        :default_task_schedule_to_close_timeout,
        :default_task_schedule_to_start_timeout,
        :default_task_start_to_close_timeout,
        :default_task_list
      ]
    end

  end
  context "#make_runtime_key" do
    it "should correctly convert default keys to runtime keys" do
      AWS::Flow::ActivityOptions.new.make_runtime_key(
        :default_task_foo
      ).should == :foo
    end

    it "should do nothing if an option without leading default_ is passed in" do
      AWS::Flow::ActivityOptions.new.make_runtime_key(:foo).should == :foo
    end

    it "should handle the special case of default_task_list properly" do
      AWS::Flow::ActivityOptions.new.make_runtime_key(
        :default_task_list
      ).should == :task_list
    end
  end
end

describe AWS::Flow::WorkflowOptions do
  context "#defaults" do
    it "should be initialized correctly" do
      options = AWS::Flow::WorkflowOptions.new
      options.default_task_start_to_close_timeout.should == "30"
      options.default_child_policy.should == "TERMINATE"
      options.tag_list.should == []
      options.data_converter.should ==
        AWS::Flow::FlowConstants.default_data_converter
    end

    it "should change to the value passed in" do
      options = AWS::Flow::WorkflowOptions.new({
        default_task_start_to_close_timeout: 20,
        default_execution_start_to_close_timeout: 120,
        default_child_policy: "ABANDON",
      })
      options.default_task_start_to_close_timeout.should == "20"
      options.default_execution_start_to_close_timeout.should == "120"
      options.default_child_policy.should == "ABANDON"
    end

    it "should remain the same when a non default value is set" do
      options = AWS::Flow::WorkflowOptions.new
      options.task_start_to_close_timeout = 20
      options.execution_start_to_close_timeout = 120
      options.child_policy = "ABANDON"

      options.default_task_start_to_close_timeout.should == "30"
      options.default_execution_start_to_close_timeout.nil?.should == true
      options.default_child_policy.should == "TERMINATE"
    end
  end

  context "#default_keys" do
    it "should return the correct set of default keys" do
      AWS::Flow::WorkflowOptions.new.default_keys.should == [
        :default_task_start_to_close_timeout,
        :default_execution_start_to_close_timeout,
        :default_task_list,
        :default_child_policy
      ]
    end

  end
  context "#make_runtime_key" do
    it "should correctly convert default keys to runtime keys" do
      AWS::Flow::WorkflowOptions.new.make_runtime_key(
        :default_foo
      ).should == :foo
    end

    it "should do nothing if an option without leading default_ is passed in" do
      AWS::Flow::WorkflowOptions.new.make_runtime_key(:foo).should == :foo
    end

    it "should handle the special case of default_task_list properly" do
      AWS::Flow::WorkflowOptions.new.make_runtime_key(
        :default_task_list
      ).should == :task_list
    end
  end

end

describe AWS::Flow::WorkflowDefaults do
  context "#default_task_start_to_close_timeout" do
    it "should return a value of 30" do
      AWS::Flow::WorkflowDefaults.new.default_task_start_to_close_timeout
      .should == 30
    end
  end

  context "#default_child_policy" do
    it "should return TERMINATE" do
      AWS::Flow::WorkflowDefaults.new.default_child_policy.should == "TERMINATE"
    end
  end

  context "#tag_list" do
    it "should return an empty array" do
      AWS::Flow::WorkflowDefaults.new.tag_list.should == []
    end
  end

  context "#data_converter" do
    it "should return the default data converter" do
      AWS::Flow::WorkflowDefaults.new.data_converter
      .should == AWS::Flow::FlowConstants.default_data_converter
    end
  end
end

describe AWS::Flow::ActivityDefaults do
  context "#default_task_schedule_to_start_timeout" do
    it "should return Float::INFINITY" do
      AWS::Flow::ActivityDefaults.new.default_task_schedule_to_start_timeout
      .should == Float::INFINITY
    end
  end

  context "#default_task_schedule_to_close_timeout" do
    it "should return Float::INFINITY" do
      AWS::Flow::ActivityDefaults.new.default_task_schedule_to_close_timeout
      .should == Float::INFINITY
    end
  end
  context "#default_task_start_to_close_timeout" do
    it "should return Float::INFINITY" do
      AWS::Flow::ActivityDefaults.new.default_task_start_to_close_timeout
      .should == Float::INFINITY
    end
  end
  context "#default_task_heartbeat_timeout" do
    it "should return Float::INFINITY" do
      AWS::Flow::ActivityDefaults.new.default_task_heartbeat_timeout
      .should == Float::INFINITY
    end
  end
  context "#data_converter" do
    it "should return the default data converter" do
      AWS::Flow::ActivityDefaults.new.data_converter
      .should == AWS::Flow::FlowConstants.default_data_converter
    end
  end
end


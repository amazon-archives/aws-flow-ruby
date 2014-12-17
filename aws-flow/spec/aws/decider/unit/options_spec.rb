require_relative 'setup'

describe AWS::Flow::ActivityRegistrationOptions do
  context "#get_registration_options" do

    it "should return the default registration options if not value is passed in" do
      options = AWS::Flow::ActivityRegistrationOptions.new
      options.get_registration_options.should == { 
        default_task_heartbeat_timeout: "NONE",
        default_task_schedule_to_close_timeout: "NONE",
        default_task_schedule_to_start_timeout: "NONE",
        default_task_start_to_close_timeout: "NONE",
        default_task_priority: "0",
        default_task_list: "USE_WORKER_TASK_LIST"
      }
    end

    it "should return the new registration options if values are passed in" do
      options = AWS::Flow::ActivityRegistrationOptions.new({
        default_task_schedule_to_start_timeout: 20,
        default_task_schedule_to_close_timeout: 50,
        default_task_start_to_close_timeout: 30,
        default_task_heartbeat_timeout: 5,
        default_task_priority: 100,
        default_task_list: "test_tasklist",
      })
      options.get_registration_options.should == {
        default_task_heartbeat_timeout: "5",
        default_task_schedule_to_close_timeout: "50",
        default_task_schedule_to_start_timeout: "20",
        default_task_start_to_close_timeout: "30",
        default_task_priority: "100",
        default_task_list: "test_tasklist"
      }
    end
  end

  context "#get_full_options" do

    it "should contain both registration and regular options" do
      options = AWS::Flow::ActivityRegistrationOptions.new
      full_options = options.get_full_options
      expected = {
        default_task_heartbeat_timeout: "NONE",
        default_task_schedule_to_close_timeout: "NONE",
        default_task_schedule_to_start_timeout: "NONE",
        default_task_start_to_close_timeout: "NONE",
        default_task_priority: "0",
        default_task_list: "USE_WORKER_TASK_LIST",
        data_converter: FlowConstants.default_data_converter
      }
      # We first compare and remove the following values because these objects have issues
      # when we sort the values array below
      full_options.delete(:data_converter) == expected.delete(:data_converter)
      full_options.keys.sort.should == expected.keys.sort
      full_options.values.sort.should == expected.values.sort
    end

    it "should return the values passed in" do
      options = AWS::Flow::ActivityRegistrationOptions.new({
        default_task_schedule_to_start_timeout: 20,
        default_task_start_to_close_timeout: 30,
        default_task_heartbeat_timeout: 5,
        task_list: "test_tasklist",
        version: "1.0",
        prefix_name: "FooActivity",
        task_priority: 100,
        manual_completion: true,
        heartbeat_timeout: 10
      })
      full_options = options.get_full_options
      expected = {
        default_task_heartbeat_timeout: "5",
        default_task_schedule_to_close_timeout: "NONE",
        default_task_schedule_to_start_timeout: "20",
        default_task_start_to_close_timeout: "30",
        default_task_list: "USE_WORKER_TASK_LIST",
        task_list: "test_tasklist",
        default_task_priority: "0",
        task_priority: "100",
        version: "1.0",
        prefix_name: "FooActivity",
        manual_completion: true,
        heartbeat_timeout: "10",
        data_converter: FlowConstants.default_data_converter
      }
      # We first compare and remove the following values because these objects have issues
      # when we sort the values array below
      [:data_converter, :manual_completion].each { |x| full_options.delete(x).should == expected.delete(x) }
      full_options.keys.sort.should == expected.keys.sort
      full_options.values.sort.should == expected.values.sort
    end

  end

end

describe AWS::Flow::WorkflowRegistrationOptions do

  context "#get_registration_options" do
    
    it "should return the default registration options if not value is passed in" do
      options = AWS::Flow::WorkflowRegistrationOptions.new
      options.get_registration_options.should == { 
        default_task_start_to_close_timeout: "30",
        default_child_policy: "TERMINATE",
        default_task_priority: "0",
        default_task_list: "USE_WORKER_TASK_LIST"
      }
    end

    it "should return the new registration options if values are passed in" do
      options = AWS::Flow::WorkflowRegistrationOptions.new({
        default_task_schedule_to_close_timeout: 30,
        default_execution_start_to_close_timeout: 600,
        default_child_policy: "ABANDON",
        default_task_priority: "100",
        default_task_list: "task_list"
      })
      options.get_registration_options.should == {
        default_task_start_to_close_timeout: "30",
        default_execution_start_to_close_timeout: "600",
        default_child_policy: "ABANDON",
        default_task_priority: "100",
        default_task_list: "task_list"
      }
    end

  end

  context "#get_full_options" do

    it "should not contain any registration options" do
      options = AWS::Flow::WorkflowRegistrationOptions.new
      full_options = options.get_full_options
      expected = {
        default_task_start_to_close_timeout: "30",
        default_child_policy: "TERMINATE",
        default_task_list: "USE_WORKER_TASK_LIST",
        default_task_priority: "0",
        tag_list: [],
        data_converter: FlowConstants.default_data_converter
      }
      # We first compare and remove the following values because these objects have issues
      # when we sort the values array below
      [:data_converter, :tag_list].each { |x| full_options.delete(x).should == expected.delete(x) }
      full_options.keys.sort.should == expected.keys.sort
      full_options.values.sort.should == expected.values.sort
    end

    it "should return the values passed in" do
      options = AWS::Flow::WorkflowRegistrationOptions.new({
        default_task_start_to_close_timeout: "30",
        default_child_policy: "TERMINATE",
        default_task_list: "USE_WORKER_TASK_LIST",
        task_list: "test_tasklist",
        version: "1.0",
        prefix_name: "FooActivity",
        tag_list: ["tag1", "tag2"]
      })
      full_options = options.get_full_options
      expected = {
        default_task_start_to_close_timeout: "30",
        default_child_policy: "TERMINATE",
        default_task_list: "USE_WORKER_TASK_LIST",
        task_list: "test_tasklist",
        default_task_priority: "0",
        version: "1.0",
        prefix_name: "FooActivity",
        data_converter: FlowConstants.default_data_converter,
        tag_list: ["tag1", "tag2"]
      }
      # We first compare and remove the following values because these objects have issues
      # when we sort the values array below
      [:data_converter, :tag_list].each { |x| full_options.delete(x).should == expected.delete(x) }
      full_options.keys.sort.should == expected.keys.sort
      full_options.values.sort.should == expected.values.sort
    end

  end

end

describe AWS::Flow::ActivityOptions do

  context "#get_full_options" do

    it "should only contain the non registration options" do
      options = AWS::Flow::ActivityOptions.new
      full_options = options.get_full_options
      expected = {
        data_converter: FlowConstants.default_data_converter
      }
      # We first compare and remove the following values because these objects have issues
      # when we sort the values array below
      full_options.should == expected
    end

    it "should return the values passed in" do
      options = AWS::Flow::ActivityOptions.new({
        task_list: "test_tasklist",
        version: "1.0",
        prefix_name: "FooActivity",
        task_priority: 100,
        manual_completion: true,
        heartbeat_timeout: 10
      })
      full_options = options.get_full_options
      expected = {
        task_list: "test_tasklist",
        version: "1.0",
        prefix_name: "FooActivity",
        manual_completion: true,
        task_priority: "100",
        heartbeat_timeout: "10",
        data_converter: FlowConstants.default_data_converter,
      }
      # We first compare and remove the following values because these objects have issues
      # when we sort the values array below
      [:data_converter, :manual_completion].each { |x| full_options.delete(x).should == expected.delete(x) }
      full_options.keys.sort.should == expected.keys.sort
      full_options.values.sort.should == expected.values.sort
    end

  end

end
describe AWS::Flow::WorkflowOptions do

  context "#get_full_options" do

    it "should not contain any registration options" do
      options = AWS::Flow::WorkflowOptions.new
      full_options = options.get_full_options
      expected = {
        data_converter: FlowConstants.default_data_converter
      }
      # We first compare and remove the following values because these objects have issues
      # when we sort the values array below
      full_options.should == expected
    end

    it "should return the values passed in" do
      options = AWS::Flow::WorkflowOptions.new({
        task_list: "test_tasklist",
        version: "1.0",
        prefix_name: "FooWorkflow",
        tag_list: ["tag1", "tag2"]
      })
      full_options = options.get_full_options
      expected = {
        task_list: "test_tasklist",
        version: "1.0",
        prefix_name: "FooWorkflow",
        tag_list: ["tag1", "tag2"],
        data_converter: FlowConstants.default_data_converter,
      }
      # We first compare and remove the following values because these objects have issues
      # when we sort the values array below
      [:data_converter, :tag_list].each { |x| full_options.delete(x).should == expected.delete(x) }
      full_options.keys.sort.should == expected.keys.sort
      full_options.values.sort.should == expected.values.sort
    end

  end

end

describe AWS::Flow::WorkflowRegistrationDefaults do

  context "#defaults" do

    it "should return the correct default values" do
      defaults = AWS::Flow::WorkflowRegistrationDefaults.new
      defaults.data_converter.should == AWS::Flow::FlowConstants.default_data_converter
      defaults.default_task_start_to_close_timeout.should == 30
      defaults.default_child_policy.should == "TERMINATE"
      defaults.tag_list.should == []
      defaults.default_task_list.should == AWS::Flow::FlowConstants.use_worker_task_list
      defaults.default_task_priority == "0"
    end

  end

end

describe AWS::Flow::ActivityRegistrationDefaults do

  context "#defaults" do

    it "should return the correct default values" do
      defaults = AWS::Flow::ActivityRegistrationDefaults.new
      defaults.data_converter.should == AWS::Flow::FlowConstants.default_data_converter
      defaults.default_task_schedule_to_start_timeout.should == Float::INFINITY
      defaults.default_task_schedule_to_close_timeout.should == Float::INFINITY
      defaults.default_task_start_to_close_timeout.should == Float::INFINITY
      defaults.default_task_heartbeat_timeout.should == Float::INFINITY
      defaults.default_task_priority == "0"
      defaults.default_task_list.should == AWS::Flow::FlowConstants.use_worker_task_list
    end

  end

end


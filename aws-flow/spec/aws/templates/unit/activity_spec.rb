require 'spec_helper'

describe AWS::Flow::Templates::ActivityTemplate do

  context "#activity" do

    it "creates an ActivityTemplate with correct name and options" do
      template = AWS::Flow::Templates::activity("ActivityClass.my_activity")
      template.should be_kind_of(AWS::Flow::Templates::ActivityTemplate)
      template.name.should == "my_activity"
      template.options[:prefix_name].should == "ActivityClass"
    end

  end

  context "#initialize" do

    it "assigns activity name and default options correctly" do
      template = AWS::Flow::Templates::ActivityTemplate.new("ActivityClass.my_activity")
      template.name.should == "my_activity"
      template.options[:version].should == "1.0"
      template.options[:prefix_name].should == "ActivityClass"
      template.options[:data_converter].should be_kind_of(FlowConstants.data_converter.class)
    end

    it "raises if full activity name is not given" do
      expect{AWS::Flow::Templates::ActivityTemplate.new("ActivityClass")}.to raise_error
    end

    it "ignores irrelevant activity options" do
      options = {
        foo: "asdf"
      }
      template = AWS::Flow::Templates::ActivityTemplate.new("ActivityClass.my_activity", options)
      template.name.should == "my_activity"
      template.options.should_not include(:foo)
    end

    it "overrides default options correctly" do
      options = {
        exponential_retry: {
          maximum_attempts: 3
        },
        version: "2.0",
        task_list: "foo_tasklist"
      }
      template = AWS::Flow::Templates::ActivityTemplate.new("ActivityClass.my_activity", options)
      template.name.should == "my_activity"
      template.options[:version].should == "2.0"
      template.options[:task_list].should == "foo_tasklist"
      template.options[:exponential_retry].should == { maximum_attempts: 3 }

      template.options[:prefix_name].should == "ActivityClass"
      template.options[:data_converter].should be_kind_of(FlowConstants.data_converter.class)
    end

  end

  context "#run" do

    it "ensures run method calls the context" do
      template = AWS::Flow::Templates::activity("ActivityClass.my_activity")
      input = { input: "foo" }

      context = double
      expect(context).to receive(:act_client).and_return(context)
      expect(context).to receive(:my_activity).with(input)

      template.run(input, context)
    end

    it "ensures activity is scheduled on the correct tasklist" do
      template = AWS::Flow::Templates::activity("ActivityClass.my_activity")
      input = { input: "foo", task_list: "bar" }

      context = double
      expect(context).to receive(:act_client).and_return(context)
      expect(context).to receive(:my_activity).with(input)
      # Couldn't find a better way to test this because internally the options
      # hash is wrapped in a block and passed to the activity client.
      expect_any_instance_of(Hash).to receive(:merge!).with({task_list: "bar"})

      template.run(input, context)
    end

  end

end

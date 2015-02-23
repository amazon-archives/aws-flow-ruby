require 'spec_helper'

describe AWS::Flow::Templates::Utils do

  context "#register_on_failure" do

    it "calls the register_default methods on failure and calls the block again" do

      block = double
      expect(block).to receive(:test).once.and_raise(AWS::SimpleWorkflow::Errors::UnknownResourceFault)
      expect(block).to receive(:test).once

      expect(AWS::Flow::Templates::Utils).to receive(:register_defaults).with("foo")
      AWS::Flow::Templates::Utils.register_on_failure("foo") { block.test }

    end

    it "calls the block once if no failure" do

      block = double
      expect(block).to receive(:test).once

      AWS::Flow::Templates::Utils.register_on_failure("foo") { block.test }
    end

  end

  context "#register_defaults" do

    it "registers default workflow and activity in default domain" do
      expect(AWS::Flow::Utilities).to receive(:register_domain) do |x|
        x.should == FlowConstants.defaults[:domain]
      end

      expect(AWS::Flow::Templates::Utils).to receive(:register_default_workflow)
      expect(AWS::Flow::Templates::Utils).to receive(:register_default_result_activity)
      AWS::Flow::Templates::Utils.register_defaults
    end

    it "registers default workflow and activity in a non default domain" do
      expect(AWS::Flow::Utilities).to receive(:register_domain) do |x|
        x.should == "foo"
      end

      expect(AWS::Flow::Templates::Utils).to receive(:register_default_workflow)
      expect(AWS::Flow::Templates::Utils).to receive(:register_default_result_activity)
      AWS::Flow::Templates::Utils.register_defaults("foo")
    end

  end

  context "#register_default_workflow" do

    it "registers the default workflow" do
      domain = double
      allow(domain).to receive(:client).and_return(domain)
      expect_any_instance_of(AWS::Flow::WorkflowWorker).to receive(:add_implementation) do |k|
        k.should == AWS::Flow::Templates.const_get("#{FlowConstants.defaults[:prefix_name]}")
      end
      expect_any_instance_of(AWS::Flow::WorkflowWorker).to receive(:register)
      AWS::Flow::Templates::Utils.register_default_workflow(domain)
    end

  end

  context "#register_default_result_activity" do

    it "registers the default result activity" do
      domain = double
      allow(domain).to receive(:client).and_return(domain)
      expect_any_instance_of(AWS::Flow::ActivityWorker).to receive(:add_implementation) do |k|
        k.should == AWS::Flow::Templates.const_get("#{FlowConstants.defaults[:result_activity_prefix]}")
      end
      expect_any_instance_of(AWS::Flow::ActivityWorker).to receive(:register)
      AWS::Flow::Templates::Utils.register_default_result_activity(domain)
    end

  end

end

require 'spec_helper'

describe AWS::Flow::Templates::RootTemplate do

  context "#root" do

    it "initializes the RootTemplate with correct step and result_step" do
      template = AWS::Flow::Templates.root("foo")
      template.should be_kind_of(AWS::Flow::Templates::RootTemplate)
      template.step.should == "foo"
      template.result_step.should be_nil

      template = AWS::Flow::Templates.root("foo", "bar")
      template.should be_kind_of(AWS::Flow::Templates::RootTemplate)
      template.step.should == "foo"
      template.result_step.should == "bar"
    end

  end

  context "#run" do

    let(:step) { double }
    let(:result_step) { double }

    it "runs the step" do
      expect(step).to receive(:run).with("input", "context")
      template = AWS::Flow::Templates.root(step)
      template.run("input", "context")
    end

    it "returns the result if result_step is nil" do
      expect(step).to receive(:run).with("input", "context").and_return("result")
      expect(result_step).not_to receive(:run)

      template = AWS::Flow::Templates.root(step)
      template.run("input", "context").should == "result"
    end

    it "calls the result_step if result_step is not nil" do
      expect(step).to receive(:run).with("input", "context").and_return("result")
      expect(result_step).to receive(:run).with("result", "context")

      template = AWS::Flow::Templates.root(step, result_step)
      template.run("input", "context").should == "result"
   end

    it "catches exceptions and calls result_step if result_step is not nil" do
      expect(step).to receive(:run).with("input", "context") do
        raise "test"
      end
      expect(result_step).to receive(:run) do |input, context|
        input.should include(:failure)
        input[:failure].should be_kind_of(RuntimeError)
        context.should == "context"
      end

      expect { AWS::Flow::Templates.root(step, result_step).run("input", "context") }.to raise_error
    end

    it "catches exceptions and doesn't call result_step if result_step is nil" do
      expect(step).to receive(:run).with("input", "context") do
        raise "test"
      end
      expect(result_step).not_to receive(:run)
      expect { AWS::Flow::Templates.root(step, result_step).run("input", "context") }.to raise_error
    end

  end

end

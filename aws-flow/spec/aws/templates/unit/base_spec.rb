require 'spec_helper'
include AWS::Flow::Templates

describe RootTemplate do

  context "#root" do

    it "initializes the RootTemplate with correct step and result_step" do
      template = root("foo")
      template.should be_kind_of(RootTemplate)
      template.step.should == "foo"
      template.result_step.should be_nil

      template = root("foo", "bar")
      template.should be_kind_of(RootTemplate)
      template.step.should == "foo"
      template.result_step.should == "bar"
    end

  end

  context "#run" do

    let(:step) { double }
    let(:result_step) { double }

    it "runs the step" do
      expect(step).to receive(:run).with("input", "context")
      template = root(step)
      template.run("input", "context")
    end

    it "returns the result if result_step is nil" do
      expect(step).to receive(:run).with("input", "context").and_return("result")
      expect(result_step).not_to receive(:run)

      template = root(step)
      template.run("input", "context").should == "result"
    end

    it "calls the result_step if result_step is not nil" do
      expect(step).to receive(:run).with("input", "context").and_return("result")
      expect(result_step).to receive(:run).with("result", "context")

      template = root(step, result_step)
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

      expect { root(step, result_step).run("input", "context") }.to raise_error
    end

    it "catches exceptions and doesn't call result_step if result_step is nil" do
      expect(step).to receive(:run).with("input", "context") do
        raise "test"
      end
      expect(result_step).not_to receive(:run)
      expect { root(step, result_step).run("input", "context") }.to raise_error
    end

  end

end

require 'spec_helper'

describe "AWS::Flow::Templates" do

  context "#start" do


    it "initializes result_step and calls get_result when get_result is true", focus: true do

      expect(AWS::Flow::Templates::ResultWorker).to receive(:start)
      expect(AWS::Flow::Templates::ResultWorker).to receive(:get_result_future)
      expect(AWS::Flow::Templates::Starter).to receive(:set_result_activity)

      options = {
        get_result: true
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end

  end
end

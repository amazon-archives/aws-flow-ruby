require 'spec_helper'

describe "AWS::Flow::Templates" do

  context "#start" do

    it "starts activity invocation with default options" do

      # Check all options being passed in
      expect(AWS::Flow).to receive(:start_workflow) do |input, options|
        input.should include(:definition)
        input[:definition].should be_kind_of(AWS::Flow::Templates::RootTemplate)
        activity = input[:definition].step
        activity.should be_kind_of(AWS::Flow::Templates::ActivityTemplate)
        activity.name.should == "hello"
        activity.options.should include(
          version: FlowConstants.defaults[:version],
          prefix_name: "HelloWorld",
        )
        input[:definition].result_step.should be_nil

        activity.options[:data_converter].should be_kind_of(FlowConstants.data_converter.class)

        input.should include(:args)
        input[:args].should include(:foo)

        options.should include(
          domain: FlowConstants.defaults[:domain],
          prefix_name: FlowConstants.defaults[:prefix_name],
          execution_method: FlowConstants.defaults[:execution_method],
          version: FlowConstants.defaults[:version],
          execution_start_to_close_timeout: FlowConstants.defaults[:execution_start_to_close_timeout],
          task_list: FlowConstants.defaults[:task_list],
          # Check if the tags are set correctly
          tag_list: ["HelloWorld.hello"]
        )
        options[:data_converter].should be_kind_of(FlowConstants.data_converter.class)

      end
      AWS::Flow::start("HelloWorld.hello", {foo: "Foo"})

    end

    it "starts activity invocation with overriden options" do

      expect(AWS::Flow).to receive(:start_workflow) do |input, options|

        activity = input[:definition].step
        # Check if the activity got the correct options
        activity.options.should include(
          version: "2.0",
          prefix_name: "HelloWorld",
          exponential_retry: {
            maximum_attempts: 10
          }
        )

        activity.options[:data_converter].should be_kind_of(FlowConstants.data_converter.class)

        options.should include(
          prefix_name: FlowConstants.defaults[:prefix_name],
          execution_method: FlowConstants.defaults[:execution_method],
          version: FlowConstants.defaults[:version],
          domain: FlowConstants.defaults[:domain],
          execution_start_to_close_timeout: 120,
          # Check if the tags are set correctly
          tag_list: ["HelloWorld.hello"]
        )

        options[:data_converter].should be_kind_of(FlowConstants.data_converter.class)

      end

      options = {
        execution_start_to_close_timeout: 120,
        version: "2.0",
        exponential_retry: { maximum_attempts: 10 }
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end

    it "doesn't muddle activity and workflow options" do

      expect(AWS::Flow).to receive(:start_workflow) do |input, options|

        activity = input[:definition].step
        # Check if the activity got the correct options
        activity.options.should include(
          version: "2.0",
          prefix_name: "HelloWorld",
          start_to_close_timeout: 10,
          data_converter: "Foo",
          exponential_retry: {
            maximum_attempts: 10
          }
        )

        options.should include(
          prefix_name: FlowConstants.defaults[:prefix_name],
          execution_method: FlowConstants.defaults[:execution_method],
          version: FlowConstants.defaults[:version],
          domain: FlowConstants.defaults[:domain],
          execution_start_to_close_timeout: 120,
          # Check if the tags are set correctly
          tag_list: ["HelloWorld.hello"],
          data_converter: "Foo"
        )

      end

      options = {
        start_to_close_timeout: 10,
        execution_start_to_close_timeout: 120,
        version: "2.0",
        exponential_retry: { maximum_attempts: 10 },
        child_policy: "TERMINATE",
        data_converter: "Foo"
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end

    it "doesn't initialize result_step when get_result is false" do

      expect(AWS::Flow).to receive(:start_workflow) do |input, options|
        input[:definition].result_step.should be_nil
      end

      expect(AWS::Flow::Templates::ResultWorker).to_not receive(:start)
      expect(AWS::Flow::Templates::ResultWorker).to_not receive(:get_result_future)
      expect(AWS::Flow::Templates::Starter).to_not receive(:set_result_activity)

      options = {
        get_result: false
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end

    it "initializes result_step and calls get_result when get_result is true" do

      expect(AWS::Flow::Templates::ResultWorker).to receive(:start)
      expect(AWS::Flow::Templates::ResultWorker).to receive(:get_result_future)
      expect(AWS::Flow::Templates::Starter).to receive(:set_result_activity)

      options = {
        get_result: true
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end

  end

  context "#set_result_activity" do

    it "sets the result step correctly and creates a new ExternalFuture" do
      root = double

      class AWS::Flow::Templates::ResultWorker
        class << self
          alias_method :start_copy, :start
          def start
            @results = SynchronizedHash.new
          end
        end
      end

      AWS::Flow::Templates::ResultWorker.start

      expect(SecureRandom).to receive(:uuid).and_return("foo")

      expect(root).to receive(:result_step=) do |x|
        x.is_a?(AWS::Flow::Templates::ResultActivityTemplate)
      end

      AWS::Flow::Templates::Starter.set_result_activity("task_list", root)

      result = AWS::Flow::Templates::ResultWorker.results["result_key: foo"]
      result.should_not be_nil
      result.should be_a(AWS::Flow::Core::ExternalFuture)

      class AWS::Flow::Templates::ResultWorker
        class << self
          alias_method :start_copy, :start
        end
      end

    end

  end

end

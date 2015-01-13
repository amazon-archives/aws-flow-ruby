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

    it "doesn't initialize result_step when wait is false" do

      expect(AWS::Flow).to receive(:start_workflow) do |input, options|
        input[:definition].result_step.should be_nil
      end

      options = {
        wait: false
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end

    it "initializes result_step and calls get_result when wait is true" do

      expect(AWS::Flow).to receive(:start_workflow) do |input, options|
        input[:definition].result_step.should_not be_nil
      end

      expect(AWS::Flow::Templates).to receive(:get_result)

      options = {
        wait: true
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end

    it "calls get_result with timeout value when wait & wait_timeout are set" do

      expect(AWS::Flow).to receive(:start_workflow) do |input, options|
        input[:definition].result_step.should_not be_nil
      end

      expect(AWS::Flow::Templates).to receive(:get_result) do |tasklist, domain, timeout|
        timeout.should == 10
      end

      options = {
        wait: true,
        wait_timeout: 10
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end


  end

  context "#get_result" do

    it "starts an activity worker and sets the future" do

      tasklist = "result_tasklist: foo"

      # Get the result activity class
      klass = AWS::Flow::Templates.result_activity
      instance = klass.new

      expect(klass).to receive(:new).and_return(instance)

      expect_any_instance_of(AWS::Flow::ActivityWorker).to receive(:add_implementation) do |k|
        k.class.should == AWS::Flow::Templates.const_get("#{FlowConstants.defaults[:result_activity_prefix]}")
        k.result.should be_kind_of(AWS::Flow::Core::Future)
      end

      expect_any_instance_of(AWS::Flow::ActivityWorker).to receive(:run_once) do
        # Manually call the activity
        instance.send(FlowConstants.defaults[:result_activity_method].to_sym, {name: "foo"} )
      end

      # Call get_result and check that the result is set
      result = AWS::Flow::Templates.get_result(tasklist, "domain")

      result.should include(name: "foo")

    end

    it "times out correctly" do

      tasklist = "result_tasklist: foo"

      # Get the result activity class
      klass = AWS::Flow::Templates.result_activity
      instance = klass.new

      expect(klass).to receive(:new).and_return(instance)

      expect_any_instance_of(AWS::Flow::ActivityWorker).to receive(:run_once) do
        # Manually call the activity
        sleep 5
        instance.send(FlowConstants.defaults[:result_activity_method].to_sym, {name: "foo"} )
      end

      # Call get_result and check that the result is set
      result = AWS::Flow::Templates.get_result(tasklist, "domain", 1)
      result.should be_nil

    end


  end

  context "#register_default_domain" do

    it "registers the default domain" do
      expect(AWS::Flow::Utilities).to receive(:register_domain) do |name|
        name.should == FlowConstants.defaults[:domain]
      end
      AWS::Flow::Templates.register_default_domain
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
      AWS::Flow::Templates.register_default_workflow(domain)
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
      AWS::Flow::Templates.register_default_result_activity(domain)
    end

  end

end

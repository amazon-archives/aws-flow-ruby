require_relative 'setup'

describe "AWS::Flow" do

  context "#start" do

    it "starts activity invocation with default options" do

      # Check all options being passed in
      expect(AWS::Flow).to receive(:start_workflow) do |input, options|
        input.should include(:root)
        input[:root].should be_kind_of(AWS::Flow::Templates::RootTemplate)
        activity = input[:root].step
        activity.should be_kind_of(AWS::Flow::Templates::ActivityTemplate)
        activity.name.should == "hello"
        activity.options.should include(
          version: FlowConstants.defaults[:version],
          prefix_name: "HelloWorld",
        )
        input[:root].result_step.should be_nil

        activity.options[:data_converter].should be_kind_of(FlowConstants.data_converter.class)

        input.should include(:input)
        input[:input].should include(:foo)

        options.should include(
          prefix_name: FlowConstants.defaults[:prefix_name],
          execution_method: FlowConstants.defaults[:execution_method],
          version: FlowConstants.defaults[:version],
          domain: FlowConstants.defaults[:domain],
          execution_start_to_close_timeout: FlowConstants.defaults[:execution_start_to_close_timeout],
          # Check if the tags are set correctly
          tag_list: ["HelloWorld.hello"]
        )
        options[:data_converter].should be_kind_of(FlowConstants.data_converter.class)

      end
      AWS::Flow::start("HelloWorld.hello", {foo: "Foo"})

    end

    it "starts activity invocation with overriden options" do

      expect(AWS::Flow).to receive(:start_workflow) do |input, options|

        activity = input[:root].step
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

        activity = input[:root].step
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

    # Fix the next two tests.
    it "doesn't initialize result_step when wait_for_result is false" do

      expect(AWS::Flow).to receive(:start_workflow) do |input, options|
        input[:root].result_step.should be_nil
      end

      options = {
        wait_for_result: false
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end

    it "initializes result_step and calls get_result when wait_for_result is true" do

      expect(AWS::Flow).to receive(:start_workflow) do |input, options|
        input[:root].result_step.should_not be_nil
      end

      expect(AWS::Flow::Templates).to receive(:get_result)

      options = {
        wait_for_result: true
      }

      AWS::Flow::start("HelloWorld.hello", {input: "input"}, options)

    end

  end

  context "#get_result" do

    it "starts an activity worker and sets the future" do

      tasklist = "result_tasklist: foo"

      # Create the mocks
      domain = double
      task = double

      # Get the result activity class
      klass = AWS::Flow::Templates.result_activity(tasklist.split(":")[1].strip)
      # Make sure the activity method doesn't exist yet
      klass.instance_methods(false).should be_empty

      expect(AWS::Flow::Runner).to receive(:setup_domain).and_return(domain)
      expect(domain).to receive(:client)

      expect_any_instance_of(AWS::Flow::ActivityWorker).to receive(:register)
      expect_any_instance_of(AWS::Flow::ActivityWorker).to receive(:add_implementation) do |k|
        k.should == AWS::Flow::Templates.const_get("#{FlowConstants.defaults[:result_activity_prefix]}foo")
      end

      expect_any_instance_of(AWS::Flow::ActivityWorker).to receive(:run_once) do
        klass = AWS::Flow::Templates.const_get("#{FlowConstants.defaults[:result_activity_prefix]}foo")
        # Manuall call the activity
        klass.new.send(FlowConstants.defaults[:result_activity_method].to_sym, {name: "foo"} )
      end

      # Call get_result and check that the result is set
      result = AWS::Flow::Templates.get_result(tasklist, "domain")
      klass.instance_methods(false).should include(FlowConstants.defaults[:result_activity_method].to_sym)

      result.should include(name: "foo")

    end

  end

  context "#start_workflow" do

    it "sets up the domain correctly" do

      domain = double
      client = double
      expect(domain).to receive(:client).and_return(client)
      AWS::Flow.stub(:workflow_client).and_return(client)
      expect(client).to receive(:start_execution)

      expect(AWS::Flow::Runner).to receive(:setup_domain) do |input|
        input.should be_a(Hash)
        input['domain']['name'].should == "foo"
        domain
      end

      options = { domain: "foo", from_class: "foo" }
      AWS::Flow::start_workflow(nil, "input", options)

    end

    it "raises error if domain is not provided" do

      options = { from_class: "FooWorkflow" }
      expect{AWS::Flow::start_workflow(nil, "input", options)}.to raise_error(ArgumentError)

    end

    it "raises error if workflow type or from_class is not provided" do

      domain = double
      AWS::Flow::Runner.stub(:setup_domain).and_return(domain)

      options = { domain: "foo", prefix_name: "FooWorkflow" }
      expect{AWS::Flow::start_workflow(nil, "input", options)}.to raise_error(ArgumentError)
      options.merge!(execution_method: "foo")
      expect{AWS::Flow::start_workflow(nil, "input", options)}.to raise_error(ArgumentError)

      expect(domain).to receive(:client).and_return("client")

      wf_client = double
      expect(wf_client).to receive(:start_execution)

      expect(AWS::Flow).to receive(:workflow_client) do |client, domain, &block|
        wf_client
      end

      options.merge!(version: "1.0")
      expect{AWS::Flow::start_workflow(nil, "input", options)}.not_to raise_error

      expect(domain).to receive(:client).and_return("client)")
      expect(wf_client).to receive(:start_execution)

      expect(AWS::Flow).to receive(:workflow_client) do |client, domain, &block|
        wf_client
      end

      options = { domain: "foo", from_class: "foo" }
      expect{AWS::Flow::start_workflow(nil, "input", options)}.not_to raise_error

    end

    it "starts a workflow with overriden options" do

      domain = double
      AWS::Flow::Runner.stub(:setup_domain).and_return(domain)
      expect(domain).to receive(:client).and_return("client")
      wf_client = double
      expect(wf_client).to receive(:start_execution)

      expect(AWS::Flow).to receive(:workflow_client) do |client, domain, &block|
        options = block.call
        options.should include(
          prefix_name: "HelloWorld",
          execution_method: "hello",
          version: "2.0",
          execution_start_to_close_timeout: 120,
        )
        wf_client
      end

      options = {
        domain: "FooDomain",
        execution_start_to_close_timeout: 120,
        version: "2.0",
      }

      AWS::Flow::start_workflow("HelloWorld.hello", "input", options)

    end

    it "starts a regular workflow execution with correct options" do

      domain = double
      AWS::Flow::Runner.stub(:setup_domain).and_return(domain)
      expect(domain).to receive(:client).and_return("client")
      wf_client = double
      expect(wf_client).to receive(:start_execution)

      expect(AWS::Flow).to receive(:workflow_client) do |client, domain, &block|
        options = block.call

        options.should include(
          prefix_name: "StarterTestWorkflow",
          execution_method: "start"
        )
        wf_client
      end

      AWS::Flow::start_workflow("StarterTestWorkflow", "input", {
        domain: "test",
        execution_method: "start",
        version: "1.0"
      })

    end

    it "starts a workflow execution with all options in options hash" do

      domain = double
      AWS::Flow::Runner.stub(:setup_domain).and_return(domain)
      expect(domain).to receive(:client).and_return("client")
      wf_client = double
      expect(wf_client).to receive(:start_execution)

      expect(AWS::Flow).to receive(:workflow_client) do |client, domain, &block|
        options = block.call

        options.should include(
          prefix_name: "FooWorkflow",
          execution_method: "start",
          version: "1.0"
        )
        wf_client
      end

      options = { prefix_name: "FooWorkflow", domain: "Foo", version: "1.0", execution_method: "start" }
      AWS::Flow::start_workflow(nil, "input", options)

    end


    it "starts workflow with from_options option correctly" do

      domain = double
      AWS::Flow::Runner.stub(:setup_domain).and_return(domain)
      expect(domain).to receive(:client).and_return("client")
      wf_client = double
      expect(wf_client).to receive(:start_execution)

      class FooWorkflow; extend AWS::Flow::Workflows; end

      expect(AWS::Flow).to receive(:workflow_client) do |client, domain, &block|
        options = block.call
        options.should include(from_class: "FooWorkflow")
        wf_client
      end

      options = { domain: "Foo", from_class: "FooWorkflow" }
      AWS::Flow::start_workflow(nil, "input", options)

    end

  end

end

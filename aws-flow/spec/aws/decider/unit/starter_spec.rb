require_relative 'setup'

describe "AWS::Flow" do

  context "#start" do

    it "calls AWS::Flow::Templates#start" do
      AWS::Flow::stub(:start_workflow)
      expect(AWS::Flow::Templates).to receive(:start)
      AWS::Flow::start("HelloWorld.hello", { foo: "foo" })
    end

  end

  context "#start_workflow" do

    it "raises error if domain is not provided" do

      options = { from_class: "FooWorkflow" }
      expect{AWS::Flow::start_workflow(nil, "input", options)}.to raise_error(ArgumentError)

    end

    it "raises error if workflow type or from_class is not provided" do

      domain = double
      AWS::Flow::Utilities.stub(:register_domain).and_return(domain)

      options = { domain: "foo", prefix_name: "FooWorkflow" }
      expect{AWS::Flow::start_workflow(nil, "input", options)}.to raise_error(ArgumentError)
      options.merge!(execution_method: "foo")
      expect{AWS::Flow::start_workflow(nil, "input", options)}.to raise_error(ArgumentError)

      wf_client = double
      expect(wf_client).to receive(:start_execution)

      expect(AWS::Flow).to receive(:workflow_client) do |client, domain, &block|
        wf_client
      end

      options.merge!(version: "1.0")
      expect{AWS::Flow::start_workflow(nil, "input", options)}.not_to raise_error

      expect(wf_client).to receive(:start_execution)

      expect(AWS::Flow).to receive(:workflow_client) do |client, domain, &block|
        wf_client
      end

      options = { domain: "foo", from_class: "foo" }
      expect{AWS::Flow::start_workflow(nil, "input", options)}.not_to raise_error

    end

    it "starts a workflow with overriden options" do

      domain = double
      AWS::Flow::Utilities.stub(:register_domain).and_return(domain)
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
      AWS::Flow::Utilities.stub(:register_domain).and_return(domain)
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
      AWS::Flow::Utilities.stub(:register_domain).and_return(domain)
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
      AWS::Flow::Utilities.stub(:register_domain).and_return(domain)
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

require_relative 'setup'

describe "AWS::Flow" do

  before(:all) do
    @domain = get_test_domain
    @domain.workflow_executions.each { |x| x.terminate }
    puts @domain.inspect
  end

  context "#start" do

    it "starts default workflow with the correct activity type" do
      AWS::Flow::start("StarterTestActivity.foo", {input: "Hello"}, {domain: @domain.name})

      until @domain.workflow_executions.count.count > 0
        sleep 2
      end

      @domain.workflow_executions.each do |x|
        x.execution_start_to_close_timeout.should == FlowConstants.defaults[:execution_start_to_close_timeout].to_i
        x.workflow_type.name.should == "#{FlowConstants.defaults[:prefix_name]}.#{FlowConstants.defaults[:execution_method]}"
        x.workflow_type.version.should == "#{FlowConstants.defaults[:version]}"
        x.tags.should include('StarterTestActivity.foo')

        data_converter = FlowConstants.defaults[:data_converter]
        input = data_converter.load(x.events.first.attributes[:input]).first

        root = input[:definition]
        root.result_step.should be_nil
        root.should be_kind_of(AWS::Flow::Templates::RootTemplate)

        activity = root.step
        activity.should be_kind_of(AWS::Flow::Templates::ActivityTemplate)
        activity.name.should == "foo"
        activity.options.should include(
          version: "1.0",
          prefix_name: "StarterTestActivity",
          exponential_retry: {
            maximum_attempts: 3
          }
        )

        input[:args].should include(input: "Hello")
        x.terminate
      end

    end

    it "starts default workflow with the correct activity type with overriden options" do

      options = {
        execution_start_to_close_timeout: 100,
        task_list: "bar",
        version: "2.0",
        tag_list: ['overriden_test'],
        get_result: true,
        domain: @domain.name
      }

      future = AWS::Flow::start("StarterTestActivity.foo", {input: "Hello"}, options)

      until @domain.workflow_executions.count.count > 0
        sleep 2
      end

      @domain.workflow_executions.tagged("overriden_test").each do |x|
        x.execution_start_to_close_timeout.should == 100
        x.workflow_type.name.should == "#{FlowConstants.defaults[:prefix_name]}.#{FlowConstants.defaults[:execution_method]}"
        x.workflow_type.version.should == "#{FlowConstants.defaults[:version]}"
        x.tags.should include('overriden_test', 'StarterTestActivity.foo')

        data_converter = FlowConstants.defaults[:data_converter]
        attrs = x.events.first.attributes

        input = data_converter.load(x.events.first.attributes[:input]).first

        root = input[:definition]
        root.should be_kind_of(AWS::Flow::Templates::RootTemplate)
        root.result_step.should_not be_nil
        result = root.result_step
        result.should be_kind_of(AWS::Flow::Templates::ResultActivityTemplate)

        activity = root.step
        activity.should be_kind_of(AWS::Flow::Templates::ActivityTemplate)
        activity.name.should == "foo"
        activity.options.should include(
          version: "2.0",
          prefix_name: "StarterTestActivity",
          task_list: "bar",
          exponential_retry: {
            maximum_attempts: 3
          }
        )

        input[:args].should include(input: "Hello")
        x.terminate
      end

      Test::Integ.kill_executors

    end

  end

  context "#start_workflow" do
    before(:all) do
      class StartWorkflowTest
        extend AWS::Flow::Workflows
        workflow :start do
          {
            version: "1.0",
            default_task_list: "foo",
            default_execution_start_to_close_timeout: 60
          }
        end
      end
      AWS::Flow::WorkflowWorker.new(@domain.client, @domain, nil, StartWorkflowTest).register

    end

    it "starts a regular workflow correctly" do
      options = {
        version: "1.0",
        domain: @domain.name,
        execution_start_to_close_timeout: 100,
        tag_list: ["Test1"]
      }
      AWS::Flow::start_workflow("StartWorkflowTest.start", "some input", options)

      until @domain.workflow_executions.count.count > 0
        sleep 2
      end

      @domain.workflow_executions.tagged("Test1").each do |x|
        x.execution_start_to_close_timeout.should == 100
        x.workflow_type.name.should == "StartWorkflowTest.start"
        x.workflow_type.version.should == "1.0"

        data_converter = FlowConstants.defaults[:data_converter]
        input = data_converter.load(x.events.first.attributes[:input]).first
        input.should == "some input"

        x.terminate
      end

    end

    it "starts a workflow with type passed in through options" do
      options = {
        version: "1.0",
        prefix_name: "StartWorkflowTest",
        execution_method: "start",
        domain: @domain.name,
        execution_start_to_close_timeout: 100,
        tag_list: ["Test2"]
      }
      AWS::Flow::start_workflow(nil, "some input", options)

      until @domain.workflow_executions.count.count > 0
        sleep 2
      end

      @domain.workflow_executions.tagged("Test2").each do |x|
        x.execution_start_to_close_timeout.should == 100
        x.workflow_type.name.should == "StartWorkflowTest.start"
        x.workflow_type.version.should == "1.0"

        data_converter = FlowConstants.defaults[:data_converter]
        input = data_converter.load(x.events.first.attributes[:input]).first
        input.should == "some input"

        x.terminate
      end

    end

    it "starts workflow with from_options option correctly" do
      options = {
        from_class: "StartWorkflowTest",
        domain: @domain.name,
        tag_list: ["Test3"]
      }

      AWS::Flow::start_workflow(nil, "some input", options)

      until @domain.workflow_executions.count.count > 0

        sleep 2
      end

      @domain.workflow_executions.tagged("Test3").each do |x|
        x.execution_start_to_close_timeout.should == 60
        x.workflow_type.name.should == "StartWorkflowTest.start"
        x.workflow_type.version.should == "1.0"

        data_converter = FlowConstants.defaults[:data_converter]
        input = data_converter.load(x.events.first.attributes[:input]).first
        input.should == "some input"

        x.terminate
      end

    end

  end

end

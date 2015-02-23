require 'spec_helper'

describe Templates do

  context "#default_workflow" do

    let(:klass) { AWS::Flow::Templates.default_workflow}

    it "correctly creates a Ruby Flow default workflow" do
      klass.should be_kind_of(AWS::Flow::Workflows)
      klass.workflows.size.should == 1
      klass.workflows.first.name.should == "#{FlowConstants.defaults[:prefix_name]}"\
        ".#{FlowConstants.defaults[:execution_method]}"
      klass.workflows.first.version.should == FlowConstants.defaults[:version]
    end

    it "creates the necessary workflow and client methods" do
      klass.instance_methods(false).should include(:act_client)
      klass.instance_methods(false).should include(:child_client)
      klass.instance_methods(false).should include(:start)
    end

    it "sets workflow options correctly" do
      options = klass.workflows.first.options
      options.execution_method.should == FlowConstants.defaults[:execution_method]
      options.prefix_name.should == FlowConstants.defaults[:prefix_name]
      options.version.should == FlowConstants.defaults[:version]
    end

    it "doesn't throw an exception if a default workflow already exists" do
      expect{AWS::Flow::Templates.default_workflow}.not_to raise_error
    end

    context "#start" do

      let(:obj) { klass.new }

      it "checks workflow input correctly" do
        expect{obj.start("some_input")}.to raise_error(ArgumentError)
        expect{obj.start(args: "some_input")}.to raise_error(ArgumentError)
        expect{obj.start(definition: "some_input")}.to raise_error(ArgumentError)
      end

      it "runs template correctly" do
        template = double
        expect(template).to receive(:run).with({input: "some_input"}, an_instance_of(klass))
        expect(template).to receive(:is_a?).and_return(true)
        expect{obj.start(
          args: {input: "some_input"},
          definition: template
        )}.not_to raise_error
      end

    end
  end

  context "#make_activity_class" do

    before(:all) do
      class FooClass
        def foo; end
        def bar; end
      end
    end

    it "returns input as-is if it is nil" do
      AWS::Flow::Templates.make_activity_class(nil).should be_nil
    end

    it "correctly creates a proxy Activity class" do
      klass = AWS::Flow::Templates.make_activity_class(FooClass)
      klass.should be_kind_of(AWS::Flow::Activities)
      klass.should == AWS::Flow::Templates::ActivityProxies.const_get("FooClassProxy")
      klass.new.instance.should be_kind_of(FooClass)
      activities = klass.activities.map(&:name).map { |x| x.split('.').last.to_sym }
      activities.should include(*FooClass.instance_methods(false))
    end

    it "correctly converts the ruby class contained in a module into an Activity class" do
      Object.const_set("FooModule", Module.new)
      klass = FooModule.const_set("FooClass1", Class.new(Object))
      klass = AWS::Flow::Templates.make_activity_class(klass)
      klass.should be_kind_of(AWS::Flow::Activities)
      klass.should == AWS::Flow::Templates::ActivityProxies.const_get("FooClass1Proxy")
    end

    it "correctly converts instance methods to activities and assigns options" do
      klass = Object.const_set("BarClass", Class.new(Object) { def foo_method; end })
      klass = AWS::Flow::Templates.make_activity_class(klass)
      klass.activities.first.name.should == "BarClass.foo_method"
      opts = klass.activities.first.options
      opts.version.should == "1.0"
      opts.prefix_name.should == "BarClass"

      Object.const_set("FooModule1", Module.new)
      klass = FooModule1.const_set("FooClass2", Class.new(Object) { def foo_method; end })
      klass = AWS::Flow::Templates.make_activity_class(klass)
      klass.activities.first.name.should == "FooClass2.foo_method"
      opts = klass.activities.first.options
      opts.version.should == "1.0"
      opts.prefix_name.should == "FooClass2"
    end

    it "passes the messages to the proxy instance methods" do
      klass = AWS::Flow::Templates.make_activity_class(FooClass)
      expect_any_instance_of(FooClass).to receive(:foo)
      expect_any_instance_of(FooClass).to receive(:bar)
      klass.new.foo
      klass.new.bar
    end

  end

  context "#result_activity" do

    let(:klass) { AWS::Flow::Templates.result_activity }

    it "correctly returns the FlowDefaultResultActivityRuby class" do
      klass.should be_kind_of(AWS::Flow::Activities)
      klass.activities.size.should == 1
      klass.name.should == "AWS::Flow::Templates::FlowDefaultResultActivityRuby"
      klass.activities.first.name.should == "#{FlowConstants.defaults[:result_activity_prefix]}"\
        ".#{FlowConstants.defaults[:result_activity_method]}"
      klass.activities.first.version.should == FlowConstants.defaults[:version]
      klass.instance_methods(false).should include(:run)
    end

    context "#run" do
      let(:inst) { klass.new }
      let(:result) { { key: "123", result: "asdf" } }

      it "raises if key or result is not present" do
        expect{inst.run("adsf")}.to raise_error(ArgumentError)
      end

      it "passes if key and result are present" do
        expect{inst.run(result)}.not_to raise_error
      end

      it "uses the writer to send marshalled result" do
        writer = double

        expect(writer).to receive(:puts) do |x|
          Marshal.load(x).should == result
        end

        inst = klass.new(writer)
        inst.run(result)
      end
    end

    it "doesn't throw an exception if a default activity class already exists" do
      expect{AWS::Flow::Templates.result_activity}.not_to raise_error
    end

  end

end

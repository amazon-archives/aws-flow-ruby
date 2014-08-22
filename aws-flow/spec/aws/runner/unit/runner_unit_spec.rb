require 'spec_helper'
require 'tempfile'
require 'socket'
require 'fileutils'

describe "Runner" do
  
  describe "Command line" do
    it "makes sure that the JSON file must be provided on the command line" do
      expect { AWS::Flow::Runner.parse_command_line([]) }.to raise_error( OptionParser::MissingArgument )
    end

    it "makes sure that the JSON file must be provided on the command line (switch must be followed by argument)" do
      expect { AWS::Flow::Runner.parse_command_line(["-f"]) }.to raise_error( OptionParser::MissingArgument )
    end

    it "makes sure that the JSON file must be provided on the command line (switch must be followed by argument which is valid file; valid case)" do
      file = Tempfile.new('foo')
      begin
        expect { AWS::Flow::Runner.parse_command_line(["-f", file.path]) }.not_to raise_error
      ensure
        file.unlink
      end
    end

  end

  describe "JSON loading" do

    it "makes sure that the JSON file exists" do
      file = Tempfile.new('foo')
      path = file.path
      file.unlink
      expect { AWS::Flow::Runner.load_config_json(path) }.to raise_error(ArgumentError)
    end

    it "makes sure that the JSON file has valid content" do
      file = Tempfile.new('foo')
      begin
        File.write(file, "garbage{")
        expect { AWS::Flow::Runner.load_config_json(file.path) }.to raise_error(JSON::ParserError)
      ensure
        file.unlink
      end
    end

  end


  describe "JSON validation" do

    it "makes sure activity classes are provided (empty list)" do
      document = '{
        "activity_paths": [],
        "activity_workers": [
          {
            "task_list": "bar",
            "activity_classes": [],
            "number_of_workers": 3
          }
        ]
      }'
      js = JSON.parse(document)

      # just in case so we don't start chid processes
      AWS::Flow::Runner.stub(:fork)

      # make sure the error is thrown
      expect {
        AWS::Flow::Runner.start_activity_workers(AWS::SimpleWorkflow.new, js)
      }.to raise_error(ArgumentError)

    end

    it "makes sure activity classes are provided (no list)" do
      document = '{
        "activity_paths": [],
        "activity_workers": [
          {
            "task_list": "bar",
            "number_of_workers": 3
          }
        ]
      }'
      js = JSON.parse(document)

      # just in case so we don't start chid processes
      allow(AWS::Flow::Runner).to receive(:fork).and_return(42)

      # make sure the error is thrown
      expect {
        AWS::Flow::Runner.start_activity_workers(AWS::SimpleWorkflow.new, js)
      }.to raise_error(ArgumentError)

    end

    it "makes sure workflow classes are provided (empty list)" do
      document = '{
        "workflow_paths": [],
        "workflow_workers": [
          {
            "task_list": "bar",
            "workflow_classes": [],
            "number_of_workers": 3
          }
        ]
      }'
      js = JSON.parse(document)

      # just in case so we don't start chid processes
      AWS::Flow::Runner.stub(:fork)

      # make sure the error is thrown
      expect {
        AWS::Flow::Runner.start_workflow_workers(AWS::SimpleWorkflow.new, js)
      }.to raise_error(ArgumentError)

    end

    it "makes sure workflow classes are provided (no list)" do
      document = '{
        "workflow_paths": [],
        "workflow_workers": [
          {
            "task_list": "bar",
            "number_of_workers": 3
          }
        ]
      }'
      js = JSON.parse(document)

      # just in case so we don't start chid processes
      allow(AWS::Flow::Runner).to receive(:fork).and_return(42)

      # make sure the error is thrown
      expect {
        AWS::Flow::Runner.start_workflow_workers(AWS::SimpleWorkflow.new, js)
      }.to raise_error(ArgumentError)

    end

  end

  describe "Starting workers" do

    def workflow_js
      document = '{
        "workflow_paths": [],
        "workflow_workers": [
          {
            "task_list": "bar",
            "workflow_classes": [ "Object", "String" ],
            "number_of_workers": 3
          }
        ]
      }'
      JSON.parse(document)
    end

    def activity_js
      document = '{
        "activity_paths": [],
        "activity_workers": [
          {
            "task_list": "bar",
            "activity_classes": [ "Object", "String" ],
            "number_of_workers": 3
          }
        ]
      }'
      JSON.parse(document)
    end

    it "makes sure the number of workflow workers is correct" do
      # mock out a few methods to focus on the fact that the workers were created
      allow_any_instance_of(AWS::Flow::WorkflowWorker).to receive(:add_implementation).and_return(nil)
      allow_any_instance_of(AWS::Flow::WorkflowWorker).to receive(:start).and_return(nil)

      AWS::Flow::Runner.stub(:setup_domain)
      AWS::Flow::Runner.stub(:load_files)

      # what we are testing:
      expect(AWS::Flow::Runner).to receive(:fork).exactly(3).times

      # start the workers 
      workers = AWS::Flow::Runner.start_workflow_workers(AWS::SimpleWorkflow.new, "", workflow_js)
    end



    it "makes sure the number of activity workers is correct" do
      # mock out a few methods to focus on the fact that the workers were created
      allow_any_instance_of(AWS::Flow::ActivityWorker).to receive(:add_implementation).and_return(nil)
      allow_any_instance_of(AWS::Flow::ActivityWorker).to receive(:start).and_return(nil)
      AWS::Flow::Runner.stub(:setup_domain)
      AWS::Flow::Runner.stub(:load_files)

      # what we are testing:
      expect(AWS::Flow::Runner).to receive(:fork).exactly(3).times

      # start the workers 
      workers = AWS::Flow::Runner.start_activity_workers(AWS::SimpleWorkflow.new, "",activity_js)
    end
    
    it "makes sure the workflow implementation classes are added" do
      # mock out a few methods to focus on the implementations being added
      allow_any_instance_of(AWS::Flow::WorkflowWorker).to receive(:start).and_return(nil)
      AWS::Flow::Runner.stub(:fork)
      AWS::Flow::Runner.stub(:load_files)
      AWS::Flow::Runner.stub(:setup_domain)
      
      # stub that we can query later
      implems = []
      AWS::Flow::WorkflowWorker.any_instance.stub(:add_implementation) do |arg|
        implems << arg
      end

      # start the workers 
      workers = AWS::Flow::Runner.start_workflow_workers(AWS::SimpleWorkflow.new, "",workflow_js)

      # validate
      expect(implems).to include(Object.const_get("Object"), Object.const_get("String"))
    end

    it "makes sure the activity implementation classes are added" do
      # mock out a few methods to focus on the implementations being added
      allow_any_instance_of(AWS::Flow::ActivityWorker).to receive(:start).and_return(nil)
      AWS::Flow::Runner.stub(:fork)
      AWS::Flow::Runner.stub(:load_files)
      AWS::Flow::Runner.stub(:setup_domain)
      
      # stub that we can query later
      implems = []
      AWS::Flow::ActivityWorker.any_instance.stub(:add_implementation) do |arg|
        implems << arg
      end

      # start the workers 
      workers = AWS::Flow::Runner.start_activity_workers(AWS::SimpleWorkflow.new, "",activity_js)

      # validate
      expect(implems).to include(Object.const_get("Object"), Object.const_get("String"))
    end

    it "makes sure the workflow worker is started" do
      # mock out a few methods to focus on the worker getting started
      allow_any_instance_of(AWS::Flow::WorkflowWorker).to receive(:add_implementation).and_return(nil)
      AWS::Flow::Runner.stub(:fork).and_yield
      AWS::Flow::Runner.stub(:load_files)
      AWS::Flow::Runner.stub(:setup_domain)
      
      # stub that we can query later
      starts = 0
      AWS::Flow::WorkflowWorker.any_instance.stub(:start) do |arg|
        starts += 1
      end

      # start the workers 
      workers = AWS::Flow::Runner.start_workflow_workers(AWS::SimpleWorkflow.new, "",workflow_js)

      # validate
      expect(starts).to equal(3)
    end

    it "makes sure the activity worker is started" do
      # mock out a few methods to focus on the worker getting started
      allow_any_instance_of(AWS::Flow::ActivityWorker).to receive(:add_implementation).and_return(nil)
      AWS::Flow::Runner.stub(:fork).and_yield
      AWS::Flow::Runner.stub(:load_files)
      AWS::Flow::Runner.stub(:setup_domain)
      
      # stub that we can query later
      starts = 0
      AWS::Flow::ActivityWorker.any_instance.stub(:start) do |arg|
        starts += 1
      end

      # start the workers 
      workers = AWS::Flow::Runner.start_activity_workers(AWS::SimpleWorkflow.new, "",activity_js)

      # validate
      expect(starts).to equal(3)
    end

  end



  describe "Loading files" do

    before(:each) do
      # let's pretend the files exist, so that loading proceeds
      allow(File).to receive(:exists?).and_return(true)
      # stubs to avoid running code that should not be run/covered in these tests
      AWS::Flow::Runner.stub(:add_implementations)
      AWS::Flow::Runner.stub(:spawn_and_start_workers)
    end

    it "looks in the directory where the config is and loads the specified default" do
      base = "/tmp/blahdir"
      relative = File.join('flow', 'activities.rb')
      
      expect(AWS::Flow::Runner).to receive(:require).with(File.join(base, relative))

      AWS::Flow::Runner.load_files( File.join(base, "blahconfig"), "", 
                                   {config_key: "any_key_name",
                                    default_file: relative})
    end

    it "loads the default only if needed" do
      base = "/tmp/blahdir"
      relative = File.join('flow', 'activities.rb')

      expect(AWS::Flow::Runner).to_not receive(:require).with(File.join(base, relative))
      expect(AWS::Flow::Runner).to receive(:require).with("foo")
      expect(AWS::Flow::Runner).to receive(:require).with("bar")

      AWS::Flow::Runner.load_files( File.join(base, "blahconfig"), 
                                    JSON.parse('{ "activity_paths": [ "foo", "bar"] }'), 
                                   {config_key: "activity_paths",
                                    default_file: relative})
    end

    it "loads the \"flow/activities.rb\" by default for activity worker" do
      def activity_js
        document = '{
        "activity_workers": [
          {
            "task_list": "bar",
            "activity_classes": [ "Object", "String" ],
            "number_of_workers": 3
          }
        ]
      }'
        JSON.parse(document)
      end

      AWS::Flow::Runner.stub(:setup_domain)
      expect(AWS::Flow::Runner).to receive(:require).with(File.join(".", "flow", "activities.rb"))

      AWS::Flow::Runner.start_activity_workers(AWS::SimpleWorkflow.new, ".", activity_js)
    end

    it "loads the \"flow/workflows.rb\" by default for workflow worker" do
      def workflow_js
        document = '{
        "workflow_workers": [
          {
            "task_list": "bar",
            "workflow_classes": [ "Object", "String" ],
            "number_of_workers": 3
          }
        ]
      }'
        JSON.parse(document)
      end

      AWS::Flow::Runner.stub(:setup_domain)
      expect(AWS::Flow::Runner).to receive(:require).with(File.join(".", "flow", "workflows.rb"))

      AWS::Flow::Runner.start_workflow_workers(AWS::SimpleWorkflow.new, ".", workflow_js)
    end

    it "takes activity_paths as override to \"flow/activities.rb\"" do
      def activity_js
        document = '{
        "activity_paths": [ "foo", "bar"],
        "activity_workers": [
          {
            "task_list": "bar",
            "activity_classes": [ "Object", "String" ],
            "number_of_workers": 3
          }
        ]
      }'
        JSON.parse(document)
      end

      AWS::Flow::Runner.stub(:setup_domain)
      expect(AWS::Flow::Runner).to_not receive(:require).with(File.join(".", "flow", "activities.rb"))
      expect(AWS::Flow::Runner).to receive(:require).with(File.join("foo"))
      expect(AWS::Flow::Runner).to receive(:require).with(File.join("bar"))

      AWS::Flow::Runner.start_activity_workers(AWS::SimpleWorkflow.new, ".", activity_js)
    end

    it "takes workflow_paths as override to \"flow/workflows.rb\"" do
      def workflow_js
        document = '{
        "workflow_paths": [ "foo", "bar"],
        "workflow_workers": [
          {
            "task_list": "bar",
            "workflow_classes": [ "Object", "String" ],
            "number_of_workers": 3
          }
        ]
      }'
        JSON.parse(document)
      end

      AWS::Flow::Runner.stub(:setup_domain)
      expect(AWS::Flow::Runner).to_not receive(:require).with(File.join(".", "flow", "workflows.rb"))
      expect(AWS::Flow::Runner).to receive(:require).with(File.join("foo"))
      expect(AWS::Flow::Runner).to receive(:require).with(File.join("bar"))

      AWS::Flow::Runner.start_workflow_workers(AWS::SimpleWorkflow.new, ".", workflow_js)
    end

  end




  describe "Implementation classes discovery" do

    # because the object space is not reset between test runs, these
    # classes are declared here for all the tests in this section to use
    class MyActivity1
      extend AWS::Flow::Activities
    end
    class MyActivity2
      extend AWS::Flow::Activities
    end

    class MyWorkflow1
      extend AWS::Flow::Workflows
    end
    class MyWorkflow2
      extend AWS::Flow::Workflows
    end

    before(:each) do
      # stubs to avoid running code that should not be run/covered in these tests
      AWS::Flow::Runner.stub(:spawn_and_start_workers)
    end

    it "finds all the subclasses properly" do
      module Clown 
      end
      class Whiteface 
        extend Clown 
      end
      class Auguste 
        extend Clown 
      end

      sub = AWS::Flow::Runner.all_subclasses(Clown)
      expect(sub).to include(Whiteface)
      expect(sub).to include(Auguste)
    end

    it "finds all the subclasses of AWS::Flow::Activities properly" do
      sub = AWS::Flow::Runner.all_subclasses(AWS::Flow::Activities)
      expect(sub).to include(MyActivity1)
      expect(sub).to include(MyActivity2)
    end

    it "finds all the subclasses of AWS::Flow::Workflows properly" do
      sub = AWS::Flow::Runner.all_subclasses(AWS::Flow::Workflows)
      expect(sub).to include(MyWorkflow1)
      expect(sub).to include(MyWorkflow2)
    end

    it "finds the activity implementations when they are in the environment" do
      def activity_js
        document = '{
        "activity_workers": [
          {
            "task_list": "bar",
            "number_of_workers": 3
          }
        ]
      }'
        JSON.parse(document)
      end

      AWS::Flow::Runner.stub(:setup_domain)
      impls = []
      AWS::Flow::ActivityWorker.any_instance.stub(:add_implementation) do |impl|
        impls << impl
      end

      AWS::Flow::Runner.start_activity_workers(AWS::SimpleWorkflow.new, ".", activity_js)

      expect(impls).to include(MyActivity2)
      expect(impls).to include(MyActivity1)
    end

    it "finds the workflow implementations when they are in the environment" do
      def workflow_js
        document = '{
        "workflow_workers": [
          {
            "task_list": "bar",
            "number_of_workers": 3
          }
        ]
      }'
        JSON.parse(document)
      end

      AWS::Flow::Runner.stub(:setup_domain)
      impls = []
      AWS::Flow::WorkflowWorker.any_instance.stub(:add_implementation) do |impl|
        impls << impl
      end

      AWS::Flow::Runner.start_workflow_workers(AWS::SimpleWorkflow.new, ".", workflow_js)

      expect(impls).to include(MyWorkflow2)
      expect(impls).to include(MyWorkflow1)
    end

  end

  describe "Host-specific tasklists" do
    
    it "expand to the local host name" do
      # note how we test for value equality; not object equality
      expect(AWS::Flow::Runner.expand_task_list("|hostname|")).to eq(Socket.gethostname)
    end

    it "expand to the local host name even in multiple places" do
      # note how we test for value equality; not object equality
      expect(AWS::Flow::Runner.expand_task_list("xxx|hostname|yy|hostname|zz")).to eq("xxx#{Socket.gethostname}yy#{Socket.gethostname}zz")
    end

    it "preserves the task list value if no expanded pattern found" do
      # note how we test for value equality; not object equality
      expect(AWS::Flow::Runner.expand_task_list("xxxzz")).to eq("xxxzz")
    end

  end

end

require 'spec_helper'
require 'logger'
require 'socket'
include Test::Integ

describe "Runner" do
  before(:all) do
    @swf, @domain = setup_swf
  end

  class PingUtils

    WF_VERSION = "1.0"
    ACTIVITY_VERSION = "1.0"
    WF_TASKLIST = "workflow_tasklist"
    ACTIVITY_TASKLIST = "activity_tasklist"

    def initialize
      @domain = Test::Integ::get_test_domain
    end

    def activity_worker
      build_activity_worker(@domain, PingActivity, ACTIVITY_TASKLIST)
    end

    def workflow_worker
      build_workflow_worker(@domain, PingWorkflow, WF_TASKLIST)
    end

    def workflow_client
      build_workflow_client(@domain, from_class: "PingWorkflow")
    end
  end  

  # PingActivity class defines a set of activities for the Ping sample.
  class PingActivity
    extend AWS::Flow::Activities

    # The activity method is used to define activities. It accepts a list of names
    # of activities and a block specifying registration options for those
    # activities
    activity :ping do
      {
        version: PingUtils::ACTIVITY_VERSION,
        default_task_list: PingUtils::ACTIVITY_TASKLIST,
        default_task_schedule_to_start_timeout: 30,
        default_task_start_to_close_timeout: 30
      }
    end

    # This activity will say hello when invoked by the workflow
    def ping() 
      "Pong from #{Socket.gethostname}"
    end
  end

  # PingWorkflow class defines the workflows for the Ping sample
  class PingWorkflow
    extend AWS::Flow::Workflows

    workflow :ping do
      {
        version: PingUtils::WF_VERSION,
        default_task_list: PingUtils::WF_TASKLIST,
        default_execution_start_to_close_timeout: 600,
      }
    end

    # Create an activity client using the activity_client method to schedule
    # activities
    activity_client(:client) { { from_class: "PingActivity" } }

    # This is the entry point for the workflow
    def ping()
      # Use the activity client 'client' to invoke the say_hello activity
      pong=client.ping()
      "Got #{pong}"
    end
  end 

  describe "Sanity Check" do

    it "makes sure credentials and region are in the execution environment" do
      # note: this could be refactored with a map, but errors are easier to figure out this way
      begin
        ENV['AWS_ACCESS_KEY_ID'].should_not be_nil
        ENV['AWS_SECRET_ACCESS_KEY'].should_not be_nil
        ENV['AWS_REGION'].should_not be_nil
      rescue RSpec::Expectations::ExpectationNotMetError
        # FIXME: there ought to be a better way to pass a useful message to the user
        puts "\tPlease see the getting started to set up the environment"
        puts "\thttp://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/installing.html#installing-credentials"
        raise RSpec::Expectations::ExpectationNotMetError
      end
    end

    it "makes sure the credentials and region in the environment can be used to talk to SWF" do
      swf = AWS::SimpleWorkflow.new
      domains = swf.client.list_domains "registration_status" => "REGISTERED"
    end

  end

  describe "Hello World" do
    
    it "runs" do

      runner_config = JSON.parse('{
        "domain":
          {
            "name": ' + "\"#{get_test_domain.name}\"" + ',
            "retention_in_days": 10
          },
        "workflow_paths": [],
        "workflow_workers": [
          {
            "task_list": ' + "\"#{PingUtils::WF_TASKLIST}\"" + ',
            "workflow_classes": [ ' + "\"PingWorkflow\""  + ' ],
            "number_of_workers": 1
          }
        ],
        "activity_paths": [],
        "activity_workers": [
          {
            "task_list": ' + "\"#{PingUtils::ACTIVITY_TASKLIST}\"" + ',
            "activity_classes": [ ' + "\"PingActivity\""  + ' ],
            "number_of_forks_per_worker": 1,
            "number_of_workers": 1
          }
        ]
      }')

      # mock the load_files method to avoid having to create default files
      AWS::Flow::Runner.stub(:load_files)

      workers = AWS::Flow::Runner.start_workers(runner_config)

      sleep 2
      utils  = PingUtils.new
      wf_client = utils.workflow_client
      
      workflow_execution = wf_client.ping()

      wait_for_execution(workflow_execution, 3)

      workflow_execution.events.map(&:event_type).last.should == "WorkflowExecutionCompleted"

      # kill the workers
      workers.each { |w| Process.kill("KILL", w) }
    end
  end
  
end

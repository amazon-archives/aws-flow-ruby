##
# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
##

require 'bundler/setup'
require 'aws/flow'
require 'aws/decider'
require 'aws/replayer'
require 'runner'

include AWS::Flow
include AWS::Flow::Replayer

def constantize(camel_case_word)
  names = camel_case_word.split('::')
  names.shift if names.empty? || names.first.empty?
  constant = Object
  names.each do |name|
    constant = constant.const_defined?(name) ? constant.const_get(name) : constant.const_missing(name)
  end
  constant
end

module Test
  module Integ
    def setup_domain(domain_name)
      swf = AWS::SimpleWorkflow.new
      domain = swf.domains[domain_name]
      unless domain.exists?
        swf.domains.create(domain_name, 10)
      end
      domain
    end

    def build_client(from_class)
      domain = get_test_domain
      build_workflow_client(domain, from_class)
    end

    def build_worker(klass, task_list)
      domain = get_test_domain
      return build_workflow_worker(domain, klass, task_list) if klass.is_a? AWS::Flow::Workflows
      return build_activity_worker(domain, klass, task_list) if klass.is_a? AWS::Flow::Activities
      raise "the class needs to extend AWS::Flow::Workflows or AWS::Flow::Activities"
    end

    def build_workflow_worker(domain, klass, task_list)
      AWS::Flow::WorkflowWorker.new(domain.client, domain, task_list, klass)
    end

    def build_generic_activity_worker(domain, task_list)
      AWS::Flow::ActivityWorker.new(domain.client, domain, task_list)
    end

    def build_activity_worker(domain, klass, task_list)
      AWS::Flow::ActivityWorker.new(domain.client, domain, task_list, klass)
    end

    def build_workflow_client(domain, options_hash)
      AWS::Flow::workflow_client(domain.client, domain) { options_hash }
    end
    def kill_executors
      return if ForkingExecutor.executors.nil?
      ForkingExecutor.executors.each do |executor|
        executor.shutdown(0) unless executor.is_shutdown rescue StandardError
      end
      #TODO Reinstate this, but it's useful to keep them around for debugging
      #ForkingExecutor.executors = []
    end

    def setup_swf domain_name=nil
      current_date = Time.now.strftime("%d-%m-%Y")
      file_name = "/tmp/" + current_date
      if File.exists?(file_name)
        last_run = File.open(file_name, 'r').read.to_i
      else
        last_run = 0
      end
      last_run += 1
      File.open(file_name, 'w+') {|f| f.write(last_run)}
      current_date = Time.now.strftime("%d-%m-%Y")
      swf = AWS::SimpleWorkflow.new
      $rubyflow_decider_domain = domain_name || "rubyflow_#{current_date}-#{last_run}"
      domain = setup_domain($rubyflow_decider_domain)
      return swf, domain
    end

    def get_test_domain
      swf = AWS::SimpleWorkflow.new
      domain = swf.domains[$rubyflow_decider_domain]
      return domain
    end

    def wait_for_execution(execution, sleep_time=5)
      sleep sleep_time until [
        "WorkflowExecutionCompleted",
        "WorkflowExecutionTimedOut",
        "WorkflowExecutionFailed",
        "WorkflowExecutionContinuedAsNew"
      ].include? execution.events.to_a.last.event_type
    end

    def wait_for_decision(execution, decision="DecisionTaskScheduled")
      sleep 1 until [
        decision
      ].flatten.include? execution.events.to_a.last.event_type
    end

    def validate_execution(execution, decision="WorkflowExecutionCompleted")
      execution.events.map(&:event_type).last.should == decision
    end
    def validate_execution_failed(execution)
      validate_execution(execution, "WorkflowExecutionFailed")
    end
  end

  module Unit

    class FakeConfig
      def to_h

      end
    end
    class FakeServiceClient
      attr_accessor :trace

      def respond_decision_task_completed(task_completed_request)
        @trace ||= []
        @trace << task_completed_request
      end
      def start_workflow_execution(options)
        FakeWorkflowExecution.new(nil, options[:workflow_id])
      end
      def register_activity_type(options)
      end
      def register_workflow_type(options)
      end
      def respond_activity_task_completed(task_token, result)
      end
      def config
        FakeConfig.new
      end
    end

    class FakeAttribute
      def initialize(data); @data = data; end
      def method_missing(method_name, *args, &block)
        if @data.keys.include? method_name
          return @data[method_name]
        end
        super
      end
      def keys; @data.keys; end
      def [](key); @data[key]; end
      def to_h; @data; end
      def []=(key, val); @data[key] = val; end
    end
    class TestHistoryEvent < AWS::SimpleWorkflow::HistoryEvent
      def initialize(event_type, event_id, attributes)
        @event_type = event_type
        @attributes = attributes
        @event_id = event_id
        @created_at = Time.now
      end
    end

    class FakeWorkflowType < WorkflowType
      class << self
        attr_accessor :types
      end
      attr_accessor :domain, :name, :version
      def initialize(domain, name, version)
        @domain = domain
        @name = name
        @version = version
        FakeWorkflowType.types ||= {}
        FakeWorkflowType.types["#{name}.#{version}"] = self
      end
    end

    class TestHistoryWrapper
      attr_accessor :workflow_execution, :workflow_type, :events
      def initialize(workflow_type, workflow_execution, events)
        @workflow_type = workflow_type
        @workflow_execution = workflow_execution
        @events = events
      end
      def task_token
        "1"
      end
      def previous_started_event_id
        1
      end
    end

    class FakeEvents
      def initialize(args)
        @events = []
        args.each_with_index do |event, index|
          event, attr = event if event.is_a? Array
          attr ||= {}
          @events << TestHistoryEvent.new(event, index + 1, FakeAttribute.new(attr))
        end
        @events
      end
      def to_a
        @events
      end
    end
    class TrivialConverter
      def dump(x)
        x
      end
      def load(x)
        x
      end
    end

    class FakeLogger
      attr_accessor :level
      def info(s); end
      def debug(s); end
      def warn(s); end
      def error(s); end
    end

    class FakePage
      def initialize(object); @object = object; end
      def page; @object; end
    end

    class FakeWorkflowExecution
      class << self
        attr_accessor :executions
      end
      def initialize(run_id, workflow_id)
        @hash = {}
        @run_id = run_id.nil? ? SecureRandom.uuid : run_id
        @workflow_id = workflow_id.nil? ? SecureRandom.uuid : workflow_id
        @hash = {"runId" => @run_id, "workflowId" => @workflow_id}
        FakeWorkflowExecution.executions ||= {}
        FakeWorkflowExecution.executions[[@run_id, @workflow_id]] = self
      end
      def self.at(workflow_id, run_id)
        FakeWorkflowExecution.executions[[run_id, workflow_id]] unless FakeWorkflowExecution.executions.nil?
      end
      def [](option)
        self.hash[option]
      end
      attr_accessor :run_id, :workflow_id, :task_list, :domain, :hash
    end

    class FakeDomain
      def initialize(workflow_type_object = nil)
        @workflow_type_object ||= []
        @workflow_type_object << workflow_type_object unless workflow_type_object.nil?
        @name = "fake_domain"
      end
      def register(workflow_type_object)
        @workflow_type_object << workflow_type_object
      end
      def page; FakePage.new(@workflow_type_object); end
      def workflow_executions; FakeWorkflowExecution; end
      attr_accessor :name
    end

    class SynchronousWorkflowWorker < WorkflowWorker
      def start
        poller = SynchronousWorkflowTaskPoller.new(@service, nil, DecisionTaskHandler.new(@workflow_definition_map), @task_list)
        poller.poll_and_process_single_task
      end
    end

    class Hash
      def to_h; self; end
    end
  end

end

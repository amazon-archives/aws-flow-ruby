require_relative '../../../spec_helper'

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

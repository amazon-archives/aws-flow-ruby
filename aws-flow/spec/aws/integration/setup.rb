require 'aws/decider'
include AWS::Flow

$RUBYFLOW_DECIDER_TASK_LIST = 'test_task_list'

def kill_executors
  return if ForkingExecutor.executors.nil?
  ForkingExecutor.executors.each do |executor|
    executor.shutdown(0) unless executor.is_shutdown rescue StandardError
  end
  #TODO Reinstate this, but it's useful to keep them around for debugging
  #ForkingExecutor.executors = []
end

def setup_swf
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
  $rubyflow_decider_domain = "rubyflow_#{current_date}-#{last_run}"
  begin
    domain = swf.domains.create($rubyflow_decider_domain, "10")
  rescue AWS::SimpleWorkflow::Errors::DomainAlreadyExistsFault => e
    domain = swf.domains[$rubyflow_decider_domain]
  end
  @swf, @domain = swf, domain
  return swf, domain
end

def wait_for_execution(execution)
    sleep 5 until [
      "WorkflowExecutionCompleted",
      "WorkflowExecutionTimedOut",
      "WorkflowExecutionFailed"
    ].include? execution.events.to_a.last.event_type
end

def get_test_domain
  swf = AWS::SimpleWorkflow.new
  domain = swf.domains[$rubyflow_decider_domain]
  return domain
end
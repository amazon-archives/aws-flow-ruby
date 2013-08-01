#--
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
#++

require 'tmpdir'

module AWS
  module Flow

    class WorkflowTaskPoller


      # Creates a new WorkflowTaskPoller
      #
      # @param service
      #   The Amazon SWF service object on which this task poller will operate.
      #
      # @param [String] domain
      #   The name of the domain containing the task lists to poll.
      #
      # @param [DecisionTaskHandler] handler
      #   A {DecisionTaskHandler} to handle polled tasks. The poller will call the {DecisionTaskHandler#handle_decision_task} method.
      #
      # @param [Array] task_list
      #   Specifies the task list to poll for decision tasks.
      #
      # @param [Object] options
      #   Options to use for the logger.
      #
      def initialize(service, domain, handler, task_list, options=nil)
        @service = service
        @handler = handler
        @domain = domain
        @task_list = task_list
        @logger = options.logger if options
        @logger ||= Utilities::LogFactory.make_logger(self, "debug")
      end



      # Retrieves any decision tasks that are ready.
      def get_decision_tasks
        @domain.decision_tasks.poll_for_single_task(@task_list)
      end

      def poll_and_process_single_task
        # TODO waitIfSuspended
        begin
          @logger.debug "Starting a new task...\n\n\n"
          tasks = get_decision_tasks
          return false if tasks.nil?
          @logger.debug "We have this many tasks #{tasks}"
          @logger.debug "debugging on #{tasks}\n"
          task_completed_request = @handler.handle_decision_task(tasks)
          @logger.debug "task to be responded to with #{task_completed_request}\n"
          if !task_completed_request[:decisions].empty? && (task_completed_request[:decisions].first.keys.include?(:fail_workflow_execution_decision_attributes))
            fail_hash = task_completed_request[:decisions].first[:fail_workflow_execution_decision_attributes]
            reason = fail_hash[:reason]
            details = fail_hash[:details]
            @logger.debug "#{reason}, #{details}"
          end
          @service.respond_decision_task_completed(task_completed_request)
        rescue AWS::SimpleWorkflow::Errors::UnknownResourceFault => e
          # Log stuff
          @logger.debug "Error in the poller, #{e}"
          @logger.debug "The error class in #{e.class}"
        end
      end
    end

    class ActivityTaskPoller
      def initialize(service, domain, task_list, activity_definition_map, options=nil)
        @service = service
        @domain = domain
        @task_list = task_list
        @activity_definition_map = activity_definition_map
        @logger = options.logger if options
        @logger ||= Utilities::LogFactory.make_logger(self, "debug")
        max_workers = options.execution_workers if options
        max_workers = 20 if (max_workers.nil? || max_workers.zero?)
        @executor = ForkingExecutor.new(:max_workers => max_workers, :logger => @logger)

      end

      def execute(task)
        activity_type = task.activity_type
        begin
          context = ActivityExecutionContext.new(@service, @domain, task)
          activity_implementation = @activity_definition_map[activity_type]
          raise "This activity worker was told to work on activity type #{activity_type.name}, but this activity worker only knows how to work on #{@activity_definition_map.keys.map(&:name).join' '}" unless activity_implementation
          output = activity_implementation.execute(task.input, context)
          @logger.debug "Responding on task_token #{task.task_token} for task #{task}"
          if ! activity_implementation.execution_options.manual_completion
            @service.respond_activity_task_completed(:task_token => task.task_token, :result => output)
          end
        rescue ActivityFailureException => e
          respond_activity_task_failed_with_retry(task.task_token, e.message, e.details)
        end
        #TODO all the completion stuffs
      end

      def respond_activity_task_failed_with_retry(task_token, reason, details)
        #TODO Set up this variable
        if @failure_retrier.nil?
          respond_activity_task_failed(task_token, reason, details)
          #TODO Set up other stuff to do if we have it
        end
      end

      def respond_activity_task_canceled_with_retry(task_token, message)
        if @failure_retrier.nil?
          respond_activity_task_canceled(task_token, message)
        end
        #TODO Set up other stuff to do if we have it
      end

      def respond_activity_task_canceled(task_token, message)
        @service.respond_activity_task_canceled({:task_token => task_token, :details => message})
      end

      def respond_activity_task_failed(task_token, reason, details)
        @logger.debug "The task token to be reported on is #{task_token}"
        @service.respond_activity_task_failed(:task_token => task_token, :reason => reason.to_s, :details => details.to_s)
      end

      def process_single_task(task)
        @service = AWS::SimpleWorkflow.new.client.with_http_handler(AWS::Core::Http::NetHttpHandler.new(:ssl_ca_file => AWS.config.ssl_ca_file))
        begin
          begin
            execute(task)
          rescue CancellationException => e
            respond_activity_task_canceled_with_retry(task.task_token, e.message)
          rescue Exception => e
            @logger.error "Got an error, #{e.message}, while executing #{task.activity_type.name}"
            @logger.error "Full stack trace: #{e.backtrace}"
            respond_activity_task_failed_with_retry(task.task_token, e.message, e.backtrace)
            #Do rescue stuff
          ensure
            @poll_semaphore.release
          end
        rescue Exception => e
          semaphore_needs_release = true
          @logger.debug "Got into the other error mode"
          raise e
        ensure
          @poll_semaphore.release if semaphore_needs_release
        end
      end

      def poll_and_process_single_task(use_forking = true)

        @poll_semaphore ||= SuspendableSemaphore.new
        @poll_semaphore.acquire
        semaphore_needs_release = true
        @logger.debug "before the poll\n\n"
        begin
          task = @domain.activity_tasks.poll_for_single_task(@task_list)
          @logger.error "got a task, #{task.activity_type.name}"
          @logger.error "The task token I got was: #{task.task_token}"
        rescue Exception => e
          @logger.debug "I have not been able to poll successfully, and am now bailing out, with error #{e}"
          @poll_semaphore.release
          return false
        end
        if task.nil?
          "Still polling at #{Time.now}, but didn't get anything"
          @logger.debug "Still polling at #{Time.now}, but didn't get anything"
          @poll_semaphore.release
          return false
        end
        semaphore_needs_release = false
        if use_forking
          @executor.execute { process_single_task(task) }
        else
          process_single_task(task)
        end
        # process_single_task(task)
        @logger.debug "finished executing the task"
        return true
      end
    end

    class SuspendableSemaphore

      def initialize

      end

      def acquire
      end

      def release
      end
    end
  end
end

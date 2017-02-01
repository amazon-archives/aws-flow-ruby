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

      # Creates a new `WorkflowTaskPoller`.
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
        @logger ||= Utilities::LogFactory.make_logger(self)
      end

      # @api private
      # Retrieves any decision tasks that are ready.
      def get_decision_task
        @domain.decision_tasks.poll_for_single_task(@task_list)
      end

      def poll_and_process_single_task
        # TODO waitIfSuspended
        begin
          @logger.debug "Polling for a new decision task of type #{@handler.workflow_definition_map.keys.map{ |x| "#{x.name} #{x.version}"} } on task_list: #{@task_list}"
          task = get_decision_task
          if task.nil?
            @logger.debug "Didn't get a task on task_list: #{@task_list}"
            return false
          end
          @logger.info Utilities.workflow_task_to_debug_string("Got decision task", task, @task_list)

          task_completed_request = @handler.handle_decision_task(task)
          @logger.debug "Response to the task will be #{task_completed_request.inspect}"

          if !task_completed_request[:decisions].empty? && (task_completed_request[:decisions].first.keys.include?(:fail_workflow_execution_decision_attributes))
            fail_hash = task_completed_request[:decisions].first[:fail_workflow_execution_decision_attributes]
            reason = fail_hash[:reason]
            details = fail_hash[:details]
          end

          begin
            @service.respond_decision_task_completed(task_completed_request)
          rescue AWS::SimpleWorkflow::Errors::ValidationException => e
            if e.message.include? "failed to satisfy constraint: Member must have length less than or equal to"
              # We want to ensure that the WorkflowWorker doesn't just sit around and
              # time the workflow out. If there is a validation failure possibly
              # because of large inputs to child workflows/activities or large custom
              # exceptions we should fail the workflow with some minimal details.
              task_completed_request[:decisions] = [
                {
                  decision_type: "FailWorkflowExecution",
                  fail_workflow_execution_decision_attributes: {
                    reason: Utilities.validation_error_string("Workflow"),
                    details: "AWS::SimpleWorkflow::Errors::ValidationException"
                  }
                }
              ]
              @service.respond_decision_task_completed(task_completed_request)
            end
            @logger.error "#{task.workflow_type.inspect} failed with exception: #{e.inspect}"
          end
          @logger.info Utilities.workflow_task_to_debug_string("Finished executing task", task, @task_list)
        rescue AWS::SimpleWorkflow::Errors::UnknownResourceFault => e
          @logger.error "Error in the poller, #{e.inspect}"
        rescue AWS::Errors::MissingCredentialsError => e
          @logger.error "Error in the poller, #{e.inspect}"
          raise e
        rescue Exception => e
          @logger.error "Error in the poller, #{e.inspect}"
        end
      end
    end

    # A poller for activity tasks.
    #
    class ActivityTaskPoller

      # Initializes a new `ActivityTaskPoller`.
      #
      # @param service
      #   *Required*. The AWS::SimpleWorkflow instance to use.
      #
      # @param domain
      #   *Required*. The domain used by the workflow.
      #
      # @param task_list
      #   *Required*. The task list used to poll for activity tasks.
      #
      # @param activity_definition_map
      #   *Required*. The {ActivityDefinition} instance that implements the
      #   activity to run. This map is in the form:
      #
      #       { :activity_type => 'activity_definition_name' }
      #
      #   The named activity definition will be run when the {#execute} method
      #   is called.
      #
      # @param options
      #   *Optional*. Options to set for the activity poller. You can set the
      #   following options:
      #
      #   * `logger` - The logger to use.
      #   * `max_workers` - The maximum number of workers that can be running at
      #       once. The default is 20.
      #
      def initialize(service, domain, task_list, activity_definition_map, executor, options=nil)
        @service = service
        @service_opts = @service.config.to_h
        @domain = domain
        @task_list = task_list
        @activity_definition_map = activity_definition_map
        @logger = options.logger if options
        @logger ||= Utilities::LogFactory.make_logger(self)
        @executor = executor
      end

      # Executes the specified activity task.
      #
      # @param task
      #   *Required*. The
      #   [AWS::SimpleWorkflow::ActivityTask](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/ActivityTask.html)
      #   object to run.
      #
      def execute(task)
        activity_type = task.activity_type
        begin
          context = ActivityExecutionContext.new(@service, @domain, task)
          unless activity_implementation = @activity_definition_map[activity_type]
            raise "This activity worker was told to work on activity type "\
              "#{activity_type.inspect}, but this activity worker only knows "\
              "how to work on #{@activity_definition_map.keys.map(&:name).join' '}"
          end

          output, original_result, too_large = activity_implementation.execute(task.input, context)

           @logger.debug "Responding on task_token #{task.task_token.inspect}."
          if too_large
            @logger.error "#{task.activity_type.inspect} failed: "\
              "#{Utilities.validation_error_string_partial("Activity")} For "\
              "reference, the result was #{original_result}"

            respond_activity_task_failed_with_retry(
              task.task_token,
              Utilities.validation_error_string("Activity"),
              ""
            )
          elsif ! activity_implementation.execution_options.manual_completion
            @service.respond_activity_task_completed(
              :task_token => task.task_token,
              :result => output
            )
          end
        rescue ActivityFailureException => e
          @logger.error "#{task.activity_type.inspect} failed with exception: #{e.inspect}."
          respond_activity_task_failed_with_retry(
            task.task_token,
            e.message,
            e.details
          )
        end
      end

      # Responds to the decider that the activity task has failed, and attempts
      # to retry the task.
      #
      # @note Retry behavior for this method is currently *not implemented*. For
      #       now, it simply wraps {#respond_activity_task_failed}.
      #
      # @api private
      def respond_activity_task_failed_with_retry(task_token, reason, details)
        #TODO Set up this variable
        if @failure_retrier.nil?
          respond_activity_task_failed(task_token, reason, details)
          #TODO Set up other stuff to do if we have it
        end
      end

      # Responds to the decider that the activity task should be canceled, and
      # attempts to retry the task.
      #
      # @note Retry behavior for this method is currently *not implemented*. For
      #       now, it simply wraps {#respond_activity_task_canceled}.
      #
      # @api private
      def respond_activity_task_canceled_with_retry(task_token, message)
        if @failure_retrier.nil?
          respond_activity_task_canceled(task_token, message)
        end
        #TODO Set up other stuff to do if we have it
      end

      # Responds to the decider that the activity task should be canceled. No
      # retry is attempted.
      #
      # @param task_token
      #   *Required*. The task token from the {ActivityDefinition} object to
      #   retry.
      #
      # @param message
      #   *Required*. A message that provides detail about why the activity task
      #   is cancelled.
      #
      def respond_activity_task_canceled(task_token, message)

        begin
          @service.respond_activity_task_canceled(
            :task_token => task_token,
            :details => message
          )
        rescue AWS::SimpleWorkflow::Errors::ValidationException => e
          if e.message.include? "failed to satisfy constraint: Member must have length less than or equal to"
            # We want to ensure that the ActivityWorker doesn't just sit
            # around and time the activity out. If there is a validation failure
            # possibly because of large custom exceptions we should fail the
            # activity task with some minimal details
            respond_activity_task_failed_with_retry(
              task_token,
              Utilities.validation_error_string("Activity"),
              "AWS::SimpleWorkflow::Errors::ValidationException"
            )
          end
          @logger.error "respond_activity_task_canceled call failed with "\
            "exception: #{e.inspect}"
        end

      end

      # Responds to the decider that the activity task has failed. No retry is
      # attempted.
      #
      # @param task_token
      #   *Required*. The task token from the {ActivityDefinition} object to
      #   retry. The task token is generated by the service and should be
      #   treated as an opaque value.
      #
      # @param reason
      #   *Required*. Description of the error that may assist in diagnostics.
      #   Although this value is *required*, you can set it to an empty string
      #   if you don't need this information.
      #
      # @param details
      #   *Required*. Detailed information about the failure. Although this
      #   value is *required*, you can set it to an empty string if you don't
      #   need this information.
      #
      def respond_activity_task_failed(task_token, reason, details)
        @logger.debug "The task token to be reported on is #{task_token}"

        begin
          @service.respond_activity_task_failed(
            task_token: task_token,
            reason: reason.to_s,
            details: details.to_s
          )
        rescue AWS::SimpleWorkflow::Errors::ValidationException => e
          if e.message.include? "failed to satisfy constraint: Member must have length less than or equal to"
            # We want to ensure that the ActivityWorker doesn't just sit
            # around and time the activity out. If there is a validation failure
            # possibly because of large custom exceptions we should fail the
            # activity task with some minimal details
            respond_activity_task_failed_with_retry(
              task_token,
              Utilities.validation_error_string("Activity"),
              "AWS::SimpleWorkflow::Errors::ValidationException"
            )
          end
          @logger.error "respond_activity_task_failed call failed with "\
            "exception: #{e.inspect}"
        end
      end

      # Processes the specified activity task.
      #
      # @param task
      #   *Required*. The
      #   [AWS::SimpleWorkflow::ActivityTask](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/ActivityTask.html)
      #   object to process.
      #
      def process_single_task(task)

        # We are using the 'build' method to create a new ConnectionPool here to
        # make sure that connection pools are not shared among forked processes.
        # The default behavior of the ConnectionPool class is to cache a pool
        # for a set of options created by the 'new' method and always use the
        # same pool for the same set of options. This is undesirable when
        # multiple processes want to use different connection pools with same
        # options as is the case here.
        #
        # Since we can't change the pool of an already existing NetHttpHandler,
        # we also create a new NetHttpHandler in order to use the new pool.

        @service_opts[:connection_pool] = AWS::Core::Http::ConnectionPool.build(@service_opts[:http_handler].pool.options)
        @service_opts[:http_handler] = AWS::Core::Http::NetHttpHandler.new(@service_opts)
        @service = @service.with_options(@service_opts)

        begin
          begin
            execute(task)
          rescue CancellationException => e
            @logger.error "#{task.activity_type.inspect} failed with exception: #{e.inspect}"
            respond_activity_task_canceled_with_retry(task.task_token, e.message)
          rescue Exception => e
            @logger.error "#{task.activity_type.inspect} failed with exception: #{e.inspect}"
            respond_activity_task_failed_with_retry(task.task_token, e.message, e.backtrace)
          ensure
            @poll_semaphore.release
          end
        rescue Exception => e
          semaphore_needs_release = true
          @logger.error "Error in the poller, exception: #{e.inspect}. stacktrace: #{e.backtrace}"
          raise e
        ensure
          @poll_semaphore.release if semaphore_needs_release
        end
      end

      # Polls the task list for a new activity task, and executes it if one is
      # found.
      #
      # If `use_forking` is set to `true` and the maximum number of workers (as
      # set in {#initialize}) are already executing, this method will block
      # until the number of running workers is less than the maximum.
      #
      # @param use_forking
      #   *Optional*. Whether to use forking to execute the task. On Windows,
      #   you should set this to `false`.
      #
      def poll_and_process_single_task(use_forking = true)
        @poll_semaphore ||= SuspendableSemaphore.new
        @poll_semaphore.acquire
        semaphore_needs_release = true
        begin
          if use_forking
            @executor.block_on_max_workers
          end
          @logger.debug "Polling for a new activity task of type #{@activity_definition_map.keys.map{ |x| "#{x.name} #{x.version}"} } on task_list: #{@task_list}"
          task = @domain.activity_tasks.poll_for_single_task(@task_list)
          if task
            @logger.info Utilities.activity_task_to_debug_string("Got activity task", task)
          end
        rescue Exception => e
          @logger.error "Error in the poller, #{e.inspect}"
          @poll_semaphore.release
          return false
        end
        if task.nil?
          @logger.debug "Didn't get a task on task_list: #{@task_list}"
          @poll_semaphore.release
          return false
        end
        semaphore_needs_release = false
        if use_forking
          @executor.execute { process_single_task(task) }
        else
          process_single_task(task)
        end
        @logger.info Utilities.activity_task_to_debug_string("Finished executing task", task)
        return true
      end
    end

    # @note This class is currently not implemented.
    # @api private
    class SuspendableSemaphore

      # @note This method is not implemented.
      # @api private
      def initialize
      end

      # @note This method is not implemented.
      # @api private
      def acquire
      end

      # @note This method is not implemented.
      # @api private
      def release
      end
    end
  end
end

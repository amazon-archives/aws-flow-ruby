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
          @logger.info Utilities.workflow_task_to_debug_string("Got decision task", task)

          task_completed_request = @handler.handle_decision_task(task)
          @logger.debug "Response to the task will be #{task_completed_request}"
          if !task_completed_request[:decisions].empty? && (task_completed_request[:decisions].first.keys.include?(:fail_workflow_execution_decision_attributes))
            fail_hash = task_completed_request[:decisions].first[:fail_workflow_execution_decision_attributes]
            reason = fail_hash[:reason]
            details = fail_hash[:details]
            @logger.debug "#{reason}, #{details}"
          end
          @service.respond_decision_task_completed(task_completed_request)
          @logger.info Utilities.workflow_task_to_debug_string("Finished executing task", task)
        rescue AWS::SimpleWorkflow::Errors::UnknownResourceFault => e
          @logger.error "Error in the poller, #{e.class}, #{e}"
        rescue Exception => e
          @logger.error "Error in the poller, #{e.class}, #{e}"
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
            raise "This activity worker was told to work on activity type #{activity_type.name}.#{activity_type.version}, but this activity worker only knows how to work on #{@activity_definition_map.keys.map(&:name).join' '}"
          end

          output, original_result, too_large = activity_implementation.execute(task.input, context)

           @logger.debug "Responding on task_token #{task.task_token} for task #{task}."
          if too_large
            @logger.error "Activity #{activity_type.name}.#{activity_type.version} failed: The output of this activity was too large (greater than 2^15), and therefore aws-flow could not return it to SWF. aws-flow is now attempting to mark this activity as failed. For reference, the result was #{original_result}"

            respond_activity_task_failed_with_retry(
              task.task_token,
              "We could not serialize the output of the Activity correctly since it was too large. Please limit the size of the output to 32768 characters. Please look at the Activity Worker logs to see the original output.",
              ""
            )
          elsif ! activity_implementation.execution_options.manual_completion
            @service.respond_activity_task_completed(
              :task_token => task.task_token,
              :result => output
            )
          end
        rescue ActivityFailureException => e
          @logger.error "Activity #{activity_type.name}.#{activity_type.version} with input #{task.input} failed with exception #{e}."

          respond_activity_task_failed_with_retry(
            task.task_token,
            e.message,
            e.details
          )
        end
        #TODO all the completion stuffs
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
        @service.respond_activity_task_canceled({:task_token => task_token, :details => message})
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
        @service.respond_activity_task_failed(:task_token => task_token, :reason => reason.to_s, :details => details.to_s)
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

        options = @service.config.to_h
        options[:connection_pool] = AWS::Core::Http::ConnectionPool.build(options[:http_handler].pool.options)
        options[:http_handler] = AWS::Core::Http::NetHttpHandler.new(options)
        @service = AWS::SimpleWorkflow.new(options).client

        begin
          begin
            execute(task)
          rescue CancellationException => e
            @logger.error "Got an error, #{e.message}, while executing #{task.activity_type.name}."
            respond_activity_task_canceled_with_retry(task.task_token, e.message)
          rescue Exception => e
            begin
              @logger.error "Got an error, #{e.message}, while executing #{task.activity_type.name}. Full stack trace: #{e.backtrace}"
              respond_activity_task_failed_with_retry(task.task_token, e.message, e.backtrace)
            rescue Exception => e
              # We want to ensure that the ActivityWorker doesn't just sit
              # around and time the activity out. If there is a failure and we can't
              # respond back with the correct exception for some reason
              # (possibly really large exceptions), we should fail the activity task with
              # some minimal details
              reason = "ActivityWorker failed to respond_activity_task_failed with the correct message and stacktrace. Please look at the ActivityWorker logs for more details."
              @logger.error reason
              respond_activity_task_failed_with_retry(task.task_token, reason, "")
            end
          ensure
            @poll_semaphore.release
          end
        rescue Exception => e
          semaphore_needs_release = true
          @logger.error "Got into the other error mode: #{e}"
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
        @logger.debug "Before the poll"
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
          @logger.error "Error in the poller, #{e.class}, #{e}"
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

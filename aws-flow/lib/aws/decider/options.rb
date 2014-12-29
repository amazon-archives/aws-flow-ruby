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

module AWS
  module Flow

    # The base class for option defaults in the AWS Flow Framework for Ruby.
    class Defaults

      # @api private
      def method_missing(method_name, *args, &block)
        return nil
      end
    end

    # The base class for all options classes in the AWS Flow Framework for Ruby.
    class Options
      extend Utilities::UpwardLookups
      include Utilities::UpwardLookups::InstanceMethods

      # @api private
      def method_missing(method_name, *args, &block)
        return nil
      end

      # Sets the default classes on a child class (a class derived from
      # {Options}).
      def self.inherited(child)
        child.precursors ||= []
        default_classes = child.ancestors.map do |precursor|
          if precursor.methods.map(&:to_sym).include? :default_classes
            precursor.default_classes
          end
        end.compact.flatten
        child.instance_variable_set("@default_classes", default_classes)
      end

      # @api private
      class << self
        # The set of default options. These are used when `use_defaults` is set
        # to `true` on {#initialize}.
        attr_accessor :default_classes
      end

      # Merges specified options with the set of options already held by the
      # class, and returns the result.
      #
      # @return [Hash]
      #   The merged set of options, returned as a hash.
      #
      # @param [Options] options
      #   An {Options}-derived class containing a set of options to use if this
      #   instance has no options, or options to add to this one if this
      #   instance already has options.
      #
      # @param [Hash] extra_to_add
      #   A hash containing extra options to merge with the options held by the
      #   class and those provided in the `options` parameter.
      #
      def get_options(options, extra_to_add = {})
        options = self.class.held_properties.compact if options.empty?

        set_options = options.select do |option|
          self.send(option) != nil && self.send(option) != ""
        end

        option_values = set_options.map do |option|
          self.send(option) == Float::INFINITY ? "NONE" : self.send(option)
        end

        result = Hash[set_options.zip(option_values)]
        result.merge(extra_to_add)
      end

      # Creates a new {Options} instance.
      #
      # @param [Hash] hash
      #   *Optional*. A hash of values to use as the default options for this
      #   instance. The members of this hash are defined by classes derived from
      #   {Options}.
      #
      # @param [true, false] use_defaults
      #   *Optional*. In derived classes, this parameter is used to tell the
      #   constructor to use the set of default options as the runtime options.
      #   This has no effect in the base {Options} class.
      #
      def initialize(hash={}, use_defaults = false)
        @precursors ||= []
        hash.each_pair do |key, val|
          if self.methods.map(&:to_sym).include? "#{key}=".to_sym
            self.send("#{key}=", val)
          end
        end
      end

      property(:from_class)
      property(:prefix_name, [lambda {|x| raise "You cannot have a . in your prefix name" if x.include? "."; x}, lambda(&:to_s)])
      property(:return_on_start, [lambda {|x| x == true}])

    end

    # Defaults for the {WorkerOptions} class.
    #
    class WorkerDefaults < Defaults

      # Whether to use Ruby's `fork` for launching new workers. The default is
      # `true`.
      #
      # On Windows, you should override this default value and set `use_forking`
      # to `false`. See {WorkerOptions} for more information.
      #
      # @return [Boolean]
      #   Returns `true`.
      #
      def use_forking; true; end
    end

    # Options for activity and workflow workers.
    #
    # @!attribute logger
    #   The logger to use for the worker.
    #
    # @!attribute execution_workers
    #   The maximum number of execution workers that can be running at once. You
    #   can set this to zero or `nil`, in which case the default value of 20
    #   will be used.
    #
    # @!attribute use_forking
    #   Whether to use Ruby's `fork` for launching new workers. The default is
    #   `true`.
    #
    #   On Windows, `use_forking` should generally be set to `false`:
    #
    #       AWS::Flow::ActivityWorker.new(
    #           @domain.client, @domain, ACTIVITY_TASKLIST, klass) { { use_forking: false } }
    #
    #   For more information, see
    #   [Important Notes](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/welcome.html#forking-windows-note)
    #   in the *AWS Flow Framework for Ruby Developer Guide*.
    #
    class WorkerOptions < Options
      property(:logger, [])
      # At current, we only support one poller per worker
      # property(:poller_workers, [lambda(&:to_i)])
      property(:execution_workers, [lambda(&:to_i)])
      property(:use_forking, [lambda {|x| x == true}] )
      default_classes << WorkerDefaults.new
    end

    # Options for {WorkflowClient#signal_workflow_execution}.
    #
    # @!attribute control
    #   Optional data attached to the signal that can be used by the workflow
    #   execution.
    #
    # @!attribute domain
    #   *Required*. The name of the domain containing the workflow execution to
    #   signal.
    #
    # @!attribute input
    #   Data to attach to the `WorkflowExecutionSignaled` event in the target
    #   workflow execution's history.
    #
    # @!attribute run_id
    #   The runId of the workflow execution to signal.
    #
    # @!attribute signal_name
    #   *Required*. The name of the signal. This name must be meaningful to the
    #   target workflow.
    #
    # @!attribute workflow_id
    #   *Required*. The workflow ID of the workflow execution to signal.
    #
    class SignalWorkflowOptions < Options
      properties(:input, :signal_name, :run_id, :workflow_id, :control, :domain)

      # Gets a hash containing the held options.
      def get_full_options
        result = {}
        SignalWorkflowOptions.held_properties.each do |option|
          result[option] = self.send(option) if self.send(option) != nil && self.send(option) != ""
        end
        result
      end
    end

    # Defaults for {RetryOptions}.
    class RetryDefaults < Defaults

      #  The default maximum number of attempts to make before the task is
      #  marked as failed.
      #
      #  @see FlowConstants.exponential_retry_maximum_attempts
      def maximum_attempts; FlowConstants.exponential_retry_maximum_attempts; end

      #  The default retry function to use.
      #
      #  @see FlowConstants.exponential_retry_function
      def retry_function; FlowConstants.exponential_retry_function; end

      #  The exceptions that will initiate a retry attempt. The default is to
      #  use *all* exceptions.
      #
      #  @see FlowConstants.exponential_retry_exceptions_to_include
      def exceptions_to_include; FlowConstants.exponential_retry_exceptions_to_include; end

      #  The exceptions that will *not* initiate a retry attempt. The default is
      #  an empty list; no exceptions are excluded.
      #
      #  @see FlowConstants.exponential_retry_exceptions_to_exclude
      def exceptions_to_exclude; FlowConstants.exponential_retry_exceptions_to_exclude; end

      # Sets the backoff coefficient to use for retry attempts.
      #
      # @see FlowConstants.exponential_retry_backoff_coefficient
      def backoff_coefficient; FlowConstants.exponential_retry_backoff_coefficient; end

      # @desc (see FlowConstants#should_jitter)
      # @return the value of {FlowConstants#should_jitter}
      def should_jitter; FlowConstants.should_jitter; end

      # @see FlowConstants.jitter_function
      def jitter_function; FlowConstants.jitter_function; end

      # @see FlowConstants.exponential_retry_initial_retry_interval
      def initial_retry_interval; FlowConstants.exponential_retry_initial_retry_interval; end
    end

    # Retry options used with {GenericClient#retry} and {ActivityClient#exponential_retry}.
    class RetryOptions < Options
      property(:is_retryable_function, [])
      property(:initial_retry_interval, [])
      property(:exceptions_to_allow, [])
      property(:maximum_attempts, [lambda {|x| x == "NONE" ? "NONE" : x.to_i}])
      property(:maximum_retry_interval_seconds, [lambda {|x| x == "NONE" ? "NONE" : x.to_i}])
      property(:exceptions_to_retry, [])
      property(:exceptions_to_exclude, [])
      property(:exceptions_to_include, [])
      property(:jitter_function, [])
      property(:should_jitter, [lambda {|x| x == true}])
      property(:retries_per_exception, [])
      property(:retry_function, [])
      default_classes << RetryDefaults.new

      # Creates a new {RetryOptions} instance.
      #
      # @param [Hash] hash The set of default RetryOptions.
      #
      # @option hash :is_retryable_function
      #  The function used to test if the activity is retryable.
      #
      # @option hash :exceptions_to_allow [Integer]
      #  The number of exceptions to allow.
      #
      # @option hash :maximum_attempts [Integer]
      #  The maximum number of attempts to make before the task is marked as failed.
      #
      # @option hash :maximum_retry_interval_seconds [Integer]
      #  The maximum retry interval, in seconds.
      #
      # @option hash :exceptions_to_retry [Array]
      #  The list of exceptions that will cause a retry attempt.
      #
      # @option hash :exceptions_to_exclude [Array]
      #  The list of exceptions to exclude from retry.
      #
      # @option hash :jitter_function
      #  The jitter function used to modify the actual retry time.
      #
      # @option hash :retries_per_exception [Integer]
      #  The number of retries to make per exception.
      #
      # @option hash :retry_function
      #  The retry function to use.
      #
      # @param [true, false] use_defaults
      #   If set to `true`, the default options specified will be used as the
      #   runtime options.
      #
      def initialize(hash={}, use_defaults=false)
        super(hash, use_defaults)
      end

      # Tests whether this activity can be retried based on the
      # `exceptions_to_retry` and `exceptions_to_exclude` options.
      #
      # @param [Object] failure
      #   The failure type to test.
      #
      # @return [true, false]
      #   Returns `true` if the activity can be retried; `false` otherwise.
      #
      def isRetryable(failure)
        #TODO stuff about checking for a DecisionException, getting cause if so
        is_retryable = false
        is_retryable = @exceptions_to_retry.reject {|exception| failure.class <= exception}.empty?
        if is_retryable
          is_retryable = @exceptions_to_exclude.select{|exception| failure.class <= exception}.empty?
        end
        return is_retryable
      end
    end

    # Exponential retry options for the {ActivityClient#exponential_retry}
    # method.
    class ExponentialRetryOptions < RetryOptions
      default_classes << RetryDefaults.new

      # The backoff coefficient to use. This is a floating point value that is
      # multiplied with the current retry interval after every retry attempt.
      # The default value is `2.0`, which means that each retry will take twice
      # as long as the previous one.
      property(:backoff_coefficient, [lambda(&:to_i)])

      # The retry expiration interval, in seconds. This will be increased after
      # every retry attempt by the factor provided in `backoff_coefficient`.
      property(:retry_expiration_interval_seconds, [lambda(&:to_i)])

      # @api private
      def next_retry_delay_seconds(first_attempt, recorded_failure, attempts)
        raise IllegalArgumentException "Attempt number is #{attempts}, when it needs to be greater than 1"
        if @maximum_attempts
        end
      end
    end

    # Defaults for `WorkflowOptions`.
    class WorkflowDefaults < Defaults

      # The default data converter. By default, this is {YAMLDataConverter}.
      def data_converter; FlowConstants.default_data_converter; end

    end

    class WorkflowRegistrationDefaults < WorkflowDefaults

      # The default task start-to-close timeout duration. The default value is
      # `30`.
      def default_task_start_to_close_timeout; 30; end

      # The default child workflow policy. The default value is `TERMINATE`.
      def default_child_policy; "TERMINATE"; end

      # Returns a list of tags for the workflow. The default value is an empty
      # array (no tags).
      def tag_list; []; end

      # The default task priority.The default value is '0'
      def default_task_priority; 0; end

      def default_task_list; FlowConstants.use_worker_task_list; end
    end

    # Options for workflows.
    #
    # @!attribute child_policy
    #   The optional policy to use for the child workflow executions when a
    #   workflow execution of this type is terminated, by calling the
    #   `TerminateWorkflowExecution` action explicitly or due to an expired
    #   timeout.
    #
    #   This can be overridden when starting a workflow execution using
    #   {WorkflowClient#start_execution} or the `StartChildWorkflowExecution`
    #   decision.
    #
    #   The supported child policies are:
    #
    #   * `TERMINATE`: the child executions will be terminated.
    #
    #   * `REQUEST_CANCEL`: a request to cancel will be attempted for each child
    #      execution by recording a `WorkflowExecutionCancelRequested` event in its
    #      history. It is up to the decider to take appropriate actions when it
    #      receives an execution history with this event.
    #
    #   * `ABANDON`: no action will be taken. The child executions will continue
    #     to run.
    #
    #   The default is `TERMINATE`.
    #
    # @!attribute execution_method
    #
    # @!attribute execution_start_to_close_timeout
    #   The optional maximum duration, specified when registering the workflow
    #   type, for executions of this workflow type.
    #
    #   This default can be overridden when starting a workflow execution using
    #   {WorkflowClient#start_execution} or with the
    #   `StartChildWorkflowExecution` decision.
    #
    #   The valid values are integers greater than or equal to zero. An integer
    #   value can be used to specify the duration in seconds while `NONE` can be
    #   used to specify unlimited duration.
    #
    # @!attribute input
    #   A string of up to 32768 characters to be provided to the workflow
    #   execution. This will be received in the decider when polling for
    #   decision tasks.
    #
    # @!attribute tag_list
    #   The list of tags to associate with the child workflow execution.
    #
    #   A maximum of five tags can be specified. You can list workflow
    #   executions with a specific tag by calling
    #   [AWS::SimpleWorkflow::Client#list_open_workflow_executions](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Client.html#list_open_workflow_executions-instance_method)
    #   or
    #   [AWS::SimpleWorkflow::Client#list_closed_workflow_executions](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Client.html#list_closed_workflow_executions-instance_method)
    #   and specifying a tag to filter on.
    #
    # @!attribute task_list
    #   The optional task list, specified when registering the workflow type,
    #   for decisions tasks scheduled for workflow executions of this type.
    #
    #   This default can be overridden when starting a workflow execution using
    #   {WorkflowClient#start_execution} or the `StartChildWorkflowExecution`
    #   decision.
    #
    # @!attribute task_start_to_close_timeout
    #   The optional maximum duration, specified when registering the workflow
    #   type, that a decision task for executions of this workflow type might
    #   take before returning completion or failure.
    #
    #   If the task does not close in the specified time, then the task is
    #   automatically timed out and rescheduled. If the decider eventually
    #   reports a completion or failure, it is ignored. This default can be
    #   overridden when starting a workflow execution using
    #   {WorkflowClient#start_execution} or the `StartChildWorkflowExecution`
    #   decision.
    #
    #   The valid values are integers greater than or equal to 0. An integer
    #   value can be used to specify the duration in seconds while `NONE` can be
    #   used to specify unlimited duration.  The default is `30`.
    #
    # @!attribute task_priority
    #   The optional task priority if set, specifies the priority for the
    #   decision tasks for a workflow execution. This overrides the
    #   defaultTaskPriority specified when registering the WorkflowType. The
    #   valid values are Integers between Integer.MAX_VALUE to
    #   Integer.MIN_VALUE, i.e. between 2147483647 and -2147483648 for 32-bit
    #   integer. An integer value can be used to specify the priority with
    #   which a workflow must be started.
    #
    #   Note: task_priority for a workflow must be specified either as a
    #   default for the WorkflowType or through this field. If neither this
    #   field is set nor a default_task_priority is specified at registration
    #   time then it will be assumed nil.
    #
    # @!attribute version
    #   A string that represents the version of the workflow. This can be any
    #   alphanumeric string. If you update any of the other options, you must
    #   also update the version.
    #
    # @!attribute workflow_id
    #   *Required*. The workflow ID of the workflow execution. The workflow ID
    #   must follow these rules:
    #
    #   * The specified string must not start or end with whitespace.
    #   * It must not contain a `:` (colon), `/` (slash), `|` (vertical bar), or
    #     any control characters (`\u0000`-`\u001f` | `\u007f`-`\u009f`).
    #   * It must not contain the literal string "arn".
    #
    class WorkflowOptions < Options

      properties(
        :version,
        :input,
        :workflow_id,
        :execution_start_to_close_timeout,
        :task_start_to_close_timeout,
        :task_priority,
        :task_list,
        :execution_method
      )

      property(:tag_list, [])
      property(:child_policy, [lambda(&:to_s), lambda(&:upcase)])
      property(:data_converter, [])

      default_classes << WorkflowDefaults.new

      def get_full_options
        result = {}
        usable_properties = self.class.held_properties
        usable_properties.delete(:from_class)
        usable_properties.each do |option|
          result[option] = self.send(option) if self.send(option) != nil && self.send(option) != ""
        end
        result
      end

    end

    class WorkflowRegistrationOptions < WorkflowOptions
      class << self
        def registration_options
          [
            :default_task_start_to_close_timeout,
            :default_execution_start_to_close_timeout,
            :default_task_list,
            :default_task_priority,
            :default_child_policy
          ]
        end
      end

      # Adding default properties
      properties(
        :default_task_start_to_close_timeout,
        :default_execution_start_to_close_timeout,
        :default_task_list,
        :default_task_priority
      )

      property(:default_child_policy, [lambda(&:to_s), lambda(&:upcase)])

      default_classes << WorkflowRegistrationDefaults.new

      def get_registration_options
        get_options(self.class.registration_options)
      end

    end


    # Options for {WorkflowClient#start_execution}.
    #
    # @!attribute workflow_name
    #   The name of this workflow.
    #
    # @!attribute from_class
    #   If present, options from the specified class will be used as the
    #   workflow execution options.
    #
    class StartWorkflowOptions < WorkflowOptions
      properties(:workflow_name, :from_class)
    end

    # Options for {AsyncDecider#continue_as_new_workflow} and
    # {Workflows.InstanceMethods#continue_as_new}.
    #
    class ContinueAsNewOptions < WorkflowOptions
    end

    # Defaults for the {ActivityOptions} class.
    class ActivityDefaults < Defaults
      def data_converter; FlowConstants.default_data_converter; end
    end

    # Default values for a registered activity type. These values are set by
    # default for all activities that use the activity type.
    class ActivityRegistrationDefaults < ActivityDefaults

      # *Optional*. The default schedule-to-start timeout for activity tasks.
      # This timeout represents the time, in seconds, between when the activity
      # task is first scheduled to when it is started.
      #
      # This default can be overridden when scheduling an activity task. You can
      # set this value to "NONE" to imply no timeout value.
      def default_task_schedule_to_start_timeout; Float::INFINITY; end

      # *Optional*. The default schedule-to-close timeout for activity tasks.
      # This timeout represents the time, in seconds, between when the activity
      # task is first scheduled to when it is closed (whether due to success,
      # failure, or a timeout).
      #
      # This default can be overridden when scheduling an activity task. You can
      # set this value to "NONE" to imply no timeout value.
      def default_task_schedule_to_close_timeout; Float::INFINITY; end

      # *Optional*. The default start-to-close timeout for activity tasks. This
      # timeout represents the time, in seconds, between when the activity task
      # is first started to when it is closed (whether due to success, failure,
      # or a timeout).
      #
      # This default can be overridden when scheduling an activity task. You can
      # set this value to "NONE" to imply no timeout value.
      def default_task_start_to_close_timeout; Float::INFINITY; end

      # *Optional*. The default maximum time, in seconds, before which a worker
      # processing a task of this type must report progress. If the timeout is
      # exceeded, the activity task is automatically timed out. If the worker
      # subsequently attempts to record a heartbeat or returns a result, it will
      # be ignored.
      #
      # This default can be overridden when scheduling an activity task. You can
      # set this value to "NONE" to imply no timeout value.
      def default_task_heartbeat_timeout; Float::INFINITY; end

      # *Optional*. The default task list to use for all activities that use
      # this activity type. If not specified, the value
      # `FlowConstants.use_worker_task_list` will be used, which causes the
      # activities to use the task list specified for the activity worker.
      def default_task_list; FlowConstants.use_worker_task_list; end

      # The optional default task priority, specified when registering the activity type,
      # for tasks of this activity type. This default can be overridden when scheduling
      # a task through - the ScheduleActivityTask Decision.
      # The valid values are Integer number(Integer.MAX_VALUE to Integer.MIN_VALUE),
      # (2147483647 to -2147483648) for 32-bit integer.
      # An integer value can be used to specify the priority with which an activity must
      # be executed.
      def default_task_priority; 0; end

      def default_task_list; FlowConstants.use_worker_task_list; end
    end

    # Options to use on an activity or decider. The following options are
    # defined:
    #
    # @!attribute default_task_heartbeat_timeout
    #   The optional default maximum time, specified when registering the
    #   activity type, before which a worker processing a task must report
    #   progress by calling
    #   {http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/ActivityTask.html#record_heartbeat!-instance_method
    #   record_heartbeat} on the `ActivityTask`.
    #
    #   You can override this default when scheduling a task through the
    #   `ScheduleActivityTask` decision. If the activity worker subsequently
    #   attempts to record a heartbeat or returns a result, the activity worker
    #   receives an UnknownResource fault. In this case, Amazon SWF no longer
    #   considers the activity task to be valid; the activity worker should
    #   clean up the activity task.
    #
    #   The valid values are integers greater than or equal to zero. An integer
    #   value can be used to specify the duration in seconds while "NONE" can be
    #   used to specify unlimited duration.
    #
    # @!attribute default_task_list
    #   The optional default task list specified for this activity type at
    #   registration. This default task list is used if a task list is not
    #   provided when a task is scheduled through the `ScheduleActivityTask`
    #   decision. You can override this default when scheduling a task through
    #   the `ScheduleActivityTask` decision.
    #
    # @!attribute default_task_priority
    #   The optional default task priority specified for this activity type at
    #   registration. This default task priority is used if a task priority is not
    #   provided when a task is scheduled through the `ScheduleActivityTask`
    #   decision. You can override this default when scheduling a task through
    #   the `ScheduleActivityTask` decision.
    #
    # @!attribute default_task_schedule_to_close_timeout
    #   The optional default maximum duration, specified when registering the
    #   activity type, for tasks of this activity type. You can override this
    #   default when scheduling a task through the `ScheduleActivityTask`
    #   decision.
    #
    #   The valid values are integers greater than or equal to zero, or the
    #   string "NONE". An integer value can be used to specify the duration in
    #   seconds while "NONE" is be used to specify *unlimited* duration.
    #
    # @!attribute default_task_schedule_to_start_timeout
    #   The optional default maximum duration, specified when registering the
    #   activity type, that a task of an activity type can wait before being
    #   assigned to a worker. You can override this default when scheduling a
    #   task through the `ScheduleActivityTask` decision.
    #
    # @!attribute default_task_start_to_close_timeout
    #   The optional default maximum duration for tasks of an activity type
    #   specified when registering the activity type. You can override this
    #   default when scheduling a task through the `ScheduleActivityTask`
    #   decision.
    #
    class ActivityOptions < Options

      properties(
        :heartbeat_timeout,
        :task_list,
        :schedule_to_close_timeout,
        :schedule_to_start_timeout,
        :start_to_close_timeout,
        :task_priority,
        :version,
        :input
      )

      property(:manual_completion, [lambda {|x| x == true}])
      property(:data_converter, [])

      default_classes << ActivityDefaults.new

      # Gets the activity prefix name.
      #
      # @return [String]
      #   The activity name.
      #
      def activity_name
        @prefix_name
      end

      # Sets the activity prefix name.
      #
      # @param [String] value
      #   The activity name to set.
      #
      def activity_name=(value)
        @prefix_name = value
      end

      # Creates a new set of `ActivityOptions`.
      #
      # @param [Hash] default_options
      #   A set of `ActivityOptions` to use as the default values.
      #
      # @option default_options [Integer] :heartbeat_timeout
      #   The optional default maximum time, specified when registering the
      #   activity type, before which a worker processing a task must report
      #   progress by calling `RecordActivityTaskHeartbeat`.
      #
      #   You can override this default when scheduling a task through the
      #   `ScheduleActivityTask` decision. If the activity worker subsequently
      #   attempts to record a heartbeat or returns a result, the activity
      #   worker receives an UnknownResource fault. In this case, Amazon SWF no
      #   longer considers the activity task to be valid; the activity worker
      #   should clean up the activity task.
      #
      # @option default_options [Integer] :schedule_to_close_timeout
      #   The optional default maximum duration, specified when registering the
      #   activity type, for tasks of this activity type.
      #
      #   You can override this default when scheduling a task through the
      #   `ScheduleActivityTask` decision.
      #
      # @option default_options [Integer] :schedule_to_start_timeout
      #   The optional default maximum duration, specified when registering the
      #   activity type, that a task of an activity type can wait before being
      #   assigned to a worker.
      #
      #   You can override this default when scheduling a task through the
      #   `ScheduleActivityTask` decision.
      #
      # @option default_options [Integer] :start_to_close_timeout
      #   The optional default maximum duration for tasks of an activity type
      #   specified when registering the activity type.
      #
      #   You can override this default when scheduling a task through the
      #   `ScheduleActivityTask` decision.
      #
      # @option default_options [Array] :task_list
      #   The optional default task list specified for this activity type at
      #   registration. This default task list is used if a task list is not
      #   provided when a task is scheduled through the ScheduleActivityTask
      #   decision.
      #
      # @option default_options [Array] :task_priority
      #   The optional default task priority specified for this activity type at
      #   registration. This default task priority is used if a task priority is not
      #   provided when a task is scheduled through the ScheduleActivityTask
      #   decision.
      #
      #   You can override this default when scheduling a task through the
      #   `ScheduleActivityTask` decision.
      #
      # @option default_options [String] :version
      #  The version of this activity. If you change any other options on the
      #  activity, you must also change the version.
      #
      # @param [true, false] use_defaults
      #   Set to `true` to use the pre-defined {ActivityDefaults}.
      #
      def initialize(default_options={}, use_defaults=false)
        if default_options.keys.include? :exponential_retry
          @_exponential_retry = ExponentialRetryOptions.new(default_options[:exponential_retry])
        end
        super(default_options, use_defaults)
      end

      property(:_exponential_retry, [])

      # Retries the supplied block with exponential retry logic.
      #
      # @param [Hash] block
      #   A hash of {ExponentialRetryOptions}.
      #
      def exponential_retry(&block)
        retry_options = Utilities::interpret_block_for_options(ExponentialRetryOptions, block)
        @_exponential_retry = retry_options
      end

      # Return the full set of options for the Activity.
      def get_full_options
        result = {}
        usable_properties = self.class.held_properties
        usable_properties.delete(:from_class)
        usable_properties.each do |option|
          result[option] = self.send(option) if self.send(option) != nil && self.send(option) != ""
        end
        result
      end
    end

    # This class is used to capture the options passed during activity declaration.
    class ActivityRegistrationOptions < ActivityOptions
      class << self
        # Default registration options. These can be set only during Activity
        # registration.
        #
        # * `default_task_heartbeat_timeout`
        # * `default_task_schedule_to_close_timeout`
        # * `default_task_schedule_to_start_timeout`
        # * `default_task_start_to_close_timeout`
        #
        def registration_options
          [
            :default_task_heartbeat_timeout,
            :default_task_schedule_to_close_timeout,
            :default_task_schedule_to_start_timeout,
            :default_task_start_to_close_timeout,
            :default_task_list,
            :default_task_priority
          ]
        end
      end

      # Adding default properties
      properties(
        :default_task_heartbeat_timeout,
        :default_task_list,
        :default_task_schedule_to_close_timeout,
        :default_task_schedule_to_start_timeout,
        :default_task_start_to_close_timeout,
        :default_task_priority
      )

      default_classes << ActivityRegistrationDefaults.new

      # Return the options for this Activity that are set during registration.
      def get_registration_options
        get_options(self.class.registration_options)
      end
    end

    # Runtime options for an activity.
    class ActivityRuntimeOptions < ActivityOptions

      # Creates a new set of runtime options based on a set of default options.
      #
      # @param [ActivityOptions] default_options
      #   The default {ActivityOptions} to use to set the values of the runtime
      #   options in this class.
      #
      # @param [true, false] use_defaults
      #   Set to `true` to use the default runtime options for the activity. The
      #   default is `false`.
      #
      def initialize(default_options = {}, use_defaults=false)
        super(default_options, use_defaults)
      end
    end
  end
end

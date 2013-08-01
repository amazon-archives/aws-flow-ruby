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
      def method_missing(method_name, *args, &block)
        return nil
      end
    end

    # The base class for all options classes in the AWS Flow Framework for Ruby.
    class Options
      extend Utilities::UpwardLookups
      include Utilities::UpwardLookups::InstanceMethods
      def method_missing(method_name, *args, &block)
        return nil
      end

      # Sets the default classes on a child class (a class derived from {Options}).
      def self.inherited(child)
        child.precursors ||= []
        default_classes = child.ancestors.map do |precursor|
          precursor.default_classes if precursor.methods.map(&:to_sym).include? :default_classes
        end.compact.flatten
        child.instance_variable_set("@default_classes", default_classes)
      end

      class << self
        # The set of default options. These are used when `use_defaults` is set to `true` on {#initialize}.
        attr_accessor :default_classes
      end

      # Merges specified options with the set of options already held by the class, and returns the result.
      #
      # @return [Hash]
      #   The merged set of options, defined as a hash.
      #
      # @param [Options] options
      #   An {Options}-derived class containing a set of options to use if this instance has no options, or options to
      #   add to this one if this instance already has options.
      #
      # @param [Hash] extra_to_add
      #   A hash containing extra options to merge with the options held by the class and those provided in the
      #   +options+ parameter.
      #
      def get_options(options, extra_to_add = {})
        options = self.class.held_properties.compact if options.empty?
        set_options = options.select {|option| self.send(option) != nil && self.send(option) != "" }
        option_values = set_options.map {|option| self.send(option) == Float::INFINITY ? "NONE" : self.send(option) }
        result = Hash[set_options.zip(option_values)]
        result.merge(extra_to_add)
      end

      # Creates a new {Options} instance.
      #
      # @param [Hash] hash
      #   A hash of values to use as the default options for this instance. The members of this hash are defined by
      #   classes derived from {Options}.
      #
      # @param [true, false] use_defaults
      #   In derived classes, this parameter is used to tell the constructor to use the set of default options as the
      #   runtime options. This has no effect in the base {Options} class.
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

    class WorkerDefaults < Defaults
      def use_forking; true; end
    end

    # Options for Activity and Workflow workers.
    #
    # @!attribute logger
    #   The logger to use for the worker.
    #
    # @!attribute poller_workers
    #   The logger to use for the worker.
    #
    # @!attribute execution_workers
    #   The logger to use for the worker.
    #
    class WorkerOptions < Options
      property(:logger, [])
      # At current, we only support one poller per worker
      # property(:poller_workers, [lambda(&:to_i)])
      property(:execution_workers, [lambda(&:to_i)])
      property(:use_forking, [lambda {|x| x == true}] )
      default_classes << WorkerDefaults.new
    end

    # Options for WorkflowClient#signal_workflow_execution.
    #
    # @!attribute control
    #   Optional data attached to the signal that can be used by the workflow execution.
    #
    # @!attribute domain
    #   *Required*. The name of the domain containing the workflow execution to signal.
    #
    # @!attribute input
    #   Data to attach to the WorkflowExecutionSignaled event in the target workflow execution's history.
    #
    # @!attribute run_id
    #   The runId of the workflow execution to signal.
    #
    # @!attribute signal_name
    #   *Required*. The name of the signal. This name must be meaningful to the target workflow.
    #
    # @!attribute workflow_id
    #   *Required*. The workflowId of the workflow execution to signal.
    #
    class SignalWorkflowOptions < Options
      properties(:input, :signal_name, :run_id, :workflow_id, :control, :domain)

      # Gets a hash containing the held options.
      def get_full_options
        result = {}
        SignalWorkflowOptions.held_properties.each do |option|
          result[option] = self.send(option) if self.send(option) && self.send(option) != ""
        end
        result
      end
    end

    # Defaults for {RetryOptions}
    class RetryDefaults < Defaults
      #  The default maximum number of attempts to make before the task is marked as failed.
      def maximum_attempts; FlowConstants.exponential_retry_maximum_attempts; end
      #  The default retry function to use.
      def retry_function; FlowConstants.exponential_retry_function; end
      def exceptions_to_include; FlowConstants.exponential_retry_exceptions_to_include; end
      def exceptions_to_exclude; FlowConstants.exponential_retry_exceptions_to_exclude; end
      def should_jitter; FlowConstants.should_jitter; end
      def jitter_function; FlowConstants.jitter_function; end
    end

    # Retry options used with {GenericClient#retry} and {ActivityClient#exponential_retry}
    class RetryOptions < Options
      property(:is_retryable_function, [])
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
      #  The number of exceptions to allow
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
      #   If set to `true`, the default options specified will be used as the runtime options.
      #
      def initialize(hash={}, use_defaults=false)
        super(hash, use_defaults)
      end

      # Tests whether or not this activity can be retried based on the `:exceptions_to_retry` and
      # `:exceptions_to_exclude` options.
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

    # Exponential retry options for the {ActivityClient#exponential_retry} method.
    class ExponentialRetryOptions < RetryOptions
      # The backoff coefficient to use. This is a floating point value that is multiplied with the current retry
      # interval after every retry attempt. The default value is 2.0, which means that each retry will take twice as
      # long as the previous.
      attr_accessor :backoff_coefficient

      # The retry expiration interval, in seconds. This will be increased after every retry attempt by the factor
      # provided in +backoff_coefficient+.
      attr_accessor :retry_expiration_interval_seconds

      def next_retry_delay_seconds(first_attmept, recorded_failure, attempts)
        raise IllegalArgumentException "Attempt number is #{attempts}, when it needs to be greater than 1"
        if @maximum_attempts
        end
      end
    end

    # Defaults for WorkflowOptions
    class WorkflowDefaults < Defaults

      # The default task start-to-close timeout duration.
      def task_start_to_close_timeout; 30; end

      # The default child workflow policy
      def child_policy; :TERMINATE; end

      # Returns a list of tags (currently an empty array).
      def tag_list; []; end

      def data_converter; FlowConstants.default_data_converter; end
    end

    # Options for workflows
    #
    # @!attribute child_policy
    #   The optional policy to use for the child workflow executions when a workflow execution of this type is
    #   terminated, by calling the TerminateWorkflowExecution action explicitly or due to an expired timeout. This can
    #   be overridden when starting a workflow execution using the StartWorkflowExecution action or the
    #   StartChildWorkflowExecution Decision. The supported child policies are:
    #
    #   * *TERMINATE*: the child executions will be terminated.
    #   * *REQUEST_CANCEL*: a request to cancel will be attempted for each child execution by recording a
    #     WorkflowExecutionCancelRequested event in its history. It is up to the decider to take appropriate actions
    #     when it receives an execution history with this event.
    #   * *ABANDON*: no action will be taken. The child executions will continue to run.
    #
    #   The default is TERMINATE.
    #
    # @!attribute execution_method
    #   TBD
    #
    # @!attribute execution_start_to_close_timeout
    #   The optional maximum duration, specified when registering the workflow type, for executions of this workflow
    #   type. This default can be overridden when starting a workflow execution using the StartWorkflowExecution action
    #   or the StartChildWorkflowExecution decision.
    #
    #   The valid values are integers greater than or equal to 0. An integer value can be used to specify the duration
    #   in seconds while NONE can be used to specify unlimited duration.
    #
    # @!attribute input
    #   A string of up to 32768 characters, to be provided to the workflow execution.
    #
    # @!attribute tag_list
    #   The list of tags to associate with the child workflow execution. A maximum of five tags can be specified. You
    #   can list workflow executions with a specific tag by calling ListOpenWorkflowExecutions or
    #   ListClosedWorkflowExecutions and specifying a TagFilter.
    #
    # @!attribute task_list
    #   The optional task list, specified when registering the workflow type, for decisions tasks scheduled for workflow
    #   executions of this type. This default can be overridden when starting a workflow execution using the
    #   StartWorkflowExecution action or the StartChildWorkflowExecution Decision.
    #
    # @!attribute task_start_to_close_timeout
    #   The optional maximum duration, specified when registering the workflow type, that a decision task for executions
    #   of this workflow type might take before returning completion or failure. If the task does not close in the
    #   specified time then the task is automatically timed out and rescheduled. If the decider eventually reports a
    #   completion or failure, it is ignored. This default can be overridden when starting a workflow execution using
    #   the StartWorkflowExecution action or the StartChildWorkflowExecution Decision.
    #
    #   The valid values are integers greater than or equal to 0. An integer value can be used to specify the duration
    #   in seconds while NONE can be used to specify unlimited duration.
    #
    #   The default is 30.
    #
    # @!attribute version
    #   The version of the Workflow. If you update any of these options, you must update the version.
    #
    # @!attribute workflow_id
    #   *Required*. The workflow id of the workflow execution.
    #
    #   The specified string must not start or end with whitespace. It must not contain a `:` (colon), `/` (slash), `|`
    #   (vertical bar), or any control characters (\u0000-\u001f | \u007f - \u009f). Also, it must not contain the
    #   literal string "arn".
    #
    class WorkflowOptions < Options
      properties(:version, :input, :workflow_id, :execution_start_to_close_timeout, :task_start_to_close_timeout, :task_list, :execution_method)
      property(:tag_list, [])
      property(:child_policy, [lambda(&:to_s), lambda(&:upcase)])
      property(:data_converter, [])
      default_classes << WorkflowDefaults.new

      # Returns a hash containing the runtime workflow options.
      #
      # @return [Hash] a hash of options with corresponding values.
      #
      def get_full_options
        result = {}
        usable_properties = self.class.held_properties
        usable_properties.delete(:from_class)
        usable_properties.each do |option|
          result[option] = self.send(option) if self.send(option) && self.send(option) != ""
        end
        result
      end
    end

    class WorkflowOptionsWithDefaults < WorkflowOptions
      properties(:default_task_start_to_close_timeout, :default_execution_start_to_close_timeout, :default_task_list)
      property(:default_child_policy, [lambda(&:to_s), lambda(&:upcase)])
    end

    # Options for #start_workflow
    #
    # @!attribute workflow_name
    #   The name of this workflow.
    #
    # @!attribute from_class
    #   If present, options from the specified class will be used as the workflow execution options.
    #
    class StartWorkflowOptions < WorkflowOptions
      properties(:workflow_name, :from_class)
    end

    # Options for {AsyncDecider#continue_as_new_workflow} and {Workflows.InstanceMethods#continue_as_new}.
    #
    class ContinueAsNewOptions < WorkflowOptions
    end

    # Defaults for the {ActivityOptions} class
    class ActivityDefaults < Defaults

      # The default Schedule to Close timeout for activity tasks. This timeout represents the time, in seconds, between
      # when the activity task is first scheduled to when it is closed (whether due to success, failure, or a timeout).
      #
      # This default can be overridden when scheduling an activity task. You can set this value to "NONE" to imply no
      # timeout value.
      #
      def default_task_schedule_to_close_timeout;  Float::INFINITY; end

      # The default maximum time in seconds before which a worker processing a task of this type must report progress.
      # If the timeout is exceeded, the activity task is automatically timed out. If the worker subsequently attempts to
      # record a heartbeat or returns a result, it will be ignored.
      #
      # This default can be overridden when scheduling an activity task. You can set this value to "NONE" to imply no
      # timeout value.
      #
      def default_task_heartbeat_timeout; Float::INFINITY; end

      # The default Schedule to Close timeout. This timeout represents the time between when the activity task is first
      # scheduled to when it is closed (whether due to success, failure, or a timeout).
      #
      # This default can be overridden when scheduling an activity task. You can set this value to "NONE" to imply no
      # timeout value.
      #
      def schedule_to_close_timeout; Float::INFINITY; end

      # The default maximum time before which a worker processing a task of this type must report progress. If the
      # timeout is exceeded, the activity task is automatically timed out. If the worker subsequently attempts to record
      # a heartbeat or returns a result, it will be ignored. This default can be overridden when scheduling an activity
      # task.
      #
      # This default can be overridden when scheduling an activity task. You can set this value to "NONE" to imply no
      # timeout value.
      #
      def heartbeat_timeout; Float::INFINITY; end

      def data_converter; FlowConstants.default_data_converter; end
    end


    # Options to use on an activity or decider. The following options are defined:
    #
    # @!attribute default_task_heartbeat_timeout
    #
    #   The optional default maximum time, specified when registering the activity type, before which a worker
    #   processing a task must report progress by calling
    #   {http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/ActivityTask.html#record_heartbeat!-instance_method
    #   record_heartbeat} on the ActivityTask.
    #
    #   You can override this default when scheduling a task through the ScheduleActivityTask Decision. If the
    #   activity worker subsequently attempts to record a heartbeat or returns a result, the activity worker receives
    #   an UnknownResource fault. In this case, Amazon SWF no longer considers the activity task to be valid; the
    #   activity worker should clean up the activity task.
    #
    #   The valid values are integers greater than or equal to zero. An integer value can be used to specify the
    #   duration in seconds while "NONE" can be used to specify unlimited duration.
    #
    # @!attribute default_task_list
    #
    #   The optional default task list specified for this activity type at registration. This default task list is
    #   used if a task list is not provided when a task is scheduled through the ScheduleActivityTask decision. You
    #   can override this default when scheduling a task through the ScheduleActivityTask decision.
    #
    # @!attribute default_task_schedule_to_close_timeout
    #
    #   The optional default maximum duration, specified when registering the activity type, for tasks of this
    #   activity type. You can override this default when scheduling a task through the ScheduleActivityTask Decision.
    #
    #   The valid values are integers greater than or equal to zero, or the string "NONE". An integer value can be
    #   used to specify the duration in seconds while "NONE" is be used to specify *unlimited* duration.
    #
    # @!attribute default_task_schedule_to_start_timeout
    #
    #   The optional default maximum duration, specified when registering the activity type, that a task of an
    #   activity type can wait before being assigned to a worker. You can override this default when scheduling a task
    #   through the ScheduleActivityTask decision.
    #
    # @!attribute default_task_start_to_close_timeout
    #
    #   The optional default maximum duration for tasks of an activity type specified when registering the activity
    #   type. You can override this default when scheduling a task through the ScheduleActivityTask decision.
    class ActivityOptions < Options
      # The default options for the activity. These can be specified when creating a new ActivityOptions instance.
      #
      class << self
        attr_reader :default_options,  :runtime_options
      end
      properties(:default_task_heartbeat_timeout, :default_task_list, :default_task_schedule_to_close_timeout, :default_task_schedule_to_start_timeout, :default_task_start_to_close_timeout, :heartbeat_timeout, :task_list, :schedule_to_close_timeout, :schedule_to_start_timeout, :start_to_close_timeout, :version, :input)
      property(:manual_completion, [lambda {|x| x == true}])
      property(:data_converter, [])

      default_classes << ActivityDefaults.new

      # Gets the activity prefix name
      # @return [String] the activity name
      def activity_name
        @prefix_name
      end

      # Sets the activity prefix name
      # @param [String] value the activity name to set
      def activity_name=(value)
        @prefix_name = value
      end

      # Creates a new set of ActivityOptions
      #
      # @param [Hash] default_options
      #   A set of ActivityOptions to use as the default values.
      #
      # @option default_options [Integer] :heartbeat_timeout
      #   The optional default maximum time, specified when registering the activity type, before which a worker
      #   processing a task must report progress by calling RecordActivityTaskHeartbeat. You can override this default
      #   when scheduling a task through the ScheduleActivityTask Decision. If the activity worker subsequently attempts
      #   to record a heartbeat or returns a result, the activity worker receives an UnknownResource fault. In this
      #   case, Amazon SWF no longer considers the activity task to be valid; the activity worker should clean up the
      #   activity task.
      #
      # @option default_options [Integer] :schedule_to_close_timeout
      #   The optional default maximum duration, specified when registering the activity type, for tasks of this
      #   activity type. You can override this default when scheduling a task through the ScheduleActivityTask Decision.
      #
      # @option default_options [Integer] :schedule_to_start_timeout
      #   The optional default maximum duration, specified when registering the activity type, that a task of an
      #   activity type can wait before being assigned to a worker. You can override this default when scheduling a task
      #   through the ScheduleActivityTask Decision.
      #
      # @option default_options [Integer] :start_to_close_timeout
      #   The optional default maximum duration for tasks of an activity type specified when registering the activity
      #   type. You can override this default when scheduling a task through the ScheduleActivityTask Decision.
      #
      # @option default_options [Array] :task_list
      #   The optional default task list specified for this activity type at registration. This default task list is
      #   used if a task list is not provided when a task is scheduled through the ScheduleActivityTask Decision. You
      #   can override this default when scheduling a task through the ScheduleActivityTask Decision.
      #
      # @option default_options [String] :version
      #  The version of this Activity. If you change any other options on the activity, you must also change the
      #  version.
      #
      # @param [true, false] use_defaults
      #   Set to `true` to use the pre-defined default ActivityOptions.
      #
      def initialize(default_options={}, use_defaults=false)
        if default_options.keys.include? :exponential_retry
          @_exponential_retry = ExponentialRetryOptions.new(default_options[:exponential_retry])
        end
        super(default_options, use_defaults)
      end

      # Retrieves the runtime options for this Activity. The runtime options returned are:
      #
      # * :heartbeat_timeout
      # * :task_list
      # * :schedule_to_close_timeout
      # * :schedule_to_start_timeout
      # * :start_to_close_timeout
      #
      # For a description of each of these options, see {#initialize}.
      #
      # @return [Hash]
      #   The runtime option names and their current values.
      #
      def get_runtime_options
        result = get_options([:heartbeat_timeout, :task_list, :schedule_to_close_timeout, :schedule_to_start_timeout, :start_to_close_timeout])
        default_options = get_options([:default_task_heartbeat_timeout, :default_task_schedule_to_close_timeout, :default_task_schedule_to_start_timeout, :default_task_start_to_close_timeout])
        default_option_keys, default_option_values = default_options.keys, default_options.values
        default_option_keys.map! { |option| option.to_s.gsub(/default_task_/, "").to_sym }
        default_hash = Hash[default_option_keys.zip(default_option_values)]
        default_hash.merge(result)
      end

      property(:_exponential_retry, [])

      # Retries the supplied block with exponential retry logic.
      #
      # @param [Hash] block
      #   A hash of ExponentialRetryOptions.
      #
      def exponential_retry(&block)
        retry_options = Utilities::interpret_block_for_options(ExponentialRetryOptions, block)
        @_exponential_retry = retry_options
      end

      # Retrieves the runtime options for this Activity.
      #
      # @return [Hash]
      #   A hash containing the runtime option names and their current values.
      #
      def get_full_options
        options_hash = self.get_runtime_options
        [:task_list, :version, :_exponential_retry, :prefix_name, :return_on_start, :manual_completion, :data_converter].each do |attribute|
          options_hash.merge!(attribute => self.send(attribute)) if self.send(attribute)
        end
        options_hash
      end

      # Retrieves the default options for this Activity.
      #
      # @return [Hash]
      #   A hash containing the default option names and their current values.
      #
      #   The options retrieved are:
      #
      #   * :default_task_heartbeat_timeout
      #   * :default_task_schedule_to_close_timeout
      #   * :default_task_schedule_to_start_timeout
      #   * :default_task_start_to_close_timeout
      #
      def get_default_options
        get_options([:default_task_heartbeat_timeout, :default_task_schedule_to_close_timeout, :default_task_schedule_to_start_timeout, :default_task_start_to_close_timeout])
      end
    end

    # Runtime options for an Activity.
    class ActivityRuntimeOptions < ActivityOptions

      # Creates a new set of runtime options based on a set of default options.
      #
      # @param [ActivityOptions] default_options
      #   The default {ActivityOptions} to use to set the values of the runtime options in this class.
      #
      # @param [true, false] use_defaults
      #   Set to `true` to use the default runtime options for the Activity.
      #
      def initialize(default_options = {}, use_defaults=false)
        super(default_options, use_defaults)
      end
    end
  end
end

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

    # Represents information about an open request.
    # @api private
    class OpenRequestInfo
      attr_accessor :completion_handle, :result, :blocking_promise, :description, :run_id
    end

    # Contains metadata about an activity.
    # @api private
    class ActivityMetadata
      # The stored ID of the activity.
      attr_reader :activity_id

      # Initializes a new ActivityMetadata object.
      #
      # @param activity_id
      #   The ID of the activity.
      #
      def initialize(activity_id); @activity_id = activity_id; end
    end

    # A generic activity client that can be used to perform standard activity actions.
    class GenericActivityClient < GenericClient

      # The data converter used for serializing/deserializing data when sending
      # requests to and receiving results from workflow executions of this
      # workflow type. By default, this is {YAMLDataConverter}.
      attr_accessor :data_converter

      # The decision helper used by the activity client.
      attr_accessor :decision_helper

      # A hash of {ActivityRuntimeOptions} for the activity client.
      attr_accessor :options

      # Returns the default option class for the activity client, which is {ActivityRuntimeOptions}.
      def self.default_option_class; ActivityRuntimeOptions; end

      # Separates the activity name from the activity type at the point of the last period.
      #
      # @param [String] name
      #   The name of the activity type.
      #
      # @api private
      def activity_name_from_activity_type(name)
        return name.to_s.split(".").last.to_sym
      end

      # Creates a new GenericActivityClient instance.
      #
      # @param [DecisionHelper] decision_helper
      #   The decision helper to use for the activity client.
      #
      # @param [ActivityOptions] options
      #   The activity options to set for the activity client.
      #
      def initialize(decision_helper, options)
        @decision_helper = decision_helper
        @options = options
        @activity_option_map = @decision_helper.activity_options
        @failure_map = {}
        @data_converter ||= YAMLDataConverter.new
        super
      end

      # Registers and schedules a new activity type, provided a name and block of options.
      #
      # @param method_name
      #   *Required*. The name of the activity type to define.
      #
      # @param args
      #   *Required*. Arguments for the method provided in *method_name*.
      #
      # @param block
      #   *Required*. A block of {ActivityOptions} to use when registering the new {ActivityType}. This can be set to an
      #   empty block, in which case, the default activity options will be used.
      #
      def method_missing(method_name, *args, &block)
        options = Utilities::interpret_block_for_options(ActivityOptions, block)
        client_options = Utilities::client_options_from_method_name(method_name, @options)

        options = Utilities::merge_all_options(client_options,
                                    @activity_option_map[method_name.to_sym],
                                    @option_map[method_name.to_sym],
                                    options
                                    )
        new_options = ActivityOptions.new(options)

        activity_type = ActivityType.new("#{new_options.prefix_name}.#{method_name.to_s}", new_options.version, new_options.get_registration_options)
        if new_options._exponential_retry
          retry_function = new_options._exponential_retry.retry_function || FlowConstants.exponential_retry_function
          new_options._exponential_retry.return_on_start ||= new_options.return_on_start
          future = _retry_with_options(lambda { self.schedule_activity(activity_type.name, activity_type, args, new_options ) }, retry_function, new_options._exponential_retry, args)
          return future if new_options.return_on_start
          result = Utilities::drill_on_future(future)
        else
          result = schedule_activity(activity_type.name, activity_type, args, new_options)
        end
        result
      end

      # @api private
      def retry_alias_to_method(retry_alias)
        retry_alias.to_s[/__(.*)_retry/, 1].to_sym
      end

      # @api private
      def method_to_retry_alias(method_name)
        "#{__method_name.to_s + "_retry"}".to_sym
      end

      # Requests that the activity is canceled.
      #
      # @param [WorkflowFuture] to_cancel
      #   The {WorkflowFuture} for the task to be canceled.
      #
      def request_cancel_activity_task(to_cancel)
        metadata = to_cancel.metadata
        if ! metadata.respond_to? :activity_id
          raise "You need to use a future obtained from an activity"
        end
        @decision_helper[metadata.activity_id].consume(:cancel)
      end

      # A handler for the `ActivityClassCanceled` event.
      #
      # @param [AWS::SimpleWorkflow::HistoryEvent] event
      #   The event data.
      #
      def handle_activity_task_canceled(event)
        activity_id = @decision_helper.get_activity_id(event.attributes[:scheduled_event_id])
        @decision_helper[activity_id].consume(:handle_cancellation_event)
        if @decision_helper[activity_id].done?
          open_request = @decision_helper.scheduled_activities.delete(activity_id)
          exception = CancellationException.new("Cancelled from ActivityTaskCanceledEvent", nil)
          if ! open_request.nil?
            open_request.completion_handle.fail(exception)
          end
        end
      end

      # A handler for the `ActivityClassTimedOut` event.
      #
      # @param [AWS::SimpleWorkflow::HistoryEvent] event
      #   The event data.
      #
      def handle_activity_task_timed_out(event)
        activity_id = @decision_helper.get_activity_id(event.attributes[:scheduled_event_id])
        activity_state_machine = @decision_helper[activity_id]
        activity_state_machine.consume(:handle_completion_event)
        if activity_state_machine.done?
          open_request = @decision_helper.scheduled_activities.delete(activity_id)
          if ! open_request.nil?
            timeout_type = event.attributes[:timeout_type]
            failure = ActivityTaskTimedOutException.new(event.id, activity_id, timeout_type, "Time out")
            open_request.completion_handle.fail(failure)
          end
        end
      end

      # A handler for the `ActivityTaskFailed` event.
      #
      # @param [ActivityClassFailed] event
      #   The event data.
      #
      def handle_activity_task_failed(event)
        attributes = event.attributes
        activity_id = @decision_helper.get_activity_id(attributes[:scheduled_event_id])
        @decision_helper[activity_id].consume(:handle_completion_event)
        open_request_info = @decision_helper.scheduled_activities.delete(activity_id)
        reason = attributes[:reason] if attributes.keys.include? :reason
        reason ||= "The activity which failed did not provide a reason"
        details = attributes[:details] if attributes.keys.include? :details
        details ||= "The activity which failed did not provide details"

        # TODO consider adding user_context to open request, and adding it here
        # @decision_helper[@decision_helper.activity_scheduling_event_id_to_activity_id[event.attributes.scheduled_event_id]].attributes[:options].data_converter
        failure = ActivityTaskFailedException.new(event.id, activity_id, reason, details)
        open_request_info.completion_handle.fail(failure)
      end

      # A handler for the `ScheduleActivityTaskFailed` event.
      #
      # @param [AWS::SimpleWorkflow::HistoryEvent] event
      #   The event data.
      #
      def handle_schedule_activity_task_failed(event)
        attributes = event.attributes
        activity_id = attributes[:activity_id]
        open_request_info = @decision_helper.scheduled_activities.delete(activity_id)
        activity_state_machine = @decision_helper[activity_id]
        activity_state_machine.consume(:handle_initiation_failed_event)
        if activity_state_machine.done?
          # TODO Fail task correctly
          failure = ScheduleActivityTaskFailedException.new(event.id, event.attributes.activity_type, activity_id, event.attributes.cause)
          open_request_info.completion_handle.fail(failure)
        end
      end

      # A handler for the `ActivityClassCompleted` event.
      #
      # @param [AWS::SimpleWorkflow::HistoryEvent] event
      #   The event data.
      #
      def handle_activity_task_completed(event)
        scheduled_id = event.attributes[:scheduled_event_id]
        activity_id = @decision_helper.activity_scheduling_event_id_to_activity_id[scheduled_id]
        @decision_helper[activity_id].consume(:handle_completion_event)
        if @decision_helper[activity_id].done?
          open_request = @decision_helper.scheduled_activities.delete(activity_id)
          open_request.result = event.attributes[:result]
          open_request.completion_handle.complete
        end
      end

      # Schedules a named activity.
      #
      # @param [String] name
      #   *Required*. The name of the activity to schedule.
      #
      # @param [String] activity_type
      #   *Required*. The activity type for this scheduled activity.
      #
      # @param [Object] input
      #   *Required*. Additional data passed to the activity.
      #
      # @param [ActivityOptions] options
      #   *Required*. {ActivityOptions} to set for the scheduled activity.
      #
      def schedule_activity(name, activity_type, input, options)
        options = Utilities::merge_all_options(@option_map[activity_name_from_activity_type(name)], options)
        new_options = ActivityOptions.new(options)
        output = Utilities::AddressableFuture.new
        open_request = OpenRequestInfo.new
        decision_id = @decision_helper.get_next_id(:Activity)
        output._metadata = ActivityMetadata.new(decision_id)
        error_handler do |t|
          t.begin do
            @data_converter = new_options.data_converter

            #input = input.map { |input_part| @data_converter.dump input_part } unless input.nil?
            input = @data_converter.dump input unless input.empty?
            attributes = {}
            new_options.input ||= input unless input.empty?
            attributes[:options] = new_options
            attributes[:activity_type] = activity_type
            attributes[:decision_id] = decision_id
            @completion_handle = nil
            external_task do |t|
              t.initiate_task do |handle|
                open_request.completion_handle = handle

                @decision_helper.scheduled_activities[decision_id.to_s] = open_request
                @decision_helper[decision_id.to_s] = ActivityDecisionStateMachine.new(decision_id, attributes)
              end
              t.cancellation_handler do |this_handle, cause|
                state_machine = @decision_helper[decision_id.to_s]
                if state_machine.current_state == :created
                  open_request = @decision_helper.scheduled_activities.delete(decision_id.to_s)
                  open_request.completion_handle.complete
                end
                state_machine.consume(:cancel)
              end
            end
          end
          t.rescue(Exception) do |error|
            @data_converter = new_options.data_converter
            # If we have an ActivityTaskFailedException, then we should figure
            # out what the cause was, and pull that out. If it's anything else,
            # we should serialize the error, and stuff that into details, so
            # that things above us can pull it out correctly. We don't have to
            # do this for ActivityTaskFailedException, as the details is
            # *already* serialized.
            if error.is_a? ActivityTaskFailedException
              details = @data_converter.load(error.details)
              error.cause = details
            else
              details = @data_converter.dump(error)
              error.details = details
            end
            @failure_map[decision_id.to_s] = error
          end
          t.ensure do
            @data_converter = new_options.data_converter
            result = @data_converter.load open_request.result
            output.set(result)
            raise @failure_map[decision_id.to_s] if @failure_map[decision_id.to_s] && new_options.return_on_start
          end
        end
        return output if new_options.return_on_start
        output.get
        this_failure = @failure_map[decision_id.to_s]
        raise this_failure if this_failure
        return output.get
      end
    end

    # Represents an activity client.
    # @api private
    class ActivityClient
      # TODO -- Is this class used anywhere? It might be helpful to remove it,
      # if it is not. Most of its functionality seems to be implemented in the
      # GenericClient and GenericActivityClient classes.

      # Gets the data converter for the activity client.
      # @api private
      def data_converter
        @generic_client.data_converter
      end

      # Sets the data converter for the activity client.
      #
      # @param other
      #   The data converter to set.
      #
      # @api private
      def data_converter=(other)
        @generic_client.data_converter = other
      end

      # Exponentially retries the supplied method with optional settings.
      #
      # @param [String] method_name
      #   The method name to retry.
      #
      # @param [ExponentialRetryOptions] block
      #   A hash of {ExponentialRetryOptions} to use.
      #
      # @api private
      def exponential_retry(method_name, &block)
        @generic_client.retry(method_name, lambda {|first, time_of_failure, attempts| 1}, block)
      end
    end

    # Methods and constants related to activities.
    #
    # @!attribute activity_client
    #   Gets the {ActivityClient} contained by the class.
    #
    # @!attribute activities
    #   Gets the list of {ActivityType} objects that were created by the
    #   {#activity} method.
    #
    module Activities
      @precursors ||= []
      attr_accessor :activity_client, :activities
      def self.extended(base)
        base.send :include, InstanceMethods
      end
      module InstanceMethods
        # Sets the {ActivityExecutionContext} instance for the activity task.
        attr_writer :_activity_execution_context

        # Gets the activity execution context for the activity task. Raises an `IllegalStateException` if the activity
        # has no context.
        #
        # @return [ActivityExecutionContext] The execution context for this activity.
        #
        def activity_execution_context
          raise IllegalStateException.new("No activity execution context") unless @_activity_execution_context
          @_activity_execution_context
        end

        # Records a heartbeat for the activity, indicating to Amazon SWF that the activity is still making progress.
        #
        # @param [String] details
        #   If specified, contains details about the progress of the activity task. Up to 2048
        #   characters can be provided.
        #
        def record_activity_heartbeat(details)
          @_activity_execution_context.record_activity_heartbeat(details)
        end
      end

      # @api private
      extend Utilities::UpwardLookups

      # @api private
      def look_upwards(variable)
        precursors = self.ancestors.dup
        precursors.delete(self)
        results = precursors.map { |x| x.send(variable) if x.methods.map(&:to_sym).include? variable }.compact.flatten.uniq
      end
      property(:activities, [])

      # @api private
      def _options; @activities; end

      # Defines one or more activities with {ActivityRegistrationOptions} provided in the
      # supplied block.
      #
      # @param [Array] activity_names
      #   The names of the activities to define. These names will be used to
      #   create {ActivityType} objects, one per name.
      #
      #   Each activity type is named as *prefix.activity_name*, where the
      #   *prefix* is specified in the options block, and each *activity_name*
      #   comes from the list passed to this parameter.
      #
      # @param [Hash] block
      #   {ActivityRegistrationOptions} to use on the defined activities.
      #
      #   The following options are *required* when registering an activity:
      #
      #   * `version` - The version of the activity type.
      #   * `task_list` - The task list used to poll for activity tasks.
      #
      # @example Defining an activity
      #   new_activity_class = Class.new(MyActivity) do
      #     extend Activities
      #
      #     activity :activity1 do
      #     {
      #       :default_task_heartbeat_timeout => "3600",
      #       :default_task_list => task_list,
      #       :default_task_schedule_to_close_timeout => "20",
      #       :default_task_schedule_to_start_timeout => "20",
      #       :default_task_start_to_close_timeout => "20",
      #       :default_task_priority => "0",
      #       :version => "1",
      #       :prefix_name => "ExampleActivity"
      #     }
      #     end
      #
      #     def activity1
      #       puts "Hello!"
      #     end
      #   end
      def activity(*activity_names, &block)
        options = Utilities::interpret_block_for_options(ActivityRegistrationOptions, block)
        activity_names.each do |activity_name|
          prefix_name = options.prefix_name || self.to_s
          activity_type = ActivityType.new(prefix_name + "." + activity_name.to_s, options.version, options)
          @activities ||= []
          @activities << activity_type
        end
      end
    end

    # @deprecated Use {Activities} instead.
    # @api private
    module Activity
      include Activities
      def self.extended(base)
        base.send :include, InstanceMethods
      end
    end

  end
end

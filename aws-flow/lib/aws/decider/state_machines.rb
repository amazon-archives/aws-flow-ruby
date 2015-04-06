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

    # @api private
    module DecisionStateMachineDFA
      attr_accessor :transitions, :symbols, :states, :start_state

      def init(start_state)
        include InstanceMethods
        @start_state = start_state
        @symbols = []
        @states = []
        @transitions = {}
        @states << start_state
      end

      def get_start_state
        @start_state
      end

      def self_transitions(symbol)
        states.each do |state|
          add_transition(state, symbol, state) unless @transitions[[state, symbol]]
        end
      end

      def get_transitions
        # Turns out, you are your own ancestor.
        ancestors.slice(1..-1).map {|x| x.transitions if x.respond_to? :transitions}.compact.
          inject({}) {|x, y| x.merge(y)}.merge(@transitions)
      end

      def add_transition(state, symbol, next_state, function = nil)
        @symbols << symbol unless @symbols.include? symbol
        [state, next_state].each do |this_state|
          @states << this_state unless @states.include? this_state
        end
        @transitions[[state, symbol]] = [next_state, function]
      end

      def add_transitions(list_of_transitions)
        list_of_transitions.each {|transition| add_transition(*transition)}
      end

      def uncovered_transitions
        @states.product(@symbols) - @transitions.keys
      end

      module InstanceMethods
        attr_accessor :current_state

        def consume(symbol)
          @state_history ||= [self.class.get_start_state]
          @state_history << @current_state
          @state_history << symbol
          transition_tuple = self.class.get_transitions[[@current_state, symbol]]
          raise "This is not a legal transition, attempting to consume #{symbol} while at state #{current_state}" unless transition_tuple
          @current_state, func_to_call = transition_tuple
          @state_history << @current_state
          func_to_call.call(self) if func_to_call
        end
      end
    end

    # @api private
    class CompleteWorkflowStateMachine
      extend DecisionStateMachineDFA
      attr_reader :id
      def consume(symbol)
        return @decision = nil if symbol == :handle_initiation_failed_event
        return if symbol == :handle_decision_task_started_event
        raise "UnsupportedOperation"
      end
      # Creates a new `CompleteWorkflowStateMachine`.
      #
      # @param id
      #   The decider ID.
      #
      # @param attributes
      #

      def done?
        ! @decision.nil?
      end
      def initialize(id, decision)
        @id = id
        @decision = decision
        @current_state = :created
      end
      init(:created)

      def get_decision
        return @decision
      end
    end


    # @api private
    class DecisionStateMachineBase
      extend DecisionStateMachineDFA
      attr_reader :id

      def initialize(id)
        @id = id
        @current_state = :created
      end


      def handle_started_event(event)
        @state_history << :handle_started_event
      end

      init(:created)

      add_transitions [
       [:cancellation_decision_sent, :handle_cancellation_event, :completed],
       [:cancellation_decision_sent, :handle_cancellation_initiated_event, :cancellation_decision_sent],
       [:cancellation_decision_sent, :handle_completion_event, :completed_after_cancellation_decision_sent],
       [:cancelled_after_initiated, :handle_completion_event, :cancelled_after_initiated],
       [:cancelled_before_initiated, :handle_initiated_event, :cancelled_after_initiated],
       [:cancelled_before_initiated, :handle_initiation_failed_event, :completed],
       [:completed_after_cancellation_decision_sent, :handle_cancellation_failure_event, :completed],
       [:created, :cancel, :completed, lambda { |immediate_cancellation_callback| immediate_cancellation_callback.run }],
       [:created, :handle_decision_task_started_event, :decision_sent],
       [:decision_sent, :cancel, :cancelled_before_initiated],
       [:decision_sent, :handle_initiated_event, :initiated],
       [:decision_sent, :handle_initiation_failed_event, :completed],
       [:initiated, :cancel, :cancelled_after_initiated],
       [:initiated, :handle_completion_event, :completed],
       [:initiated, :handle_initiation_failed_event, :completed],
       [:started, :handle_decision_task_started_event, :started],
                      ]
      self_transitions(:handle_decision_task_started_event)

      def done?
        @current_state == :completed || @current_state == :completed_after_cancellation_decision_sent
      end
    end


    # @api private
    class ActivityDecisionStateMachine < DecisionStateMachineBase

      attr_reader :attributes
      # Creates a new `ActivityDecisionStateMachine`.
      #
      # @param [DecisionID] decision_id
      #
      # @param attributes
      #
      def initialize(decision_id, attributes)
        @attributes = attributes
        super(decision_id)
      end
      init(:created)
      add_transitions [
       [:cancelled_after_initiated, :handle_decision_task_started_event, :cancellation_decision_sent],
       [:cancellation_decision_sent, :handle_cancellation_failure_event, :initiated]
      ]
      def get_decision
        case @current_state
        when :created
          return create_schedule_activity_task_decision
        when :cancelled_after_initiated
          return create_request_cancel_activity_task_decision
        end
      end

      def create_schedule_activity_task_decision
        options = @attributes[:options]
        attribute_type = :schedule_activity_task_decision_attributes
        result = { :decision_type => "ScheduleActivityTask",
            attribute_type =>
            {
              :activity_type =>
              {
                :name => @attributes[:activity_type].name.to_s,
                :version => options.version.to_s
              },
              :activity_id => @attributes[:decision_id].to_s,
            }
        }
        task_list = options.task_list ? {:task_list => {:name => options.task_list}} : {}
        to_add = options.get_options([:heartbeat_timeout, :schedule_to_close_timeout, :task_priority, :schedule_to_start_timeout, :start_to_close_timeout, :input], task_list)
        result[attribute_type].merge!(to_add)
        result
      end

      def create_request_cancel_activity_task_decision
        { :decision_type => "RequestCancelActivityTask",
          :request_cancel_activity_task_decision_attributes => {:activity_id => @attributes[:decision_id]} }
      end
    end


    # @api private
    class TimerDecisionStateMachine < DecisionStateMachineBase
      attr_accessor :cancelled
      def initialize(decision_id, attributes)
        @attributes = attributes
        super(decision_id)
      end

      def create_start_timer_decision
        {
          :decision_type => "StartTimer",
          :start_timer_decision_attributes =>
          {
            :timer_id => @attributes[:timer_id].to_s,
            # TODO find out what the "control" field is, and what it is for
            :start_to_fire_timeout => @attributes[:start_to_fire_timeout]
          }
        }
      end

      def create_cancel_timer_decision
        {
          :decision_type => "CancelTimer",
          :cancel_timer_decision_attributes => {
            :timer_id => @attributes[:timer_id].to_s,
          }
        }
      end

      def get_decision
        case @current_state
        when :created
          return create_start_timer_decision
        when :cancelled_after_initiated
          return create_cancel_timer_decision
        end
      end

      def done?
        @current_state == :completed || @cancelled
      end

      init(:created)
      add_transitions [
       [:cancelled_after_initiated, :handle_decision_task_started_event, :cancellation_decision_sent],
       [:cancellation_decision_sent, :handle_cancellation_failure_event, :initiated],
      ]
    end


    # @api private
    class SignalDecisionStateMachine < DecisionStateMachineBase
      def initialize(decision_id, attributes)
        @attributes = attributes
        super(decision_id)
      end

      def get_decision
        case @current_state
        when :created
          return create_signal_external_workflow_execution_decison
        end
      end

      def create_signal_external_workflow_execution_decison
        extra_options = {}
        [:input, :control, :run_id].each do |type|
          extra_options[type] = @attributes.send(type) if @attributes.send(type)
        end
        result = {
          :decision_type => "SignalExternalWorkflowExecution",
          :signal_external_workflow_execution_decision_attributes =>
          {
            :signal_name => @attributes.signal_name,
            :workflow_id => @attributes.workflow_id
          }
        }
        if ! extra_options.empty?
          result[:signal_external_workflow_execution_decision_attributes].merge! extra_options
        end
        result
      end
      init(:created)
      add_transitions [
                       [:created, :handle_decision_task_started_event, :decision_sent],
                       [:created, :cancel, :created],
                       [:initiated, :cancel, :completed, lambda {|immediate_cancellation_callback| immediate_cancellation_callback.run }],
                       [:decision_sent, :handle_initiated_event, :initiated],
                       [:cancelled_before_initiated, :handle_initiated_event, :cancelled_before_initiated],
                       [:decision_sent, :handle_completion_event, :completed],
                       [:initiated, :handle_completion_event, :completed],
                       [:cancelled_before_initiated, :handle_completion_event, :completed],
                       [:completed, :handle_completion_event, :completed]
                      ]

    end


    # @api private
    class ChildWorkflowDecisionStateMachine < DecisionStateMachineBase
      attr_accessor :run_id, :attributes
      def initialize(decision_id, attributes)
        @attributes = attributes
        super(decision_id)
      end

      def create_start_child_workflow_execution_decision
        options = @attributes[:options]
        workflow_name = options.workflow_name || options.prefix_name
        attribute_name = :start_child_workflow_execution_decision_attributes
        result = {
          :decision_type => "StartChildWorkflowExecution",
           attribute_name =>
          {
            :workflow_type =>
            {
              :name => "#{workflow_name}.#{options.execution_method}",
              :version => options.version
            },
            :workflow_id => @attributes[:workflow_id].to_s,
            :task_list => {
              :name => options.task_list
            },
            # :control => @attributes[:control]
            :tag_list => @attributes[:tag_list]
          }
        }
        result[:start_child_workflow_execution_decision_attributes].delete(:task_list) if options.task_list.nil?
        #TODO Figure out what control is
        to_add = options.get_options([:execution_start_to_close_timeout, :task_start_to_close_timeout, :task_priority, :child_policy, :tag_list, :input])
        result[attribute_name].merge!(to_add)
        result
      end

      def create_request_cancel_external_workflow_execution_decision
        result = {
          :decision_type => "RequestCancelExternalWorkflowExecution",
          :request_cancel_external_workflow_execution_decision_attributes => {
            :workflow_id => @attributes[:workflow_id].to_s,
            :run_id => @run_id.to_s,
          }
        }
      end

      def get_decision
        case @current_state
        when :created
          return create_start_child_workflow_execution_decision
        when :cancelled_after_started
          return create_request_cancel_external_workflow_execution_decision
        end
      end
      init(:created)
      add_transitions [
                       [:cancelled_after_started, :handle_decision_task_started_event, :cancellation_decision_sent],
                       [:initiated, :handle_started_event, :started],
                       [:cancelled_after_initiated, :handle_started_event, :cancelled_after_started],
                       [:cancellation_decision_sent, :handle_cancellation_failure_event, :started],
                       [:started, :cancel, :cancelled_after_started],
                       [:started, :handle_completion_event, :completed],
                       [:started, :handle_cancellation_event, :completed],
                       [:cancelled_after_started, :handle_completion_event, :completed],
                      ]
    end
  end
end

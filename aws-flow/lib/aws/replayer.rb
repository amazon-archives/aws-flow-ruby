module AWS
  module Flow
    # Provides a number of classes useful for debugging workflow executions. For
    # an example of general use, see {WorkflowReplayer}.
    #
    module Replayer
      require 'aws/decider'
      include AWS::Flow

      # Used by {Replayer} to fetch a [DecisionTask][] which will be used by
      # {DecisionTaskHandler}.
      #
      # [DecisionTask]: http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/DecisionTask.html
      #
      # @abstract Implement the `get_history_page` and `get_execution_info` methods to
      #   use it.
      class DecisionTaskProvider

        # Fetches the workflow history and wraps all the history events,
        # workflow type and workflow execution inside a decision task for the
        # decider to work on.
        #
        # @param replay_upto [Fixnum] *Optional*. The event_id of the last event
        #   to return. If set, returns the history only until the specified
        #   event is reached. If not set, then all available history will be
        #   returned. See [HistoryEvent][] for more information.
        #
        #   [HistoryEvent]: http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/HistoryEvent.html
        #
        # @return [DecisionTask] the workflow history encapsulated in a
        #   [DecisionTask][], optionally truncated to the event ID passed to
        #   `replay_upto`.
        #
        #   [DecisionTask]: http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/DecisionTask.html
        #
        def get_decision_task(replay_upto = nil)

          # Get workflow execution info so that we can populate the workflowType
          # and execution fields of the [DecisionTask][].
          #
          # [DecisionTask]: http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/DecisionTask.html
          #
          execution_info = get_execution_info
          events = get_history

          # Truncate history if replay_upto variable is set so that we only
          # replay the history till the specified event
          #
          events = truncate_history(events, replay_upto)
          return nil if events.nil?

          # Generate the hash to instantiate a [DecisionTask][]. We can set
          # *taskToken* and *nextPageToken* to nil since we don't need the
          # values in the replayer.
          #
          # [DecisionTask]: http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/DecisionTask.html
          data = {
            'taskToken' => nil,
            'workflowExecution' => execution_info["execution"],
            'workflowType' => execution_info["workflowType"],
            'events' => events,
            'nextPageToken' => nil
          }
          AWS::SimpleWorkflow::DecisionTask.new(nil, nil, data)
        end

        # Truncates workflow history to a specified event id.
        #
        # @param events the workflow history (events) to truncate.
        #
        # @param replay_upto [Fixnum] *Optional*. The event ID of the final
        #   [HistoryEvent][] to return.
        #
        #   [HistoryEvent]: http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/HistoryEvent.html
        #
        # @return the truncated list of events.
        #
        def truncate_history(events, replay_upto = nil)
          return nil if events.nil? || events.empty?

          # Just return the original array of events if replay_upto is not set
          # or if the number of events is less than replay_upto
          return events if replay_upto.nil? || events.last['eventId'] <= replay_upto

          # Select the events whose eventId is lesser than replay_upto
          truncated = events.select { |event| event['eventId'] <= replay_upto }
          return nil if truncated.empty?
          truncated
        end

        # Fetches the workflow history. Implementing classes *must* override
        # this method.
        #
        # @param page_token *Optional*. A token used to get further pages of
        #   workflow history if all events could not be retrieved by the first
        #   call to the method.
        #
        # @return a list of [HistoryEvent][]s that comprise the workflow's
        #   available history.
        #
        #   [HistoryEvent]: http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/HistoryEvent.html
        #
        def get_history(page_token = nil); end

        # Fetches the workflow execution information used to fill in the
        # [DecisionTask][] details. Implementing classes *must* override this
        # method.
        #
        # @return the workflow execution information as returned by
        #   [AWS::SimpleWorkflow::Client#describe_workflow_execution][].
        #
        #   [AWS::SimpleWorkflow::Client#describe_workflow_execution]: http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Client.html#describe_workflow_execution-instance_method
        def get_execution_info; end

      end

      # Loads a decision task directly from the AWS Simple Workflow Service.
      class ServiceDecisionTaskProvider < DecisionTaskProvider
        attr_reader :domain, :execution, :swf

        # Initialize a new **ServiceDecisionTaskProvider**.
        #
        # @param options a hash of options to provide. Entries for `:domain` and
        #   `:execution` must be present in the hash.
        #
        # @raise |ArgumentError| if either `:domain` or `:execution` is missing
        #   from the *options* parameter.
        #
        def initialize(options = {})
          raise ArgumentError.new("options hash must contain :domain") if options[:domain].nil?
          raise ArgumentError.new("options hash must contain :execution") if options[:execution].nil?
          @execution = options[:execution]
          @domain = options[:domain]
          @swf = AWS::SimpleWorkflow.new.client
        end

        # Get the complete workflow history.
        def get_history
          events = []
          # Get the first page of the workflow history
          page = get_history_page
          page["events"].each { |x| events << x }

          # Get the remaining pages of the workflow history
          until page["nextPageToken"].nil?
            page = get_history_page(page["nextPageToken"])
            page["events"].each { |x| events << x }
          end
          events
        end

        # Fetches a page of workflow history.
        #
        # @param page_token *Optional*. A page token used to retrieve a
        #   particular page of results.
        def get_history_page(page_token = nil)
          # Generate the request options for the service call. Optionally merge
          # next_page_token to the hash if the page_token value is not nil.
          request_opts = {
            domain: @domain,
            execution: @execution,
          }.merge(page_token ? { next_page_token: page_token } : {})

          @swf.get_workflow_execution_history(request_opts)
        end

        # Call AWS Simple Workflow Service to get workflow execution
        # information.
        #
        # @return the execution information based on the `:execution` and
        #   `:domain` provided as input to the class constructor.
        def get_execution_info
          execution = @swf.describe_workflow_execution(
            domain: @domain,
            execution: @execution
          )
          execution["executionInfo"]
        end

      end

      # An AWS Flow Framework utility used to replay a workflow history in the
      # decider against the workflow implementation. Primarily used for
      # debugging workflows.
      #
      # ## Usage
      #
      # **Create an instance of the replayer with the required options:**
      #
      # ~~~~
      # replayer = AWS::Flow::Replayer::WorkflowReplayer.new(
      #   domain: '<domain_name>',
      #   execution: {
      #     workflow_id: "<workflow_id",
      #     run_id: "<run_id>"
      #   },
      #   workflow_class: WorkflowClass
      # )
      # ~~~~
      #
      # **Call the replay method (optionally) with the replay_upto event_id number**
      #
      # ~~~~
      # decision = replayer.replay(20)
      # ~~~~
      #
      class WorkflowReplayer
        attr_reader :task_handler, :task_provider

        # Initialize a new **WorkflowReplayer**.
        #
        # @param options A hash of options. The hash must contain at least
        #   `:workflow_class`.
        #
        # @raise |ArgumentError| if no options hash was passed in, or if the
        #   options are missing the `:workflow_class` key.
        #
        def initialize(options)
          raise ArgumentError.new("You must pass in an options hash") if options.nil?
          raise ArgumentError.new("options hash must contain :workflow_class") if options[:workflow_class].nil?

          # Create the service decision task helper to fetch and truncate the
          # history
          @task_provider = ServiceDecisionTaskProvider.new(options)
          @task_handler = DecisionTaskHandler.from_workflow_class(options[:workflow_class])
        end

        # Performs a replay of workflow history.
        #
        # @param replay_upto [Fixnum] *Optional*. If set, replays the history
        #   only until the specified event is reached. If not set, then all
        #   history will be returned.
        #
        def replay(replay_upto = nil)
          task = @task_provider.get_decision_task(replay_upto)
          @task_handler.handle_decision_task(task) unless task.nil?
        end
      end
    end
  end
end

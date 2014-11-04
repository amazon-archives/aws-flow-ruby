module AWS
  module Flow
    module Replayer
      require 'aws/decider'
      include AWS::Flow

      # This class is used by the Replayer to fetch the DecisionTask which will
      # be used by the DecisionTaskHandler. This is an 'abstract' class. We need
      # to extend it and implement get_history_page and get_execution_info
      # methods to use it.
      class DecisionTaskProvider

        # This method fetches the workflow history and wraps all the history events,
        # workflow type, workflow execution inside a decision task for the
        # decider to work on
        def get_decision_task(replay_upto = nil)
          # Get workflow execution info so that we can populate the workflowType
          # and execution fields of the DecisionTask.
          execution_info = get_execution_info
          events = get_history

          # Truncate history if replay_upto variable is set so that we only
          # replay the history till the specified event
          events = truncate_history(events, replay_upto)
          return nil if events.nil?

          # Generate the hash to instantiate a DecisionTask. We can set
          # taskToken and nextPageToken to nil since we don't need the values
          # in the replayer
          data = {
            'taskToken' => nil,
            'workflowExecution' => execution_info["execution"],
            'workflowType' => execution_info["workflowType"],
            'events' => events,
            'nextPageToken' => nil
          }
          AWS::SimpleWorkflow::DecisionTask.new(nil, nil, data)
        end

        # This method truncates the workflow history to the event_id specified
        # by the replay_upto variable
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

        # This method is used to fetch the actual history. Implementing classes
        # must override this method.
        def get_history(page_token = nil); end

        # This method is used to fetch the WorkflowExecutionInfo to fill in the
        # DecisionTask details. Implementing classes must override this method
        def get_execution_info; end

      end

      # This DecisionTaskProvider loads the decision task directly from the
      # SimpleWorkflowService
      class ServiceDecisionTaskProvider < DecisionTaskProvider
        attr_reader :domain, :execution, :swf

        def initialize(options = {})
          raise ArgumentError.new("options hash must contain :domain") if options[:domain].nil?
          raise ArgumentError.new("options hash must contain :execution") if options[:execution].nil?
          @execution = options[:execution]
          @domain = options[:domain]
          @swf = AWS::SimpleWorkflow.new.client
        end

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

        # This method calls the service to fetch a page of workflow history
        def get_history_page(page_token = nil)
          # generate the request options for the service call. Optionally merge
          # next_page_token to the hash if the page_token value is not nil.
          request_opts = {
            domain: @domain,
            execution: @execution,
          }.merge(page_token ? { next_page_token: page_token } : {})

          @swf.get_workflow_execution_history(request_opts)
        end

        # This method calls the service to get the workflow execution
        # information
        def get_execution_info
          execution = @swf.describe_workflow_execution(
            domain: @domain,
            execution: @execution
          )
          execution["executionInfo"]
        end

      end

      # WorkflowReplayer is an AWS Flow Framework utility that is used to
      # 'replay' a workflow history in the decider against the workflow
      # implementation. It is a useful debugging tool.
      #
      # Usage -
      #
      # # Create an instance of the replayer with the required options -
      #
      # replayer = AWS::Flow::Replayer::WorkflowReplayer.new(
      #   domain: '<domain_name>',
      #   execution: {
      #     workflow_id: "<workflow_id",
      #     run_id: "<run_id>"
      #   },
      #   workflow_class: WorkflowClass
      # )
      #
      # # Call the replay method (optionally) with the replay_upto event_id number -
      #
      # decision = replayer.replay(20)
      #
      class WorkflowReplayer
        attr_reader :task_handler, :task_provider

        def initialize(options)
          raise ArgumentError.new("You must pass in an options hash") if options.nil?
          raise ArgumentError.new("options hash must contain :workflow_class") if options[:workflow_class].nil?

          # Create the service decision task helper to fetch and truncate the
          # history
          @task_provider = ServiceDecisionTaskProvider.new(options)
          @task_handler = DecisionTaskHandler.from_workflow_class(options[:workflow_class])
        end

        # This method performs the actual replay.
        def replay(replay_upto = nil)
          task = @task_provider.get_decision_task(replay_upto)
          @task_handler.handle_decision_task(task) unless task.nil?
        end

      end

    end
  end
end

module AWS
  module Flow

    # @api private
    module Templates

      # Starts an Activity or a Workflow Template execution using the default
      # workflow class FlowDefaultWorkflowRuby
      #
      # @param [String or AWS::Flow::Templates::TemplateBase] name_or_klass
      #   The Activity or the Workflow Template that needs to be scheduled via
      #   the default workflow. This argument can either be a string that
      #   represents a fully qualified activity name - <ActivityClass>.<method_name>
      #   or it can be an instance of AWS::Flow::Templates::TemplateBase
      #
      # @param [Hash] input
      #   Input hash for the workflow execution
      #
      # @param [Hash] opts
      #   Additional options to configure the workflow or activity execution.
      #
      # @option opts [true, false] :wait
      #   *Optional* This boolean flag can be set to true if the result of the
      #   task is required. Default value is false.
      #
      # @option opts [Integer] :wait_timeout
      #   *Optional* This sets the timeout value for :wait. Default value is
      #   nil.
      #
      # @option opts [Hash] :exponential_retry
      #   A hash of {AWS::Flow::ExponentialRetryOptions}. Default value is -
      #   { maximum_attempts: 3 }
      #
      # @option opts [String] *Optional* :domain
      #   Default value is FlowDefault
      #
      # @option opts [Integer] *Optional* :execution_start_to_close_timeout
      #   Default value is 3600 seconds (1 hour)
      #
      # @option opts [Integer] *Optional* :retention_in_days
      #   Default value is 7 days
      #
      # @option opts [String] *Optional* :workflow_id
      #
      # @option opts [Integer] *Optional* :task_priority
      #   Default value is 0
      #
      # @option opts [String] *Optional* :tag_list
      #   By default, the name of the activity task gets added to the workflow's
      #   tag_list
      #
      # @option opts *Optional* :data_converter
      #   Default value is {AWS::Flow::YAMLDataConverter}. To use the
      #   {AWS::Flow::S3DataConverter}, set the AWS_SWF_BUCKET_NAME environment
      #   variable name with a valid AWS S3 bucket name.
      #
      # @option opts *Optional* A hash of {AWS::Flow::ActivityOptions}
      #
      # Usage -
      #
      #    AWS::Flow::start("<ActivityClassName>.<method_name>", <input_hash>,
      #    <options_hash> )
      #
      # Example -
      #
      # 1) Start an activity execution -
      #    AWS::Flow::start("HelloWorldActivity.say_hello", { name: "World" })
      #
      # 2) Start an activity execution with overriden options -
      #    AWS::Flow::start("HelloWorldActivity.say_hello", { name: "World" }, {
      #      exponential_retry: { maximum_attempts: 10 } }
      #    )
      #
      def self.start(name_or_klass, input, opts = {})

        options = opts.dup

        if name_or_klass.is_a?(String)
          # Add activity name as a tag to the workflow execution
          (options[:tag_list] ||= []) << name_or_klass

          # If name_or_klass passed in is a string, we are assuming the user is
          # trying to start a single activity task. Wrap the activity information
          # in the activity template
          name_or_klass = AWS::Flow::Templates.activity(name_or_klass, options)

          # Keep only the required options in the hash
          keys = [
            :domain,
            :retention_in_days,
            :execution_start_to_close_timeout,
            :task_priority,
            :wait,
            :wait_timeout,
            :workflow_id,
            :data_converter,
            :tag_list
          ]
          options.select! { |x| keys.include?(x) }

        end

        # Wrap the template in a root template
        root = AWS::Flow::Templates.root(name_or_klass)

        # Get the default options and merge them with the options passed in. The
        # order of the two hashes 'defaults' and 'options' is important here.
        defaults = FlowConstants.defaults.select do |key|
          [
            :domain,
            :prefix_name,
            :execution_method,
            :version,
            :execution_start_to_close_timeout,
            :data_converter,
            :task_list
          ].include?(key)
        end
        options = defaults.merge(options)

        raise "input needs to be a Hash" unless input.is_a?(Hash)

        # Set the input for the default workflow
        workflow_input = {
          definition: root,
          args: input,
        }

        # Set the result_step for the root template if wait flag is
        # set.
        wait = options.delete(:wait)
        wait_timeout = options.delete(:wait_timeout)
        result_tasklist = set_result_activity(root) if wait

        # Call #start_workflow with the correct options to start the workflow
        # execution
        begin
          AWS::Flow::start_workflow(workflow_input, options)
        rescue AWS::SimpleWorkflow::Errors::UnknownResourceFault => e
          register_defaults(options[:domain])
          AWS::Flow::start_workflow(workflow_input, options)
        end

        # Wait for result
        get_result(result_tasklist, options[:domain], wait_timeout) if wait

      end

      # Sets the result activity with a unique tasklist name for the root template.
      # @api private
      def self.set_result_activity(root)
        # We want the result to be sent to a specific tasklist so that no other
        # worker gets the result of this workflow.
        result_tasklist = "result_tasklist: #{SecureRandom.uuid}"

        name = "#{FlowConstants.defaults[:result_activity_prefix]}."\
          "#{FlowConstants.defaults[:result_activity_method]}"

        # Set the result_step of the root template to the result activity and
        # override the tasklist and timeouts.
        root.result_step = activity(name, {
            task_list: result_tasklist,
            schedule_to_start_timeout: FlowConstants.defaults[:schedule_to_start_timeout],
            start_to_close_timeout: FlowConstants.defaults[:start_to_close_timeout]
          }
        )
        result_tasklist
      end

      # Gets the result of the workflow execution by starting an ActivityWorker
      # on the FlowDefaultResultActivityRuby class. The result activity will set
      # the instance variable future :result with the result of the template.
      # It will block till either the result future is set or till the timeout
      # expires - whichever comes first.
      # @api private
      def self.get_result(tasklist, domain, timeout=nil)

        swf = AWS::SimpleWorkflow.new
        domain = swf.domains[domain]

        # Create a new instance of the FlowDefaultResultActivityRuby class and
        # add it to the ActivityWorker. We pass in the instance instead of the
        # class itself, so that we can locally access the instance variable set
        # by the activity method.
        activity = FlowDefaultResultActivityRuby.new

        # Create the activity worker to poll on the result tasklist
        worker = AWS::Flow::ActivityWorker.new(domain.client, domain, tasklist, activity) {{ use_forking: false }}

        # Keep polling till we get the result or timeout. A 0 or nil timeout
        # will let the loop run to completion.
        begin
          Timeout::timeout(timeout) do
            until activity.result.set?
              worker.run_once(false)
            end
          end
        rescue Timeout::Error => e
          activity.result.set
          return
        end

        # Get the result from the future
        result = activity.result.get
        if result.is_a?(Hash) && result[:failure] && result[:failure].is_a?(Exception)
          raise result[:failure]
        end

        result
      end

      # Registers the relevant defaults with the Simple Workflow Service
      # @api private
      def self.register_defaults(name=nil)
        domain = name.nil? ? register_default_domain : AWS::SimpleWorkflow.new.domains[name]

        register_default_workflow(domain)
        register_default_result_activity(domain)
      end

      # Registers the default domain FlowDefault with the Simple Workflow
      # Service
      # @api private
      def self.register_default_domain
        AWS::Flow::Utilities.register_domain(FlowConstants.defaults[:domain])
      end

      # Registers the default workflow type FlowDefaultWorkflowRuby with the
      # Simple Workflow Service
      # @api private
      def self.register_default_workflow(domain)
        AWS::Flow::WorkflowWorker.new(
          domain.client,
          domain,
          nil,
          AWS::Flow::Templates.default_workflow
        ).register
      end

      # Registers the default result activity type FlowDefaultResultActivityRuby
      # with the Simple Workflow Service
      # @api private
      def self.register_default_result_activity(domain)
        worker = AWS::Flow::ActivityWorker.new(
          domain.client,
          domain,
          nil,
          AWS::Flow::Templates.result_activity
        ) {{ use_forking: false }}
        worker.register
      end

    end

  end
end

module AWS
  module Flow

    # @api private
    module Templates

      class Starter

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
        # @option opts [true, false] :get_result
        #   *Optional* This boolean flag can be set to true if the result future
        #   if required. The future can be waited on by using the
        #   AWS::Flow::wait_for_all, AWS::Flow::wait_for_any methods or by
        #   calling the ExternalFuture#get method. Default value is false.
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
              :get_result,
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
            args: input
          }

          # get_result specifies if we should return back a result future
          # for this task
          get_result = options.delete(:get_result)

          if get_result
            # Start the default result activity worker
            task_list = ResultWorker.start(options[:domain])

            # Set the result_step for the root template. We need to pass in the
            # task_list to ensure the result activity task is sent to the right
            # task list. This method will return back a unique key that will
            # help us locate the result of this task in ResultWorker.results hash
            key = set_result_activity(task_list, root)
          end

          # Call #start_workflow with the correct options to start the workflow
          # execution. If it fails with UnknownResourceFault, then regsiter the
          # default types and retry.
          AWS::Flow::Templates::Utils.register_on_failure(options[:domain]) do
            AWS::Flow::start_workflow(workflow_input, options)
          end

          # Get the result identified by this key
          ResultWorker.get_result_future(key) if get_result

        end

        # Sets the result activity with a unique key. The key is used to match
        # the task with the result of the task. It is provided as an input to
        # the default result activity and is used to create a new ExternalFuture
        # in the ResultWorker.results hash.
        # @api private
        def self.set_result_activity(task_list, root)

          key = "result_key: #{SecureRandom.uuid}"
          # Set the result_step of the root template to the result activity and
          # override the tasklist and timeouts.
          root.result_step = AWS::Flow::Templates.result(key, {
            task_list: task_list,
            schedule_to_start_timeout: FlowConstants.defaults[:schedule_to_start_timeout],
            start_to_close_timeout: FlowConstants.defaults[:start_to_close_timeout]
          })

          # Create a new ExternalFuture in the ResultWorker.results hash.
          ResultWorker.results[key] = ExternalFuture.new

          key
        end

      end

    end

  end
end

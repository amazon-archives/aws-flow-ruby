module AWS
  module Flow

    # Utility method used to start a workflow execution with the service.
    #
    # @param [String or Class (that extends AWS::Flow::Workflows)] workflow
    #   Represents an AWS Flow Framework workflow class. If not provided,
    #   details of the workflow must be passed via the opts Hash.
    #
    # @param [Hash] input
    #   Input hash for the workflow execution
    #
    # @param [Hash] opts
    #   Hash of options to configure the workflow execution
    #
    # @option opts [String] *Required* :domain
    #
    # @option opts [String] *Required* :version
    #
    # @option opts [String] *Optional* :prefix_name
    #   Must be specified if workflow is not passed in as an argument
    #
    # @option opts [String] *Optional* :execution_method
    #   Must be specified if workflow is not passed in as an argument
    #
    # @option opts [String] *Optional* :from_class
    #
    # @option opts [String] *Optional* :workflow_id
    #
    # @option opts [Integer] *Optional* :execution_start_to_close_timeout
    #
    # @option opts [Integer] *Optional* :task_start_to_close_timeout
    #
    # @option opts [Integer] *Optional* :task_priority
    #
    # @option opts [String] *Optional* :task_list
    #
    # @option opts [String] *Optional* :child_policy
    #
    # @option opts [Array] *Optional* :tag_list
    #
    # @option opts [] *Optional* :data_converter
    #
    # Usage -
    #
    # 1) Passing a fully qualified workflow <prefix_name>.<execution_method> name -
    #
    #    AWS::Flow::start_workflow("HelloWorkflow.say_hello", "world", {
    #      domain: "FooDomain",
    #      version: "1.0"
    #      ...
    #    })
    #
    # 2) Passing workflow class name with other details in the options hash -
    #
    #    AWS::Flow::start_workflow("HelloWorkflow", "world", {
    #      domain: "FooDomain",
    #      execution_method: "say_hello",
    #      version: "1.0"
    #      ...
    #    })
    #
    # 3) Acquiring options using the :from_class option -
    #
    #    AWS::Flow::start_workflow(nil, "hello", {
    #      domain: "FooDomain",
    #      from_class: "HelloWorkflow"
    #    })
    #
    #    # This will take all the required options from the HelloWorkflow class.
    #    # If execution_method options is not passed in, it will use the first
    #    # workflow method in the class.
    #
    # 4) All workflow options are present in the options hash. This is the case
    #    when this method is called by AWS::Flow#start
    #
    #    AWS::Flow::start_workflow(nil, "hello", {
    #      domain: "FooDomain",
    #      prefix_name: "HelloWorkflow",
    #      execution_method: "say_hello",
    #      version: "1.0",
    #      ...
    #    })
    def self.start_workflow(workflow = nil, input, opts)

      raise ArgumentError, "Please provide an options hash" if opts.nil? || !opts.is_a?(Hash)

      options = opts.dup

      # Get the domain out of the options hash.
      domain = options.delete(:domain)

      raise ArgumentError, "You must provide a :domain in the options hash" if domain.nil?

      if options[:from_class]
        # Do nothing. Use options as they are. They will be taken care of in the
        # workflow client
      elsif workflow.nil?
        # This block is usually executed when #start_workflow is called from
        # #start. All options required to start the workflow must be present
        # in the options hash.
        prefix_name = options[:prefix_name] || options[:workflow_name]
        # Check if required options are present
        raise ArgumentError, "You must provide a :prefix_name in the options hash" unless prefix_name
        raise ArgumentError, "You must provide an :execution_method in the options hash" unless options[:execution_method]
        raise ArgumentError, "You must provide a :version in the options hash" unless options[:version]
      else
        # When a workflow class name is given along with some options

        # If a fully qualified workflow name is given, split it into prefix_name
        # and execution_method
        prefix_name, execution_method = workflow.to_s.split(".")
        # If a fully qualified name is not given, then look for it in the options
        # hash
        execution_method ||= options[:execution_method]

        # Make sure all required options are present
        raise ArgumentError, "You must provide an :execution_method in the options hash" unless execution_method
        raise ArgumentError, "You must provide a :version in the options hash" unless options[:version]

        # Set the :prefix_name and :execution_method options correctly
        options.merge!(
          prefix_name: prefix_name,
          execution_method: execution_method,
        )
      end

      swf = AWS::SimpleWorkflow.new
      domain = swf.domains[domain]

      # Get a workflow client for the domain
      client = workflow_client(domain.client, domain) { options }

      # Start the workflow execution
      client.start_execution(input)
    end

    # Starts an Activity or a Workflow Template execution using the
    # default workflow class FlowDefaultWorkflowRuby
    #
    # @param [String, AWS::Flow::Templates::TemplateBase] name_or_klass
    #   The Activity or the Workflow Template that needs to be scheduled via
    #   the default workflow. This argument can either be a string that
    #   represents a fully qualified activity name - <ActivityClass>.<method_name>
    #   or it can be an instance of AWS::Flow::Templates::TemplateBase
    #
    # @param [Hash] input
    #   Input hash for the workflow execution
    #
    # @param [Hash] options
    #   Additional options to configure the workflow or activity execution.
    #
    # @option options [true, false] :get_result
    #   *Optional* This boolean flag can be set to true if the result future
    #   if required. The future can be waited on by using the
    #   AWS::Flow::wait_for_all, AWS::Flow::wait_for_any methods or by
    #   calling the ExternalFuture#get method. Default value is false.
    #
    # @option options [Hash] :exponential_retry
    #   A hash of {AWS::Flow::ExponentialRetryOptions}. Default value is -
    #   { maximum_attempts: 3 }
    #
    # @option options [String] *Optional* :domain
    #   Default value is FlowDefault
    #
    # @option options [Integer] *Optional* :execution_start_to_close_timeout
    #   Default value is 3600 seconds (1 hour)
    #
    # @option options [String] *Optional* :workflow_id
    #
    # @option options [Integer] *Optional* :task_priority
    #   Default value is 0
    #
    # @option options [String] *Optional* :tag_list
    #   By default, the name of the activity task gets added to the workflow's
    #   tag_list
    #
    # @option options [YAMLDataConverter, S3DataConverter] *Optional* :data_converter
    #   The default value is {AWS::Flow::YAMLDataConverter}.
    #
    #   To use {AWS::Flow::S3DataConverter}, set the environment variable
    #   `AWS_SWF_BUCKET_NAME` with a valid Amazon S3 bucket name.
    #
    # @option options [Hash] *Optional* A hash of {AWS::Flow::ActivityOptions}
    #
    # Usage:
    #
    #     AWS::Flow::start("<ActivityClassName>.<method_name>", <input_hash>,
    #         <options_hash> )
    #
    # Examples:
    #
    # * Start an activity execution:
    #
    #         AWS::Flow::start("HelloWorldActivity.say_hello", { name: "World" })
    #
    # * Start an activity execution with overridden options:
    #
    #         AWS::Flow::start("HelloWorldActivity.say_hello", { name: "World" },
    #             { exponential_retry: { maximum_attempts: 10 } } )
    #
    def self.start(name_or_klass, input, options = {})
      AWS::Flow::Templates::Starter.start(name_or_klass, input, options)
    end

  end
end

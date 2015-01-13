module AWS
  module Flow

    # Utility method used to start a workflow execution with the service.
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
      options = opts.dup

      # Get the domain out of the options hash.
      domain = options.delete(:domain)

      raise ArgumentError, "You must provide a :domain in the options hash" if domain.nil?

      if options[:from_class]
        # Do nothing. Use options as they are.
      elsif workflow.nil?
        # This block is usually executed when #start_workflow is called from
        # #start. All options required to start the workflow must be present
        # in the options hash.
        prefix_name = options[:prefix_name] || options[:workflow_name]
        # Check if required options are present
        raise ArgumentError, "You must provide a :prefix_name in the options hash" unless prefix_name
        raise ArgumentError, "You must provide an :execution_method in the options hash" unless options[:execution_method]
        raise ArgumentError, "You must provide a :version in the options hash" unless options[:version]
        # Merge the prefix name back into the options hash
        options.merge!(prefix_name: prefix_name)
      else
        # When a workflow class name is given along with some options
        workflow = "#{workflow}"
        # If a fully qualified workflow name is given, split it into prefix_name
        # and execution_method
        prefix_name, execution_method = workflow.split(".")
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

      # Setup the domain where we will start the workflow
      domain = AWS::Flow::Runner.setup_domain({
        'domain' => {
          'name' => domain
        }
      })

      # Get a workflow client for the domain
      client = workflow_client(domain.client, domain) { options }

      # Start the workflow execution
      begin
        client.start_execution(input)
      rescue AWS::SimpleWorkflow::Errors::UnknownResourceFault => e
        # Register the workflow type if not registered.
        if options[:from_class]
          # This is pretty hacky but the quickest way of registering workflows
          # from a class. We should change it in the future
          worker = WorkflowWorker.new(domain.client, domain, options[:task_list], const_get(options[:from_class]))
          worker.register
        else

          # Get flow defaults
          reg_options = WorkflowRegistrationOptions.new.get_registration_options

          # Override the flow detaults with RubyFlowDefaultWorkflow defaults
          reg_options.merge!(
            name: "#{options[:prefix_name]}.#{options[:execution_method]}",
            version: "#{options[:version]}",
              domain: domain.name,
              default_task_list: {
                name: FlowConstants.defaults[:task_list]
              },
              default_execution_start_to_close_timeout: FlowConstants.defaults[:execution_start_to_close_timeout],
              description: "Default workflow type registered for the AWS Flow Framework for Ruby."
          )
          reg_keys = [
            :default_execution_start_to_close_timeout,
            :default_task_start_to_close_timeout,
            :default_task_list,
            :domain,
            :name,
            :version,
            :default_child_policy,
            :description
          ]
          reg_options.select!{ |x| reg_keys.include?(x) }

          domain.client.register_workflow_type(reg_options)
        end
        client.start_execution(input)
      end

    end

    def self.start(name_or_klass, input, options = {})
      AWS::Flow::Templates.start(name_or_klass, input, options)
    end

  end
end

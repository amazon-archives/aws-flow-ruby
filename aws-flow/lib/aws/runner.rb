module AWS
  module Flow
    # The Runner is a command-line utility that can spawn workflow and activity workers according to a specification
    # that you provide in a [JSON](http://json.org/) configuration file. It is available beginning with version 1.3.0 of
    # the AWS Flow Framework for Ruby.
    #
    # ## Invoking the Runner
    #
    # It is invoked like so:
    #
    #     aws-flow-ruby -f runspec.json
    #
    # Where **runspec.json** represents a local JSON file that specifies how to run your activities and workflows.
    #
    # ## The Runner Specification File
    #
    # The runner is configured by passing it a JSON-formatted configuration file. Here is a minimal example, providing
    # only the required fields:
    #
    #     {
    #       "domain": { "name": "ExampleDomain" },
    #       "workflow_workers": [
    #         {
    #           "task_list": "example_workflow_tasklist"
    #         }
    #       ],
    #       "activity_workers": [
    #         {
    #           "task_list": "example_activity_tasklist"
    #         }
    #       ],
    #     }
    #
    # ## For More Information
    #
    # For a complete description of the runner's specification, with examples of both configuring and using the runner,
    # see [The Runner](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/the-runner.html) in the *AWS Flow
    # Framework for Ruby Developer Guide*.
    #
    module Runner

      # Import the necessary gems to run Ruby Flow code.
      require 'aws/decider'
      require 'aws/templates'
      include AWS::Flow
      require 'json'
      require 'optparse'
      require 'socket'
      require 'remote_syslog_logger'

      # Registers the domain if it is not already registered.
      #
      # @api private
      def self.setup_domain(json_config)

        set_user_agent(json_config)

        # If domain is not provided, use the default ruby flow domain
        domain = json_config['domain'] || { 'name' => FlowConstants.defaults[:domain] }

        # If retention period is not provided, default it to 7 days
        retention = domain['retention_in_days'] || FlowConstants::RETENTION_DEFAULT

        AWS::Flow::Utilities.register_domain(domain['name'], retention.to_s)

      end

      # @api private
      def self.set_process_name(name)
        $0 = name
      end

      # Searches the object space for all subclasses of `clazz`.
      #
      # @api private
      def self.all_subclasses(clazz)
        ObjectSpace.each_object(Class).select { |klass| klass.is_a? clazz }
      end

      # Gets the classes to run.
      #
      # This method extracts and validates the 'activity_classes' and
      # 'workflow_classes' fields from the runner specification file, or by
      # autodiscovery of subclasses of [AWS::Flow::Activities]] and
      # [AWS::Flow::Workflows] in the object space.
      #
      #
      # @api private
      def self.get_classes(json_fragment, what)
        classes = json_fragment[what[:config_key]]
        if classes.nil? || classes.empty? then
          # discover the classes
          classes = all_subclasses( what[:clazz] )
        else
          # constantize the class names we just read from the config
          classes.map! { |c| Object.const_get(c) }
        end
        if classes.nil? || classes.empty? then
          raise ArgumentError.new "need at least one implementation class"
        end
        classes
      end

      # Spawns the workers.
      #
      # @api private
      def self.spawn_and_start_workers(json_fragment, process_name, worker)
        workers = []
        num_of_workers = json_fragment['number_of_workers'] || FlowConstants::NUM_OF_WORKERS_DEFAULT
        should_register = true
        num_of_workers.times do
          workers << fork do
            set_process_name(process_name)
            worker.start(should_register)
          end
          should_register = false
        end
        workers
      end

      # Used to support host-specific task lists. When the string "|hostname|"
      # is found in the task list it is replaced by the actual host name.
      #
      # @api private
      def self.expand_task_list(value)
        raise ArgumentError.new unless value
        ret = value
        ret.gsub!("|hostname|", Socket.gethostname)
        ret
      end

      # @api private
      def self.is_empty_field?(json_fragment, field_name)
        field = json_fragment[field_name]
        field.nil? || field.empty?
      end

      # Runs the necessary "require" commands to load the code needed to run a
      # module.
      #
      # config_path: the path where the config file is, to be able to
      #     resolve relative references
      #
      # json_config: the content of the config
      #
      # what: what should be loaded. This is a hash expected to contain two keys:
      #
      #     - :default_file : the file to load unless a specific list is provided
      #
      #     - :config_key : the key of the config element which can contain a
      #           specific list of files to load
      #
      # @api private
      def self.load_files(config_path, json_config, what)
        if is_empty_field?(json_config, what[:config_key]) then
          file = File.join(File.dirname(config_path), what[:default_file])
          require file if File.exists? file
        else
          json_config[what[:config_key]].each { |file| require file if File.exists? file }
        end
      end

      # Starts the activity workers.
      #
      # The activities run by the workers consist of each class that extends
      # [AWS::Flow::Activities] in the paths provided in the `activity_paths`
      # section of [the runner specification file][], or that are loaded from
      # `require` statements in the `workflows.rb` file.
      #
      # If the 'activity' classes are regular ruby classes, this method will
      # create a proxy AWS::Flow::Activities class for each regular ruby class
      # loaded and add the proxy implementation to the ActivityWorker.
      #
      # @api private
      def self.start_activity_workers(swf, domain = nil, json_config)
        workers = []
        domain = setup_domain(json_config) if domain.nil?

        # This will be used later to start default workflow workers. If the
        # 'activity_workers' and 'default_workers' keys are not specified in the
        # json spec, then we don't start any default workers. Hence this value
        # is defaulted to 0.
        number_of_default_workers = 0

        # TODO: logger
        # start the workers for each spec
        if json_config['activity_workers']
          json_config['activity_workers'].each do |w|
            # If number of forks is not provided, it will automatically default to 20
            # within the ActivityWorker
            fork_count = w['number_of_forks_per_worker']
            task_list = expand_task_list(w['task_list']) if w['task_list']

            # Get activity classes
            classes = get_classes(w, {config_key: 'activity_classes',
                                      clazz: AWS::Flow::Activities})

            # If task_list is not provided, use the name of the first class as the
            # task_list for this worker
            task_list ||= "#{classes.first}"

            options = { execution_workers: fork_count }

            logger = json_config['logger']

            if logger
              options[:logger] = RemoteSyslogLogger.new(logger['log_url'],
                                                        logger['port'],
                                                        program: "workflow-server-#{logger['environment']}",
                                                        local_hostname: "workflow-server-#{logger['environment']}")
            end

            # Create a worker
            worker = ActivityWorker.new(swf.client, domain, task_list) { options }

            classes.each do |c|
              c = AWS::Flow::Templates.make_activity_class(c) unless c.is_a?(AWS::Flow::Activities)
              worker.add_implementation(c)
            end

            # We add 1 default worker for each activity worker.
            number_of_default_workers += w['number_of_workers'] || FlowConstants::NUM_OF_WORKERS_DEFAULT

            # start as many workers as desired in child processes
            workers << spawn_and_start_workers(w, "activity-worker", worker)
          end

          # Create the config for default workers if it's not passed in the
          # json_config
          if json_config['default_workers'].nil? || json_config['default_workers']['number_of_workers'].nil?
            json_config['default_workers'] = {
              'number_of_workers' => number_of_default_workers
            }
          end
        end

        # Start the default workflow workers
        workers << start_default_workers(swf, domain, json_config)

        return workers
      end

      # Starts workflow workers for the default workflow type 'FlowDefaultWorkflowRuby'.
      # If 'default_workers' key is not set in the json spec, we set the number
      # of workers equal to the number of activity workers
      # Default workers are used to for processing workflow templates
      #
      # @api private
      def self.start_default_workers(swf, domain = nil, json_config)
        workers = []
        domain = setup_domain(json_config) if domain.nil?

        if json_config['default_workers']
          # Also register the default result activity type in the given domain
          AWS::Flow::Templates::Utils.register_default_result_activity(domain)

          klass = AWS::Flow::Templates.default_workflow
          task_list = FlowConstants.defaults[:task_list]
          # Create a worker
          worker = WorkflowWorker.new(swf.client, domain, task_list, klass)
          # This will take care of both registering and starting the default workers
          workers << spawn_and_start_workers(json_config['default_workers'], "default-worker", worker)
        end
        workers
      end

      # Starts the workflow workers.
      #
      # The workflows run by the workers consist of each class that extends
      # [AWS::Flow::Workflows] in the paths provided in the `workflow_paths`
      # section of [the runner specification file][], or that are loaded from
      # `require` statements in the `workflows.rb` file.
      #
      # @api private
      def self.start_workflow_workers(swf, domain = nil, json_config)
        workers = []
        domain = setup_domain(json_config) if domain.nil?

        # TODO: logger
        # start the workers for each spec
        if json_config['workflow_workers']
          json_config['workflow_workers'].each do |w|
            task_list = expand_task_list(w['task_list'])

            # Get workflow classes
            classes = get_classes(w, {config_key: 'workflow_classes',
                                      clazz: AWS::Flow::Workflows})

            options = {}

            logger = json_config['logger']

            if logger
              options[:logger] = RemoteSyslogLogger.new(logger['log_url'],
                                                        logger['port'],
                                                        program: "workflow-server-#{logger['environment']}",
                                                        local_hostname: "workflow-server-#{logger['environment']}")
            end
            # Create a worker
            worker = WorkflowWorker.new(swf.client, domain, task_list, *classes) { options }

            # Start as many workers as desired in child processes
            workers << spawn_and_start_workers(w, "workflow-worker", worker)
          end
        end

        return workers
      end

      # @api private
      def self.set_user_agent(json_config)
        # set the UserAgent prefix for all clients
        if json_config['user_agent_prefix'] then
          AWS.config(user_agent_prefix: json_config['user_agent_prefix'])
        end
      end

      # @api private
      def self.create_service_client(json_config)
        set_user_agent(json_config)
        swf = AWS::SimpleWorkflow.new
      end

      # Starts the workers and returns an array of process IDs (pids) for the
      # worker processes.
      #
      # @api private
      def self.start_workers(domain = nil, json_config)

        workers = []

        swf = create_service_client(json_config)

        workers << start_activity_workers(swf, domain, json_config)
        workers << start_workflow_workers(swf, domain, json_config)

        # needed to avoid returning nested arrays based on the calls above
        workers.flatten!
      end

      # Loads activity and workflow classes
      #
      # config_path: the path where the config file is, to be able to
      #     resolve relative references
      #
      # json_config: the content of the config
      #
      # @api private
      def self.load_classes(config_path, json_config)
        # load all classes for the activities
        load_files(config_path, json_config, {config_key: 'activity_paths',
                                              default_file: File.join('flow', 'activities.rb')})
        # load all the classes for the workflows
        load_files(config_path, json_config, {config_key: 'workflow_paths',
                                              default_file: File.join('flow', 'workflows.rb')})
      end

      # Sets up forwarding of signals to child processes to facilitate and
      # support orderly shutdown.
      #
      # @api private
      def self.setup_signal_handling(workers)
        Signal.trap("INT") { workers.each { |w| Process.kill("INT", w) }  }
      end

      # Waits until all the child workers are finished.
      #
      # TODO: use a logger
      # @api private
      def self.wait_for_child_processes(workers)
        until workers.empty?
          puts "waiting on workers " + workers.to_s + " to complete"
          dead_guys = Process.waitall
          dead_guys.each { |pid, status| workers.delete(pid); puts pid.to_s + " exited" }
        end
      end

      # Extends the load path so that the 'require' of workflow and activity
      # implementation files can succeed before adding the implementation
      # classes to the workers.
      #
      # @api private
      def self.add_dir_to_load_path(path)
        raise ArgumentError.new("Invalid directory path: \"" + path.to_s + "\"") if not FileTest.directory? path
        $LOAD_PATH.unshift path.to_s
      end

      #
      # Loads the runner specification from a JSON file (passed in with the
      # `--file` parameter when run from the shell).
      #
      # @api private
      def self.load_config_json(path)
        raise ArgumentError.new("Invalid file path: \"" + path.to_s + "\"") if not File.file? path
        config = JSON.parse(File.open(path) { |f| f.read })
      end

      # Interprets the command-line paramters pased in from the shell.
      #
      # The parameter `--file` (short: `-f`) is *required*, and must provide the
      # path to the runner configuration file.
      #
      # @api private
      def self.parse_command_line(argv = ARGV)
        options = {}
        optparse = OptionParser.new do |opts|
          opts.on('-f', '--file JSON_CONFIG_FILE', "Mandatory JSON config file") do |f|
            options[:file] = f
          end
        end

        optparse.parse!(argv)

        # The `--file` parameter is not optional.
        raise OptionParser::MissingArgument.new("file") if options[:file].nil?

        return options
      end


      #
      # Invoked from code. This is a helper method that can be used to start the
      # runner from code. This is especially helpful for debugging purposes.
      #
      # worker_spec: Hash representation of the json worker spec
      #
      def self.run(worker_spec)
        workers = start_workers(worker_spec)
        setup_signal_handling(workers)

        # Hang there until killed: this process is used to relay signals to
        # children to support and facilitate an orderly shutdown.
        wait_for_child_processes(workers)
      end

      #
      # Invoked from the shell.
      #
      # @api private
      def self.main
        options = parse_command_line
        config_path =  options[:file]
        worker_spec = load_config_json(config_path)
        add_dir_to_load_path(Pathname.new(config_path).dirname)
        load_classes(config_path, worker_spec)
        run(worker_spec)
      end

    end
  end
end

if __FILE__ == $0
  AWS::Flow::Runner.main()
end

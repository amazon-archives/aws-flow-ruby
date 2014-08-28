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
      include AWS::Flow
      require 'json'
      require 'optparse'
      require 'socket'

      # Registers the domain if it is not already registered.
      #
      # @api private
      def self.setup_domain(json_config)

        swf = create_service_client(json_config)

        domain = json_config['domain']
        # If retention period is not provided, default it to 7 days
        retention = domain['retention_in_days'] || FlowConstants::RETENTION_DEFAULT

        begin
          swf.client.register_domain({
            name: domain['name'],
            workflow_execution_retention_period_in_days: retention.to_s
          })
        rescue AWS::SimpleWorkflow::Errors::DomainAlreadyExistsFault => e
          # possible log an INFO/WARN if the domain already exists.
        end
        return AWS::SimpleWorkflow::Domain.new( domain['name'] )
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

      # Used to add implementations to workers; see [get_classes] for more
      # information.
      #
      # @api private
      def self.add_implementations(worker, json_fragment, what)
        classes = get_classes(json_fragment, what)
        classes.each { |c| worker.add_implementation(c) }
      end

      # Spawns the workers.
      #
      # @api private
      def self.spawn_and_start_workers(json_fragment, process_name, worker)
        workers = []
        num_of_workers = json_fragment['number_of_workers'] || FlowConstants::NUM_OF_WORKERS_DEFAULT
        num_of_workers.times do
          workers << fork do
            set_process_name(process_name)
            worker.start()
          end
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
      # what: what should loaded. This is a hash expected to contain two keys:
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
      # @api private
      def self.start_activity_workers(swf, domain = nil, config_path, json_config)
        workers = []
        # load all classes for the activities
        load_files(config_path, json_config, {config_key: 'activity_paths',
                     default_file: File.join('flow', 'activities.rb')})
        domain = setup_domain(json_config) if domain.nil?

        # TODO: logger
        # start the workers for each spec
        json_config['activity_workers'].each do |w|
          # If number of forks is not provided, it will automatically default to 20
          # within the ActivityWorker
          fork_count = w['number_of_forks_per_worker']
          task_list = expand_task_list(w['task_list'])

          # create a worker
          worker = ActivityWorker.new(swf.client, domain, task_list, *w['activities']) {{ max_workers: fork_count }}
          add_implementations(worker, w, {config_key: 'activity_classes',
                     clazz: AWS::Flow::Activities})

          # start as many workers as desired in child processes
          workers << spawn_and_start_workers(w, "activity-worker", worker)
        end

        return workers
      end

      # Starts the workflow workers.
      #
      # The workflows run by the workers consist of each class that extends
      # [AWS::Flow::Workflows] in the paths provided in the `workflow_paths`
      # section of [the runner specification file][], or that are loaded from
      # `require` statements in the `workflows.rb` file.
      #
      # @api private
      def self.start_workflow_workers(swf, domain = nil, config_path, json_config)
        workers = []
        # load all the classes for the workflows
        load_files(config_path, json_config, {config_key: 'workflow_paths',
                     default_file: File.join('flow', 'workflows.rb')})
        domain = setup_domain(json_config) if domain.nil?

        # TODO: logger
        # start the workers for each spec
        json_config['workflow_workers'].each do |w|
          task_list = expand_task_list(w['task_list'])

          # create a worker
          worker = WorkflowWorker.new(swf.client, domain, task_list, *w['workflows'])
          add_implementations(worker, w, {config_key: 'workflow_classes',
                     clazz: AWS::Flow::Workflows})

          # start as many workers as desired in child processes
          workers << spawn_and_start_workers(w, "workflow-worker", worker)
        end

        return workers
      end

      # @api private
      def self.create_service_client(json_config)
        # set the UserAgent prefix for all clients
        if json_config['user_agent_prefix'] then
          AWS.config(user_agent_prefix: json_config['user_agent_prefix'])
        end

        swf = AWS::SimpleWorkflow.new
      end

      # Starts the workers and returns an array of process IDs (pids) for the
      # worker processes.
      #
      # @api private
      def self.start_workers(domain = nil, config_path, json_config)
        workers = []

        swf = create_service_client(json_config)

        workers << start_activity_workers(swf, domain, config_path, json_config)
        workers << start_workflow_workers(swf, domain, config_path, json_config)

        # needed to avoid returning nested arrays based on the calls above
        workers.flatten!
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
      # The parameter --file (short: -f) is *required*, and must provide the
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

        # The `--file  parameter is not optional.
        raise OptionParser::MissingArgument.new("file") if options[:file].nil?

        return options
      end

      #
      # Invoked from the shell.
      #
      # @api private
      def self.main
        options = parse_command_line
        config_path =  options[:file]
        config = load_config_json( config_path )
        add_dir_to_load_path( Pathname.new(config_path).dirname )
        domain = setup_domain(config)
        workers = start_workers(domain, config_path, config)
        setup_signal_handling(workers)

        # Hang there until killed: this process is used to relay signals to
        # children to support and facilitate an orderly shutdown.
        wait_for_child_processes(workers)
      end
    end
  end
end

if __FILE__ == $0
  AWS::Flow::Runner.main()
end



module AWS
  module Flow
    module Runner

      # import the necessary gems to run Ruby Flow code
      require 'aws/decider'
      include AWS::Flow
      require 'json'
      require 'optparse'
      require 'socket'


      ##
      ## Helper to start workflow and activity workers according to a predefined
      ## JSON file format that decribes where to find the required elements
      ## 

      # Example of the format:
      # {
      #     "domain":
      #     {
      #        "name": <name_of_the_domain>,
      #        "retention_in_days": <days>
      #     }
      #     "activity_workers": [
      #         
      #        {
      #             "task_list": <name_of_the_task_list>,
      #             "activity_classes": [ <name_of_class_containing_the_activities_to_be_worked_on> ],
      #             "number_of_workers": <number_of_activity_workers_to_spawn>,
      #             "number_of_forks_per_worker": <number_of_forked_workers>
      #         }
      #         //, ... can add more
      #     ],
      #     "workflow_workers": [
      #         {
      #             "task_list": <name_of_the_task_list>,
      #             "workflow_classes": [ <name_of_class_containing_the_workflows_to_be_worked_on> ],
      #             "number_of_workers": <number_of_workflow_workers_to_spawn>
      #         }
      #         //, ... can add more
      #     ],
      #     // Configure which files are 'require'd in order to load the classes
      #     "workflow_paths": [
      #         "lib/workflow.rb"
      #     ],
      #     "activity_paths": [
      #         "lib/activity.rb"
      #     ],
      #     // This is used by the opsworks recipe
      #     "user_agent_prefix" : "ruby-flow-opsworks"
      # }


      # registers the domain if it is not
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

      def self.set_process_name(name)
        $0 = name
      end

      # searches the object space for all subclasses of clazz
      def self.all_subclasses(clazz)
        ObjectSpace.each_object(Class).select { |klass| klass.is_a? clazz }
      end

      # used to extract and validate the 'activity_classes'
      # and 'workflow_classes' fields from the config, or autodiscover 
      # subclasses in the ObjectSpace
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

      # used to add implementations to workers; see get_classes
      def self.add_implementations(worker, json_fragment, what)
        classes = get_classes(json_fragment, what)
        classes.each { |c| worker.add_implementation(c) }
      end

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

      # used to support host-specific task lists
      # when the string "|hostname|" is found in the task list
      # it is replaced by the host name
      def self.expand_task_list(value)
        raise ArgumentError.new unless value
        ret = value
        ret.gsub!("|hostname|", Socket.gethostname)
        ret
      end

      def self.is_empty_field?(json_fragment, field_name)
        field = json_fragment[field_name]
        field.nil? || field.empty?
      end

      # This is used to issue the necessary "require" commands to 
      # load the code needed to run a module
      #
      # config_path: the path where the config file is, to be able to 
      #     resolve relative references
      # json_config: the content of the config
      # what: what should loaded. This is a hash expected to contain two keys:
      #     - :default_file : the file to load unless a specific list is provided
      #     - :config_key : the key of the config element which can contain a
      #             specific list of files to load
      def self.load_files(config_path, json_config, what)
        if is_empty_field?(json_config, what[:config_key]) then 
          file = File.join(File.dirname(config_path), what[:default_file])
          require file if File.exists? file
        else
          json_config[what[:config_key]].each { |file| require file if File.exists? file }
        end
      end

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

      def self.create_service_client(json_config)
        # set the UserAgent prefix for all clients
        if json_config['user_agent_prefix'] then
          AWS.config(user_agent_prefix: json_config['user_agent_prefix'])
        end
        
        swf = AWS::SimpleWorkflow.new
      end

      #
      # this will start all the workers and return an array of pids for the worker
      # processes
      # 
      def self.start_workers(domain = nil, config_path, json_config)
        
        workers = []
        
        swf = create_service_client(json_config)

        workers << start_activity_workers(swf, domain, config_path, json_config)
        workers << start_workflow_workers(swf, domain, config_path, json_config)

        # needed to avoid returning nested arrays based on the calls above
        workers.flatten!

      end

      # setup forwarding of signals to child processes, to facilitate and support
      # orderly shutdown
      def self.setup_signal_handling(workers)
        Signal.trap("INT") { workers.each { |w| Process.kill("INT", w) }  }
      end

      # TODO: use a logger
      # this will wait until all the child workers have died
      def self.wait_for_child_processes(workers)
        until workers.empty?
          puts "waiting on workers " + workers.to_s + " to complete"
          dead_guys = Process.waitall
          dead_guys.each { |pid, status| workers.delete(pid); puts pid.to_s + " exited" }
        end
      end

      # this is used to extend the load path so that the 'require' 
      # of workflow and activity implementation files can succeed
      # before adding the implementation classes to the workers
      def self.add_dir_to_load_path(path)
        raise ArgumentError.new("Invalid directory path: \"" + path.to_s + "\"") if not FileTest.directory? path
        $LOAD_PATH.unshift path.to_s
      end

      #
      # loads the configuration from a JSON file
      #
      def self.load_config_json(path)
        raise ArgumentError.new("Invalid file path: \"" + path.to_s + "\"") if not File.file? path
        config = JSON.parse(File.open(path) { |f| f.read })
      end


      def self.parse_command_line(argv = ARGV)
        options = {}
        optparse = OptionParser.new do |opts|
          opts.on('-f', '--file JSON_CONFIG_FILE', "Mandatory JSON config file") do |f|
            options[:file] = f
          end
        end

        optparse.parse!(argv)

        # file parameter is not optional
        raise OptionParser::MissingArgument.new("file") if options[:file].nil?

        return options
      end

      def self.main
        options = parse_command_line
        config_path =  options[:file]
        config = load_config_json( config_path )
        add_dir_to_load_path( Pathname.new(config_path).dirname )
        domain = setup_domain(config)
        workers = start_workers(domain, config_path, config)
        setup_signal_handling(workers)

        # hang there until killed: this process is used to relay signals to children
        # to support and facilitate an orderly shutdown
        wait_for_child_processes(workers)

      end

    end
  end
end

if __FILE__ == $0
  AWS::Flow::Runner.main()
end



module AWS
  module Flow
    module Templates

      # Used to start a template execution with the service. Usage -
      #
      # 1) Start an activity execution -
      #    AWS::Flow::start("HelloActivity.hello", { name: "aws-flow" })
      #
      # 2) Start an activity execution with overriden options -
      #    AWS::Flow::start("HelloActivity.hello", { name: "aws-flow" }, {
      #      exponential_retry: { maximum_attempts: 10 } }
      #    )
      def self.start(name_or_klass, input, options = {})

        if name_or_klass.is_a?(String)
          # If name_or_klass passed in is a string, we are assuming the user is
          # trying to start a single activity task. We will wrap it in an activity
          # template here.
          activity_opts = options.dup
          # Merge the activity name as a workflow tag
          (options[:tag_list] ||= []) << name_or_klass
          # Wrap the activity information in the activity template
          name_or_klass = activity(name_or_klass, activity_opts)

          # Keep only the required options in the hash
          keys = [
            :domain,
            :execution_start_to_close_timeout,
            :wait_for_result,
            :workflow_id,
            :data_converter,
            :tag_list
          ]
          options.select! { |x| keys.include?(x) }

        end

        # Wrap the template in a root template
        root = root(name_or_klass)

        # Get all the required defaults
        defaults = FlowConstants.defaults
        [:result_activity_prefix, :result_activity_method].each { |x| defaults.delete(x) }

        # Get the default options and merge them with the options passed in. The
        # order of the two hashes 'defaults' and 'options' is important here.
        options = defaults.merge(options)

        input ||= {}
        raise "input needs to be a Hash" unless input.is_a?(Hash)

        # Set the input for the default workflow
        workflow_input = {
          root: root,
          input: input,
        }

        wait_for_result = options.delete(:wait_for_result)
        # We want the result to be sent to a specific tasklist so that no other
        # worker gets the result of this workflow.
        if wait_for_result
          result_tasklist = "result_tasklist: #{SecureRandom.uuid}"

          name = "#{FlowConstants.defaults[:result_activity_prefix]}."\
            "#{FlowConstants.defaults[:result_activity_method]}"

          root.result_step = activity(
            name,
            { task_list: result_tasklist }
          )
        end

        # Call #start_workflow with the correct options to start the workflow
        # execution
        AWS::Flow::start_workflow(workflow_input, options)

        # Wait for result
        get_result(result_tasklist, options[:domain]) if wait_for_result

      end

      private
      # Gets the result of the activity
      def self.get_result(tasklist, domain)

        # Setup the domain where we will start the workflow
        domain = AWS::Flow::Runner.setup_domain({
          'domain' => {
            'name' => domain
          }
        })

        # Get the default result activity class. We want a unique name for the
        # activity class so that the activity method defined below is unique per
        # result to ensure thread safety. Hence we use the tasklist name (which
        # is a uuid) as a part of the class name. However, the activity is
        # registered with a prefix of RubyFlowDefaultResultActivity instead of
        # the actual class name to keep the user domain space clean.
        name = tasklist.split(":")[1].strip.gsub('-','')
        klass = AWS::Flow::Templates.result_activity(name)

        result = Future.new
        # Define the activity method and set the future inside it.
        klass.class_exec do
          # Create the acitivity method that will set our result future
          define_method(FlowConstants.defaults[:result_activity_method]) do |input|
            result.set(input)
          end
        end

        # Create the activity worker to poll on the result tasklist
        worker = AWS::Flow::ActivityWorker.new(domain.client, domain, tasklist, klass) {{ use_forking: false }}
        # Register the activity. Probably need a better way to register since we
        # want to avoid calling register API everytime we start a workflow.
        worker.register

        # Keep polling till we get the result
        until result.set?
          worker.run_once
        end

        # Get the result from the future
        result.get
      end

    end

  end
end

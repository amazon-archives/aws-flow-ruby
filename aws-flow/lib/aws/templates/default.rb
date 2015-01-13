module AWS
  module Flow
    module Templates

      module UserActivities; end

      # Creates a default workflow for the AWS Flow Framework for Ruby. The
      # default workflow can run workflows defined by WorkflowTemplates.
      def self.default_workflow

        # Get the default workflow name
        name = FlowConstants.defaults[:prefix_name]

        begin
          # Check and return if the class already exists in the namespace
          return AWS::Flow::Templates.const_get(name)
        rescue NameError => e
          # pass
        end

        # Create the workflow class in the AWS::Flow::Templates module
        workflow_class = AWS::Flow::Templates.const_set(name, Class.new(Object))

        # Create default activity client, child workflow client and create a
        # workflow method :start
        workflow_class.class_exec do
          extend AWS::Flow::Workflows
          # Add the activity and child workflow clients to the workflow class
          activity_client :act_client
          child_workflow_client :child_client

          # Create workflow types with default options
          workflow(FlowConstants.defaults[:execution_method]) do
            {
              version: FlowConstants.defaults[:version],
              prefix_name: FlowConstants.defaults[:prefix_name],
              default_task_list: FlowConstants.defaults[:task_list],
              default_execution_start_to_close_timeout: FlowConstants.defaults[:execution_start_to_close_timeout]
            }
          end

          # Define the workflow method :start. It will take in an input hash
          # that contains the root template (:root) and the input to the
          # template (:input).
          define_method(FlowConstants.defaults[:execution_method]) do |input|
            raise ArgumentError, "Workflow input must be a Hash" unless input.is_a?(Hash)
            raise ArgumentError, "Input hash must contain key :root" if input[:root].nil?
            raise ArgumentError, "Input hash must contain key :input" if input[:input].nil?

            root = input[:root]
            root_input = input[:input]

            unless root.is_a?(AWS::Flow::Templates::RootTemplate)
              raise "Root must be a AWS::Flow::Templates::RootTemplate"
            end
            raise "Input must be a Hash" unless root_input.is_a?(Hash)

            # Run the root workflow template
            root.run(root_input, self)
          end

        end
        workflow_class

      end

      # Used to convert a regular ruby class into a Ruby Flow Activity class,
      # i.e. extends the AWS::Flow::Activities module. It converts all user
      # defined instance methods into activities and assigns the following
      # defaults to the ActivityType - version: "1.0"
      def self.make_activity_class(klass)
        return klass if klass.nil?

        name = klass.name.split(":").last

        proxy_name = name + "Proxy"
        # Create a proxy activity class that will define activities for all
        # instance methods of the class.
        new_klass = self::UserActivities.const_set(proxy_name.to_sym, Class.new(Object))

        # Extend the AWS::Flow::Activities module and create activities for all
        # instance methods
        new_klass.class_exec do
          extend AWS::Flow::Activities

          attr_reader :instance

          @@klass = klass

          def initialize
            @instance = @@klass.new
          end

          # Creates activities for all instance methods of the held klass
          @@klass.instance_methods(false).each do |method|
            activity(method) do
              {
                version: FlowConstants.defaults[:version],
                prefix_name: name
              }
            end
          end

          # Redirect all method calls to the held instance
          def method_missing(method, *args, &block)
            @instance.send(method, *args, &block)
          end

        end
        new_klass
      end

      # Creates a default result reporting activity for the Ruby Flow Framework.
      def self.result_activity(name="")

        # Get the activity name
        name = "#{FlowConstants.defaults[:result_activity_prefix]}#{name}"
        # Check if the class already exists in the namespace
        begin
          return AWS::Flow::Templates.const_get(name)
        rescue NameError => e
          # pass
        end

        # Create the activity class in the AWS::Flow::Templates module
        activity_class = AWS::Flow::Templates.const_set(name, Class.new(Object))

        activity_class.class_exec do
          extend AWS::Flow::Activities
          # Create activity type with default options. The activity will be
          # implemented in the starter.
          activity(FlowConstants.defaults[:result_activity_method]) do
            {
              version: FlowConstants.defaults[:version],
              prefix_name: FlowConstants.defaults[:result_activity_prefix],
              default_task_list: FlowConstants.defaults[:task_list]
            }
          end
        end
        activity_class
      end

    end
  end
end

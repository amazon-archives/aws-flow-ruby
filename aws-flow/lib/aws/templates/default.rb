module AWS
  module Flow
    module Templates

      # Default workflow class for the AWS Flow Framework for Ruby. It
      # can run workflows defined by WorkflowTemplates.
      class FlowDefaultWorkflowRuby
        extend AWS::Flow::Workflows

        # Create activity client and child workflow client
        activity_client :act_client
        child_workflow_client :child_client

        # Create the workflow type with default options
        workflow FlowConstants.defaults[:execution_method] do
          {
            version: FlowConstants.defaults[:version],
            prefix_name: FlowConstants.defaults[:prefix_name],
            default_task_list: FlowConstants.defaults[:task_list],
            default_execution_start_to_close_timeout: FlowConstants.defaults[:execution_start_to_close_timeout]
          }
        end

        # Define the workflow method :start. It will take in an input hash
        # that contains the root template (:definition) and the arguments to the
        # template (:args).
        # @param input Hash
        #   A hash containing the following keys -
        #     definition: An object of type AWS::Flow::Templates::RootTemplate
        #     args: Hash of arguments to be passed to the definition
        #
        def start(input)

          raise ArgumentError, "Workflow input must be a Hash" unless input.is_a?(Hash)
          raise ArgumentError, "Input hash must contain key :definition" if input[:definition].nil?
          raise ArgumentError, "Input hash must contain key :args" if input[:args].nil?

          definition = input[:definition]
          args = input[:args]

          unless definition.is_a?(AWS::Flow::Templates::RootTemplate)
            raise "Workflow Definition must be a AWS::Flow::Templates::RootTemplate"
          end
          raise "Input must be a Hash" unless args.is_a?(Hash)

          # Run the root workflow template
          definition.run(args, self)

        end

      end

      # Proxy classes for user activities are created in this module
      module ActivityProxies; end

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
        new_klass = self::ActivityProxies.const_set(proxy_name.to_sym, Class.new(Object))

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
                version: "1.0",
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

      # Default result reporting activity class for the AWS Flow Framework for
      # Ruby
      class FlowDefaultResultActivityRuby
        extend AWS::Flow::Activities

        attr_reader :result

        # Create the activity type with default options
        activity FlowConstants.defaults[:result_activity_method] do
          {
            version: FlowConstants.defaults[:result_activity_version],
            prefix_name: FlowConstants.defaults[:result_activity_prefix],
            default_task_list: FlowConstants.defaults[:task_list],
            exponential_retry: FlowConstants.defaults[:retry_policy]
          }
        end

        # Initialize the future upon instantiation
        def initialize
          @result = Future.new
        end

        # Set the future when the activity is run
        def run(input)
          @result.set(input)
          input
        end

      end

      # Returns the default result activity class
      # @api private
      def self.result_activity
        return AWS::Flow::Templates.const_get(FlowConstants.defaults[:result_activity_prefix])
      end

      # Returns the default workflow class
      # @api private
      def self.default_workflow
        return AWS::Flow::Templates.const_get(FlowConstants.defaults[:prefix_name])
      end

    end
  end
end

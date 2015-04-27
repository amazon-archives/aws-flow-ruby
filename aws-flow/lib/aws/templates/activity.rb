module AWS
  module Flow
    module Templates

      # This template represents an Activity in SWF. It holds the name and
      # scheduling options for the activity
      class ActivityTemplate < TemplateBase
        attr_reader :name, :options

        def initialize(name, opts = {})
          options = opts.dup
          # Split the name into prefix name and activity method
          prefix_name, @name = name.split(".")

          # Raise if we don't have a fully qualified name for the activity
          raise ArgumentError, "Activity name should be fully qualified: "\
            "<prefix_name>.<activity_method>" unless @name

          # Get all the property keys from the ActivityOptions class
          keys = ActivityOptions.held_properties.push(:exponential_retry)

          # Only select the options that are needed
          options.select!{ |x| keys.include?(x) }

          # Merge in default values for the activity in case they are not passed
          # by the user
          options = {
            version: FlowConstants.defaults[:version],
            prefix_name: "#{prefix_name}",
            data_converter:  FlowConstants.defaults[:data_converter],
            exponential_retry: FlowConstants.defaults[:retry_policy]
          }.merge(options)

          @options = options
        end

        # Uses the ActivityClient given in the context (workflow class) passed
        # in by the calling template to schedule this activity
        def run(input, context)
          # Get a duplicate of the options hash so as not to change what's
          # stored in this object
          options = @options.dup
          # If a :tasklist key is passed as input to this template, then schedule
          # this activity on that tasklist
          if input.is_a?(Hash) && input[:task_list]
            options.merge!(task_list: input[:task_list])
          end
          # Schedule the activity using the ActivityClient in the context
          context.act_client.send(@name, input) { options }
        end
      end

      # Initializes an activity template
      # @param [String] name
      # @param [Hash] opts
      def activity(name, opts = {})
        AWS::Flow::Templates.send(:activity, name, opts)
      end

      # Initializes an activity template
      # @param [String] name
      # @param [Hash] opts
      def self.activity(name, opts = {})
        ActivityTemplate.new(name, opts)
      end

      # This template represents a Result Activity in SWF.
      class ResultActivityTemplate < ActivityTemplate
        attr_reader :key

        def initialize(key, opts = {})
          @key = key

          # Get the name of the result activity
          name = "#{FlowConstants.defaults[:result_activity_prefix]}."\
            "#{FlowConstants.defaults[:result_activity_method]}"

          super(name, opts)
        end

        # Wraps the input into a result hash and calls the ActivityTemplate#run
        # method to report the result
        def run(input, context)
          result = {}
          result[:key] = @key
          result[:result] = input
          super(result, context)
        end
      end

      # Initializes a result activity template
      # @param [String] key
      #   A unique key that identifies the result of an activity execution
      # @param [Hash] opts
      def result(key, opts = {})
        AWS::Flow::Templates.send(:result, key, opts)
      end

      # Initializes a result activity template
      # @param [String] key
      #   A unique key that identifies the result of an activity execution
      # @param [Hash] opts
      def self.result(key, opts = {})
        ResultActivityTemplate.new(key, opts)
      end


    end
  end
end

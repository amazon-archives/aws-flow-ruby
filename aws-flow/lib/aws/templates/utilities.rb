module AWS
  module Flow
    module Templates

      module Utils

        # This method calls the given block. If an UnknownResourceFault is
        # returned, then it tries to register AWS Flow defaults with the service
        # and calls the block again.
        def self.register_on_failure(domain, &block)
          begin
            block.call(domain)
          rescue AWS::SimpleWorkflow::Errors::UnknownResourceFault => e
            register_defaults(domain)
            block.call(domain)
          end
        end

        # Registers the relevant defaults with the Simple Workflow Service. If
        # domain name is not provided, it registers the FlowDefault domain
        # @api private
        def self.register_defaults(name=nil)
          name ||= FlowConstants.defaults[:domain]
          domain = AWS::Flow::Utilities.register_domain(name)

          register_default_workflow(domain)
          register_default_result_activity(domain)
        end

        # Registers the default workflow type FlowDefaultWorkflowRuby with the
        # Simple Workflow Service
        # @api private
        def self.register_default_workflow(domain)
          AWS::Flow::WorkflowWorker.new(
            domain.client,
            domain,
            nil,
            AWS::Flow::Templates.default_workflow
          ).register
        end

        # Registers the default result activity type FlowDefaultResultActivityRuby
        # with the Simple Workflow Service
        # @api private
        def self.register_default_result_activity(domain)
          worker = AWS::Flow::ActivityWorker.new(
            domain.client,
            domain,
            nil,
            AWS::Flow::Templates.result_activity
          ) {{ use_forking: false }}
          worker.register
        end

      end

    end
  end
end

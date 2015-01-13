module AWS
  module Flow
    module Templates

      # A Template is a precanned workflow definition that can be combined with
      # other templates to construct a workflow body. TemplateBase is a class
      # that must be inherited by all templates. It provides the 'abstract'
      # method #run that needs to be implemented by subclasses.
      class TemplateBase

        # This method needs to be implemented by the sub classes.
        # @param {Hash} input
        #   This is the input to the template
        # @param {AWS::Flow::Workflows} context
        #   A class that extends AWS::Flow::Workflows. The workflow that runs a
        #   template passes itself as an argument to provide the template with
        #   the right context to execute in.
        def run(input, context)
          raise NotImplementedError, "Please implement the #run method of your template."
        end

      end

      # Root template is the top level template that is sent to a workflow to
      # run. It contains a step (which is another template) that it passes the
      # input and the context to. It also contains a result_step that it uses to
      # report the result of the workflow.
      class RootTemplate < TemplateBase
        attr_reader :step, :input
        attr_accessor :result_step

        def initialize(step, result_step)
          @step = step
          @result_step = result_step
        end

        # Calls the run method on the step (top level template). Manages overall
        # error handling and reporting of results for the workflow
        def run(input, context)
          result = nil
          failure = nil
          begin
            result = @step.run(input, context)
          rescue Exception => e
            failure = e
          ensure
            if failure
              # If there is a result_step, pass the failure as an input to it.
              @result_step.run({failure: failure}, context) if @result_step
              # Now fail the workflow
              raise e
            else
              # Pass the result as an input to the result_step
              @result_step.run(result, context) if @result_step
              # Complete the workflow by returning the result
              result
            end
          end
        end
      end

      # Initializes a root template
      # @param {TemplateBase} step
      #   An AWS Flow Framework Template class that inherits TemplateBase. It
      #   contains the actual orchestration of workflow logic inside it.
      # @param {ActivityTemplate} result_step
      #   An optional ActivityTemplate that can be used to report the result
      #   of the 'step'
      def root(step, result_step = nil)
        AWS::Flow::Templates.send(:root, step, result_step)
      end

      # Initializes a root template
      # @param {TemplateBase} step
      #   An AWS Flow Framework Template class that inherits TemplateBase. It
      #   contains the actual orchestration of workflow logic inside it.
      # @param {ActivityTemplate} result_step
      #   An optional ActivityTemplate that can be used to report the result
      #   of the 'step'
      def self.root(step, result_step = nil)
        RootTemplate.new(step, result_step)
      end


    end
  end
end

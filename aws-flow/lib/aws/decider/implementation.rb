#--
# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#++

module AWS
  module Flow

    # Creates a new {WorkflowClient} instance
    #
    # @param service
    #   A {http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow.html SWF service} reference. This is usually
    #   created with:
    #
    #       swf = AWS::SimpleWorkflow.new
    #
    # @param domain
    #   The SWF {http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SimpleWorkflow/Domain.html Domain} to use for this
    #   workflow client. This is usually created on the service object, such as:
    #
    #       domain = swf.domains.create('my-domain', 10)
    #
    #   or retrieved from it (for existing domains):
    #
    #       domain = swf.domains['my-domain']
    #
    # @param [Hash, StartWorkflowOptions] block
    #   A hash of options to start the workflow.
    #
    def workflow_client(service = nil, domain = nil, &block)
      AWS::Flow.send(:workflow_client, service, domain, &block)
    end

    # Execute a block with retries within a workflow context.
    #
    # @param options
    #   The {RetryOptions} to use.
    #
    # @param block
    #   The block to execute.
    #
    def with_retry(options = {}, &block)
      raise "with_retry can only be used inside a workflow context!" if Utilities::is_external
      retry_options = RetryOptions.new(options)
      retry_policy = RetryPolicy.new(retry_options.retry_function, retry_options)
      async_retrying_executor = AsyncRetryingExecutor.new(retry_policy, self.decision_context.workflow_clock, retry_options.return_on_start)
      future = async_retrying_executor.execute(lambda { block.call })
      Utilities::drill_on_future(future) unless retry_options.return_on_start
    end


    # @!visibility private
    def self.workflow_client(service = nil, domain = nil, &block)
      options = Utilities::interpret_block_for_options(StartWorkflowOptions, block)
      if ! Utilities::is_external
        service = AWS::SimpleWorkflow.new
        # So, we probably shouldn't be doing this, but we need to slightly
        # redesign where this is available from
        domain = FlowFiber.current[:decision_context].workflow_context.decision_task.workflow_execution.domain
      else
        if service.nil? || domain.nil?
          raise "You must provide both a service and domain when using workflow client in an external setting"
        end
      end

      workflow_class_name = options.from_class || options.workflow_name
      workflow_class = get_const(workflow_class_name) rescue nil
      WorkflowClient.new(service, domain, workflow_class, options)
    end

  end
end

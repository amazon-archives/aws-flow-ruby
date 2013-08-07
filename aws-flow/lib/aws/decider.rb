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

# @!visibility private
def require_all(path)
  glob = File.join(path, "*.rb")
  Dir[glob].each { |f| require f}
  Dir[glob].map { |f| File.basename(f) }
end

require 'aws/flow'
include AWS::Flow::Core

require 'aws-sdk'
# Setting the user-agent as ruby-flow for all calls to the service
AWS.config(:user_agent_prefix => "ruby-flow")

#LOAD_PATH << File.dirname(File.expand_path(__FILE__))


require "aws/decider/utilities"
require "aws/decider/worker"
require 'aws/decider/generic_client'
require "aws/decider/async_retrying_executor"
require "aws/decider/activity_definition"
require "aws/decider/decider"
require "aws/decider/task_handler"
require "aws/decider/data_converter"
require "aws/decider/state_machines"
require "aws/decider/workflow_definition"

require "aws/decider/executor"
require "aws/decider/workflow_enabled"
require "aws/decider/options"
require "aws/decider/activity"
require "aws/decider/async_decider"
require "aws/decider/workflow_clock"
require "aws/decider/decision_context"
require "aws/decider/workflow_definition_factory"
require "aws/decider/workflow_client"
require "aws/decider/history_helper"
require "aws/decider/exceptions"
require "aws/decider/task_poller"
require "aws/decider/flow_defaults"
require "aws/decider/implementation"
require "aws/decider/version"

# @!visibility private
def get_const(name)
  name = name.split('::').reverse
  current = Object
  while ! name.empty?
    current = current.const_get(name.pop)
  end
  current
end

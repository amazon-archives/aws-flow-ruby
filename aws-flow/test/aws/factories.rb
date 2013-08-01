##
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
##

require 'rubygems'

class WorkflowGenerator
  class << self
    def generate_workflow(domain, options = {})

      name = options[:name] || "default_name"
      version = options[:version] || "1"
      task_list = options[:task_list] || "default_task_list"
      child_policy = options[:child_policy] || :request_cancel
      task_start_to_close = options[:task_start_to_close] || 3600
      default_execution_timeout = options[:default_execution_timeout] || 24 * 3600


      target_workflow = domain.workflow_types.page.select { |x| x.name == name}
      if target_workflow.length == 0
        workflow_type = domain.workflow_types.create(name, version,
                                                     :default_task_list => task_list,
                                                     :default_child_policy => child_policy,
                                                     :default_task_start_to_close_timeout => task_start_to_close,
                                                     :default_execution_start_to_close_timeout => default_execution_timeout)
      else
        workflow_type = target_workflow.first
      end

      return workflow_type
    end
  end

end

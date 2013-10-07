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
     class DecisionContext
       attr_accessor :activity_client, :workflow_client, :workflow_clock, :workflow_context, :decision_helper
       def initialize(activity_client, workflow_client, workflow_clock, workflow_context, decision_helper)
         @activity_client = activity_client
         @workflow_client = workflow_client
         @workflow_clock = workflow_clock
         @workflow_context = workflow_context
         @decision_helper = decision_helper
       end
     end


     # The context for a workflow
     class WorkflowContext

       attr_accessor :continue_as_new_options
       # The decision task method for this workflow.
       attr_accessor :decision_task

       # The {WorkflowClock} for this workflow.
       attr_accessor :workflow_clock

       # Creates a new `WorkflowContext`
       #
       # @param decision_task
       #   The decision task method for this workflow. This is accessible after instance creation by using the
       #   {#decision_task} attribute.
       #
       # @param workflow_clock
       #   The {WorkflowClock} to use to schedule timers for this workflow. This is accessible after instance
       #   creation by using the {#workflow_clock} attribute.
       #
       def initialize(decision_task, workflow_clock)
         @decision_task = decision_task
         @workflow_clock = workflow_clock
       end
       def workflow_execution
         @decision_task.workflow_execution
       end
     end

   end
 end

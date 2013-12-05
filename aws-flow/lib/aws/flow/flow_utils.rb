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

# This file simply contains definitions and functions useful to the overall running of flow
module AWS
  module Flow
    module Core
      class IllegalStateException < Exception; end
      class CancellationException < Exception
        attr_accessor :reason, :details
        def initialize(reason = nil, details = nil)
          @reason = reason
          @details = details
        end
      end

      def make_backtrace(parent_backtrace)
        # 1 frame for the function that actually removes the stack traces
        # 1 frame for the function that calls into the function that removes
        # frames in AsyncBacktrace
        # 1 frame for the call into this function
        # 1 frame for the initialize call of the BRE or External Task
        # 1 frame for the new call into the BRE or ET
        # 1 frame for the AsyncScope initialize that the BRE/ET has to be in

        # "./lib/aws/rubyflow/asyncBacktrace.rb:75:in `caller'"
        # "./lib/aws/rubyflow/asyncBacktrace.rb:21:in `create'"
        # "./lib/aws/rubyflow/flow.rb:16:in `make_backtrace'"
        # "./lib/aws/rubyflow/flow.rb:103:in `initialize'"
        # "./lib/aws/rubyflow/asyncScope.rb:17:in `new'"
        # "./lib/aws/rubyflow/asyncScope.rb:17:in `initialize'"

        frames_to_skip = 7
        backtrace = AsyncBacktrace.create(parent_backtrace, frames_to_skip)
      end
    end
  end
end

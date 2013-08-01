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

def validate_stacktrace_content(method_to_call_on_async_backtrace, thing_to_look_for, should_it_be_there)
    it "makes sure that any of #{thing_to_look_for.join", "} #{should_it_be_there} be printed when we call #{method_to_call_on_async_backtrace} on AsyncBacktrace" do
    AsyncBacktrace.enable
    AsyncBacktrace.send(method_to_call_on_async_backtrace)
    scope = AsyncScope.new do
    error_handler do |t|
      t.begin { raise StandardError.new }
      t.rescue(IOError) {}
    end
  end
  begin
    scope.eventLoop
  rescue Exception => e
    matching_lines = thing_to_look_for.select { |part| e.backtrace.to_s.include? part.to_s }

    matching_lines.send(should_it_be_there, be_empty)
    end
  end
end

describe AsyncBacktrace do
  validate_stacktrace_content(:enable, [:continuation], :should_not)
  validate_stacktrace_content(:disable, [:continuation], :should)
  validate_stacktrace_content(:enable_filtering, $RUBY_FLOW_FILES, :should)
  validate_stacktrace_content(:disable_filtering, $RUBY_FLOW_FILES, :should_not)
end

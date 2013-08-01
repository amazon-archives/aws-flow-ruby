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
require 'factory_girl'
require 'aws/flow'
include AWS::Flow::Core


class FlowFactory

  attr_accessor :async_scope
  def generate_BRE(options = {})
    scope = generate_scope
    bre = BeginRescueEnsure.new(:parent => scope.root_context)
    begin_statement = options[:begin] ? options[:begin] : lambda {}
    rescue_statement = options[:rescue] ? options[:rescue] : lambda {|x| }
    rescue_exception = options[:rescue_exceptions] ? options[:rescue_exceptions] : StandardError
    ensure_statement = options[:ensure] ? options[:ensure] : lambda {}
    bre.begin begin_statement
    bre.rescue(rescue_exception, rescue_statement)
    bre.ensure ensure_statement
    scope << bre
    bre
  end

  def generate_scope(options = {})
    lambda = options[:lambda] || lambda {}
    @async_scope ||= AsyncScope.new do
      lambda.call
    end
    return @async_scope
  end

  def generate_daemon_task(options = {})
    scope = generate_scope
    task = DaemonTask.new

  end
end

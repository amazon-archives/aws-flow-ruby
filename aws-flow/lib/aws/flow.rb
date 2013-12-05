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


# Everything depends on fiber, so we have to require that before anything else
require 'aws/flow/fiber'

# Require everything else
require 'aws/flow/async_backtrace.rb'
require 'aws/flow/async_scope.rb'
require 'aws/flow/begin_rescue_ensure.rb'
require 'aws/flow/flow_utils.rb'
require 'aws/flow/future.rb'
require 'aws/flow/implementation.rb'
require 'aws/flow/simple_dfa.rb'
require 'aws/flow/tasks.rb'
$RUBY_FLOW_FILES = ["async_backtrace","tasks.rb","simple_dfa.rb","implementation.rb","future.rb","flow_utils.rb","begin_rescue_ensure.rb","async_scope.rb"]

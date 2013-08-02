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

def require_all(path)
  glob = File.join(File.dirname(__FILE__), path, "*.rb")
  Dir[glob].each { |f| require f}
  Dir[glob].map { |f| File.basename(f) }
end


# Everything depends on fiber, so we have to require that before anything else
require 'aws/flow/fiber'

$RUBY_FLOW_FILES = require_all 'flow/'

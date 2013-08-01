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
    # We special case exception for now, as YAML doesn't propagate backtraces
    # properly, and they are very handy for debugging
    class YAMLDataConverter

      def dump(object)
        if object.is_a? Exception
          return YAML.dump_stream(object, object.backtrace)
        end
        object.to_yaml
      end
      def load(source)
        return nil if source.nil?
        output = YAML.load source
        if output.is_a? Exception
          backtrace = YAML.load_stream(source).find {|x| ! x.is_a? Exception}
          output.set_backtrace(backtrace.to_a)
        end
        output
      end
    end

  end
end

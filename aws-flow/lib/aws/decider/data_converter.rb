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
    # Converts an object to YAML. Exceptions are handled differently because YAML doesn't propagate backtraces
    # properly, and they are very handy for debugging.
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
          documents = YAML.load_stream(source)
          if YAML::ENGINE.yamler == 'syck'
            documents = documents.documents
          end
          backtrace = documents.find {|x| ! x.is_a? Exception}
          output.set_backtrace(backtrace.to_a)
        end
        output
      end
    end

    # S3DataConverter uses YAMLDataConverter internally to serialize and
    # deserialize ruby objects. Additionally it stores objects larger than
    # 32k characeters in S3 and returns a serialized s3 link to be deserialized
    # remotely. It caches objects locally to minimize calls to S3.
    class S3DataConverter

      require 'lru_redux'

      class S3Cache
        attr_reader :cache

        MAX_SIZE = 1000

        def initialize
          @cache = LruRedux::ThreadSafeCache.new(MAX_SIZE)
        end

        # Cache lookup
        def [](key)
          @cache[key]
        end

        # Cache entry
        def []=(key, value)
          @cache[key] = value
        end

      end

      attr_reader :converter, :bucket, :cache

      class << self
        attr_accessor :conv

        # Ensures singleton
        def converter
          return self.conv if self.conv
          name = ENV['AWS_SWF_BUCKET_NAME']
          if name.nil?
            raise "Need a valid S3 bucket name to initialize S3DataConverter."\
              " Please set the AWS_SWF_BUCKET_NAME environment variable with the"\
              " bucket name."
          end
          self.conv ||= self.new(name)
          return self.conv
        end

      end

      def initialize(bucket)
        @bucket = bucket
        @cache = S3Cache.new
        s3 = AWS::S3.new
        s3.buckets.create(bucket) unless s3.buckets[bucket].exists?
        @converter = FlowConstants.default_data_converter
      end

      # Serializes an object into a string. By default, it serializes to YAML
      def dump(object)
        string = @converter.dump(object)
        ret = string
        if string.size > 32768
          filename = put_to_s3(string)
          ret = @converter.dump({ s3_filename: filename })
        end
        ret
      end

      # Deserializes a string into an object. By default it expects a YAML
      # string
      def load(source)
        object = @converter.load(source)
        ret = object
        if object.is_a?(Hash) && object[:s3_filename]
          ret = @converter.load(get_from_s3(object[:s3_filename]))
        end
        ret
      end

      private #-----------------------------------------------------------------

      # Helper method to write a string to an s3 file
      def put_to_s3(string)
        filename = "rubyflow_data_#{SecureRandom.uuid}"
        s3 = AWS::S3.new
        s3.buckets[@bucket].objects.create(filename, string)
        @cache[filename] = string
        return filename
      end

      # Helper method to read an s3 file
      def get_from_s3(s3_filename)
        return @cache[s3_filename] if @cache[s3_filename]
        s3 = AWS::S3.new
        s3_object = s3.buckets[@bucket].objects[s3_filename]
        begin
          ret = s3_object.read
          @cache[s3_filename] = ret
        rescue AWS::S3::Errors::NoSuchKey => e
          raise "Could not find key #{s3_filename} in bucket #{@bucket} on S3. #{e}"
        end
        return ret
      end

      # Helper method to delete an s3 file
      def delete_from_s3(s3_filename)
        s3 = AWS::S3.new
        s3.buckets[@bucket].objects.delete(s3_filename)
      end

    end

  end
end

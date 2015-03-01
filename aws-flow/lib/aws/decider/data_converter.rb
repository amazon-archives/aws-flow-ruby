#--
# Copyright (C) 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    # Serializes/deserializes Ruby objects using {http://yaml.org/ YAML} format.
    # *This is the default data converter for the AWS Flow Framework for Ruby*.
    #
    # @note There is a 32,768 (32K) character limit on activity/workflow input
    #     and output. If the amount of data that you need to pass exceeds this
    #     limit, use {S3DataConverter} instead.
    #
    class YAMLDataConverter

      # Serializes a Ruby object into a YAML string.
      #
      # @param object [Object]
      #   The object to serialize.
      #
      # @return the object's data in YAML format.
      #
      def dump(object)
        if object.is_a? Exception
          return YAML.dump_stream(object, object.backtrace)
        end
        object.to_yaml
      end

      # Deserializes a YAML string into a Ruby object.
      #
      # @param source [String]
      #   The YAML string to deserialize.
      #
      # @return a Ruby object generated from the YAML string.
      #
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

    # S3DataConverter serializes/deserializes Ruby objects using
    # {YAMLDataConverter}, storing the serialized data on Amazon S3. This data
    # can exceed 32K characters in size.
    #
    # To activate it, set the `AWS_SWF_BUCKET_NAME` environment variable to the
    # name of an Amazon S3 bucket to use to store workflow/activity data. The
    # bucket will be created if necessary.
    #
    # S3DataConverter caches data less than 32K characters in size on the local
    # system; it behaves like {YAMLDataConverter} for files that fit within the
    # [input/output limit](limits) for workflows and activities.
    #
    # limits: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-limits.html
    #
    # For larger data, S3DataConverter serializes it using {YAMLDataConverter}
    # and stores it in the Amazon S3 bucket specified by `AWS_SWF_BUCKET_NAME`,
    # passing the workflow/activity a link to the S3 bucket instead of the data
    # itself. The data in the S3 bucket is read when the object is deserialized.
    #
    # @note The AWS Flow Framework for Ruby doesn't delete files from S3 to
    #     prevent loss of data. It is recommended that you use [Object Lifecycle
    #     Management](olm) in Amazon S3 to automatically delete files after a
    #     certain period.
    #
    # olm: http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html
    #
    #
    class S3DataConverter

      require 'lru_redux'

      # S3Cache is a wrapper around the LruRedux cache.
      # @api private
      class S3Cache
        attr_reader :cache

        MAX_SIZE = 1000

        # @api private
        def initialize
          @cache = LruRedux::ThreadSafeCache.new(MAX_SIZE)
        end

        # Cache lookup
        # @api private
        def [](key)
          @cache[key]
        end

        # Cache entry
        # @api private
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

      # Initialize a new S3DataConverter, providing it the name of an Amazon S3
      # bucket that will be used to store serialized Ruby objects. The bucket
      # will be created if it doesn't already exist.
      #
      # @note The default data converter specified by
      #     {FlowConstants.default_data_converter} will be used to serialize and
      #     deserialize the data. Ordinarily, this is {YAMLDataConverter}.
      #
      # @param bucket [String]
      #     The Amazon S3 bucket name to use for serialized data storage.
      #
      def initialize(bucket)
        @bucket = bucket
        @cache = S3Cache.new
        s3 = AWS::S3.new
        s3.buckets.create(bucket) unless s3.buckets[bucket].exists?
        @converter = FlowConstants.default_data_converter
      end

      # Serializes a Ruby object into a string. If the size of the converted
      # string is greater than 32,768 characters, the serialized string is
      # uploaded to an Amazon S3 file and a hash containing the filename is
      # returned instead. The filename is generated at random in the following
      # format:
      #
      #     rubyflow_data_<UUID>.
      #
      # The format of the returned hash is:
      #
      #     { s3_filename: <filename> }
      #
      # @param object
      #   The object to be serialized into a string. By default, the framework
      #   serializes the object into a YAML string using {YAMLDataConverter}.
      #
      def dump(object)
        string = @converter.dump(object)
        ret = string
        if string.size > 32768
          filename = put_to_s3(string)
          ret = @converter.dump({ s3_filename: filename })
        end
        ret
      end

      # Deserializes a string into a Ruby object. If the deserialized
      # string is a Ruby hash of the format: *{ s3_filename: <filename> }*, then
      # it will first look for the file in a local cache. In case of a cache
      # miss, it will try to download the file from Amazon S3, deserialize the
      # contents of the file and return the new object.
      #
      # @param source
      #   The source that needs to be deserialized into a Ruby object. By
      #   default it expects the source to be a YAML string that was serialized
      #   using {#dump}.
      #
      def load(source)
        object = @converter.load(source)
        ret = object
        if object.is_a?(Hash) && object[:s3_filename]
          ret = @converter.load(get_from_s3(object[:s3_filename]))
        end
        ret
      end

      # Helper method to write a string to an Amazon S3 file. A random filename
      # is generated of the format: *rubyflow_data_<UUID>*.
      #
      # @param string
      #   The string to be uploaded to Amazon S3
      #
      # @api private
      def put_to_s3(string)
        filename = "rubyflow_data_#{SecureRandom.uuid}"
        s3 = AWS::S3.new
        s3.buckets[@bucket].objects.create(filename, string)
        @cache[filename] = string
        return filename
      end

      # Helper method to read an Amazon S3 file
      #
      # @param s3_filename
      #   The file name on Amazon S3 to be deleted
      #
      # @api private
      def get_from_s3(s3_filename)
        return @cache[s3_filename] if @cache[s3_filename]
        s3 = AWS::S3.new
        s3_object = s3.buckets[@bucket].objects[s3_filename]
        begin
          ret = s3_object.read
          @cache[s3_filename] = ret
        rescue AWS::S3::Errors::NoSuchKey => e
          raise "Could not find key #{s3_filename} in bucket #{@bucket} on Amazon S3. #{e}"
        end
        return ret
      end

      # Helper method to delete an Amazon S3 file
      #
      # @param s3_filename
      #   The file name on S3 to be deleted
      #
      # @api private
      def delete_from_s3(s3_filename)
        s3 = AWS::S3.new
        s3.buckets[@bucket].objects.delete(s3_filename)
      end

    end

  end
end

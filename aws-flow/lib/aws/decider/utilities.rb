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

require 'tmpdir'

module AWS
  module Flow
    # Utilities for the AWS Flow Framework for Ruby.
    module Utilities
      # @api private
      class LogFactory
        def self.make_logger(klass)
          make_logger_with_level(klass, Logger::INFO)
        end
        def self.make_logger_with_level(klass, level)
          logname = "#{Dir.tmpdir}/#{klass.class.to_s}"
          logname.gsub!(/::/, '-')
          log = Logger.new(logname)
          log.level = level
          log
        end
      end

      def self.workflow_task_to_debug_string(message, task)
        return "#{message} #{task.workflow_type.name}.#{task.workflow_type.version} for execution with workflow_id: #{task.workflow_execution.workflow_id}, run_id: #{task.workflow_execution.run_id}, task_list: #{task.workflow_execution.task_list} with task_token: #{task.task_token}"
      end
      def self.activity_task_to_debug_string(message, task)
        return "#{message} #{task.activity_type.name}.#{task.activity_type.version} with input: #{task.input} and task_token: #{task.task_token}"
      end


      # @api private
      def self.drill_on_future(future)
        while (future.respond_to? :is_flow_future?) && future.is_flow_future?
          future = future.get
        end
        future
      end

      # @api private
      def self.merge_all_options(*args)
        args.compact!
        youngest = args.last
        args.delete(youngest)
        youngest.precursors.concat(args.reverse)
        youngest.get_full_options
      end

      # @api private
      def self.client_options_from_method_name(method_name, options)
        client_options = options.dup
        if method_name.nil?
          client_options.precursors = options.precursors.empty? ? [] : [options.precursors.first]
        else
          client_options.precursors = options.precursors.select { |x| x.name.split(".").last.to_sym == method_name }
        end

        unless options.precursors.empty?
          client_options.precursors.map!(&:options)
        end
        client_options
      end


      # @api private
      def self.interpret_block_for_options(option_class, block, use_defaults = false)

        return option_class.new({}, use_defaults) if block.nil?
        if block.arity <= 0
          result = block.call
          if result.is_a? Hash
            options = option_class.new(result, use_defaults)
          else
            raise "If using 0 arguments to the option configuration, you must return a hash"
          end
        else
          options = option_class.new({}, use_defaults)
          block.call(options)
        end

        if options.from_class
          # Insert into the next-to-last position, as these options should be used excepting where they might conflict with the options specified in the block
          klass = get_const(options.from_class) rescue nil
          if options.precursors.empty?
            options.precursors = klass._options
          else
            options.precursors.insert(-2, klass._options).flatten!
          end
          options.prefix_name ||= options.from_class
        end
        options
      end


      class AddressableFuture

        attr_accessor :return_value, :_metadata
        def initialize(initial_metadata = nil)
          @_metadata = initial_metadata
          @return_value = Future.new
        end

        # Determines whether the object is a flow future. The contract is that
        # flow futures must have a #get method.
        def is_flow_future?
          true
        end

        def metadata
          @_metadata
        end

        def method_missing(method_name, *args, &block)
          @return_value.send(method_name, *args, &block)
        end
      end

      # @api private
      def self.is_external
        if (defined? Fiber).nil?
          return true
        elsif FlowFiber.current != nil && FlowFiber.current.class != Fiber && FlowFiber.current[:decision_context] != nil
          return false
        end
        return true
      end

      # @api private
      module SelfMethods
        # @api private
        def handle_event(event, options)
          id = options[:id_lambda].call(event) if options[:id_lambda]
          id = event.attributes
          options[:id_methods].each {|method| id = id.send(method)}
          id = options[:id_methods].reduce(event.attributes, :send)
          id = @decision_helper.send(options[:decision_helper_id])[id] if options[:decision_helper_id]
          state_machine = @decision_helper[id]
          state_machine.consume(options[:consume_symbol])
          if options[:decision_helper_scheduled]
            if state_machine.done?
              scheduled_array = options[:decision_helper_scheduled]
              open_request = @decision_helper.send(scheduled_array).delete(id)
            else
              scheduled_array = options[:decision_helper_scheduled]
              open_request = @decision_helper.send(scheduled_array)[id]
            end
            if options[:handle_open_request]
              options[:handle_open_request].call(event, open_request)
            end
          end
          return state_machine.done?
        end
      end

      # @api private
      module UpwardLookups
        attr_accessor :precursors

        # @api private
        def held_properties
          precursors = self.ancestors.dup
          precursors.delete(self)
          result = precursors.map{|precursor| precursor.held_properties if precursor.methods.map(&:to_sym).include? :held_properties}.flatten.compact
          result << @held_properties
          result.flatten
        end

        # @api private
        def property(name, methods_to_prepare = [lambda(&:to_s)])
          @held_properties ||= []
          @held_properties << name
          define_method(name) do
            return_value = instance_variable_get("@#{name}")
            # Make sure we correctly return false values
            return_value = (look_upwards(name) || nil) if return_value.nil?
            return nil if return_value.nil?
            return_value = "NONE" if return_value == Float::INFINITY
            methods_to_prepare.each {|method| return_value = method.call(return_value)}
            return_value
          end
          define_method("#{name}=") do |*args|
            instance_variable_set("@#{name}", args.first) unless args.first.nil?
          end
        end

        # @api private
        def properties(*args)
          args.each { |arg| property(arg) }
        end

        # @api private
        module InstanceMethods
          attr_accessor :precursors
          def look_upwards(variable)
            all_precursors = @precursors.dup
            all_precursors.concat self.class.default_classes
            results = all_precursors.map { |x| x.send(variable) if x.methods.map(&:to_sym).include? variable }.compact
            results.first
          end
        end
      end

    end
  end
end

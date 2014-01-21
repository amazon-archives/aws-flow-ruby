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

# This file contains AsyncBacktrace, which takes care of decorating and properly filtering backtraces.

module AWS
  module Flow
    module Core
      # @api private
      class AsyncBacktrace

        # @api private
        def initialize(parent, backtrace)
          @backtrace = AsyncBacktrace.filter(backtrace)
          @parent = parent
        end

        # @api private
        def backtrace
          if @parent
            AsyncBacktrace.merge(@backtrace, @parent.backtrace)
          else
            @backtrace
          end
        end

        # @api private
        class << self

          # @api private
          def create(parent, frames_to_skip)

            unless @disable_async_backtrace
              b = AsyncBacktrace.caller(frames_to_skip)
              AsyncBacktrace.new(parent, b)
            end
          end

          # @api private
          def create_from_exception(parent, exception)
            unless @disable_async_backtrace
              AsyncBacktrace.new(parent, exception.backtrace);
            end
          end

          # Removes all framework-related frames after application frames. Keep framework frames before application
          # frames.
          #
          # @todo
          #   The correct implementation should not have framework frames before application frames as it is expected to
          #   call Kernel.caller with the correct number. In cases when this number is not correct,
          #   the frames are kept to not create confusion.
          #
          # @api private
          def filter(backtrace)
            if @disable_filtering
              backtrace
            else
              do_filter(backtrace)
            end
          end

          # @api private
          def merge(*backtraces)
            result = []
            backtraces.each do | b |
              if b
                result << "------ continuation ------" if result.size > 0
                result += b
              end
            end
            result
          end

          # @api private
          def disable_filtering
            @disable_filtering = true
          end

          # @api private
          def enable_filtering
            @disable_filtering = false
          end

          # @api private
          def disable
            @disable_async_backtrace = true
          end

          # @api private
          def enable
            @disable_async_backtrace = false
          end

          # @api private
          def caller(skip)
            random_var = Kernel.caller 0
            this_stuff =  1.upto(6).map { |x| Kernel.caller(x) }
            other_var = Kernel.caller skip
            Kernel.caller(@disable_filtering ? 0 : skip)
          end

          private

          # @api private
          def do_filter(backtrace)
            return nil unless backtrace
            # Keep asynchrony frames at the top of the backtrace only
            # then cut all frames starting from asynchrony frame.
            skip_asynchrony_frames = false
            @backtrace = backtrace.take_while do |frame|
              if ! $RUBY_FLOW_FILES.select {|file| Regexp.new(file) =~ frame}.empty?
                !skip_asynchrony_frames
              else
                skip_asynchrony_frames = true
              end
            end
          end
        end
      end
    end
  end
end

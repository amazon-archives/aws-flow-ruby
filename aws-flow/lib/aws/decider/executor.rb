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
require 'logger'

module AWS
  module Flow

    class LogMock
      attr_accessor :log_level
      def initialize()
      end
      def info(s)
        p "info: #{s}" if @log_level > 4
      end
      def debug(s)
        p "debug: #{s}" if @log_level > 3
      end
      def warn(s)
        p "warn: #{s}" if @log_level > 2
      end
      def error(s)
        p "error: #{s}" if @log_level > 1
        p s.backtrace if s.respond_to?(:backtrace)
      end
    end
    class RejectedExecutionException < Exception; end

    class ForkingExecutor

      class << self
        attr_accessor :executors
      end
      attr_accessor :max_workers, :pids, :is_shutdown

      def initialize(options = {})
        @log = options[:logger]
        @log ||= Logger.new("#{Dir.tmpdir}/forking_log")
        @semaphore = Mutex.new
        log_level = options[:log_level] || 4
        @log.level = Logger::DEBUG
        @log.info("LOG INITIALIZED")
        @max_workers = options[:max_workers] || 1
        @pids = []
        @is_shutdown = false
        ForkingExecutor.executors ||= []
        ForkingExecutor.executors << self
      end

      def execute(&block)
        @log.error "Here are the pids that are currently running #{@pids}"
        raise RejectedExecutionException if @is_shutdown
        block_on_max_workers
        @log.debug "PARENT BEFORE FORK #{Process.pid}"
        child_pid = fork do
          begin
            @log.debug "CHILD #{Process.pid}"
            # TODO: which signals to ignore?
            # ignore signals in the child
            %w{ TERM INT HUP SIGUSR2 }.each { |signal| Signal.trap(signal, 'SIG_IGN') }
            block.call
            @log.debug "CHILD #{Process.pid} AFTER block.call"
            Process.exit!(0)
          rescue => e
            @log.error e
            @log.error "Definitely dying off right here"
            Process.exit!(1)
          end
        end
        @log.debug "PARENT AFTER FORK #{Process.pid}, child_pid=#{child_pid}"
        @pids << child_pid
      end

      def shutdown(timeout_seconds)
        @is_shutdown = true
        remove_completed_pids

        unless @pids.empty?
          @log.info "Exit requested, waiting up to #{timeout_seconds} seconds for child processes to finish"

          # check every second for child processes to finish
          timeout_seconds.times do
            sleep 1
            remove_completed_pids
            break if @pids.empty?
          end

          # forcibly kill all remaining children
          unless @pids.empty?
            @log.warn "Child processes still running, sending KILL signal: #{@pids.join(',')}"
            @pids.each { |pid| Process.kill('KILL', pid) }
          end
        end
      end

      private

      # Remove all child processes from @pids list that have finished
      # Block for at least one child to finish if block argument is set to true
      def remove_completed_pids(block=false)
        loop do
          # waitpid2 throws an Errno::ECHILD if there are no child processes,
          # so we don't even call it if there aren't any pids to wait on
          break if @pids.empty?
          # non-blocking wait - only returns a non-null pid
          # if the child process has exited
          pid, status = Process.waitpid2(-1, block ? 0 : Process::WNOHANG)
          @log.debug "#{pid}"
          # no more children have finished
          break unless pid

          if status.success?
            @log.debug "Worker #{pid} exited successfully"
          else
            @log.error "Worker #{pid} exited with non-zero status code"
          end
          @pids.delete(pid)
          break if pid
        end
      end

      def block_on_max_workers
        @log.debug "block_on_max_workers workers=#{@pids.size}, max_workers=#{@max_workers}"
        start_time = Time.now
        if @pids.size > @max_workers
          @log.info "Reached maximum number of workers (#{@max_workers}), \
                     waiting for some to finish before polling again"
          begin
            remove_completed_pids(true)
          end while @pids.size > @max_workers
        end
      end
    end

  end
end

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

    # @api private
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

    # @api private
    class ForkingExecutor

      class << self
        attr_accessor :executors
      end
      attr_accessor :max_workers, :pids, :is_shutdown

      def initialize(options = {})
        unless @log = options[:logger]
          @log = Utilities::LogFactory.make_logger(self)
        end
        @semaphore = Mutex.new
        @max_workers = options[:max_workers] || 1
        @pids = []
        @is_shutdown = false
        ForkingExecutor.executors ||= []
        ForkingExecutor.executors << self
      end

      def execute(&block)
        @log.debug "Currently running pids: #{@pids}"
        raise RejectedExecutionException if @is_shutdown
        block_on_max_workers
        @log.debug "Creating a new child process: parent=#{Process.pid}"
        child_pid = fork do
          begin
            @log.debug "Inside the new child process: parent=#{Process.ppid}, child_pid=#{Process.pid}"
            # TODO: which signals to ignore?
            # ignore signals in the child
            %w{ TERM INT HUP SIGUSR2 }.each { |signal| Signal.trap(signal, 'SIG_IGN') }
            @log.debug "Executing block from child process: parent=#{Process.ppid}, child_pid=#{Process.pid}"
            block.call
            @log.debug "Exiting from child process: parent=#{Process.ppid}, child_pid=#{Process.pid}"
            Process.exit!(0)
          rescue => e
            @log.error "child_pid=#{Process.pid} failed while executing the task: #{e}. Exiting: parent=#{Process.ppid}, child_pid=#{Process.pid}"
            Process.exit!(1)
          end
        end
        @log.debug "Created a new child process: parent=#{Process.pid}, child_pid=#{child_pid}"
        @pids << child_pid
      rescue => e
        # Failing to rescue exceptions here results in some worker processes
        # exiting, leaving the service in an inconsistent state.
        @log.error "Error creating a new child process: parent=#{Process.pid}, error=#{e}"
      end

      def shutdown(timeout_seconds)
        @log.debug "Shutdown requested. Currently running pids: #{@pids}"
        @is_shutdown = true
        remove_completed_pids

        unless @pids.empty?
          # If the timeout_seconds value is set to Float::INFINITY, it will wait
          # indefinitely till all workers finish their work. This allows us to
          # handle graceful shutdown of workers.
          if timeout_seconds == Float::INFINITY
            @log.info "Exit requested, waiting indefinitely till all child processes finish"
            remove_completed_pids true while !@pids.empty?
          else
            @log.info "Exit requested, waiting up to #{timeout_seconds} seconds for child processes to finish"
            # check every second for child processes to finish
            timeout_seconds.times do
              sleep 1
              remove_completed_pids
              break if @pids.empty?
            end
          end

          # forcibly kill all remaining children
          unless @pids.empty?
            @log.warn "Child processes #{@pids} still running, sending KILL signal: #{@pids.join(',')}"
            @pids.each { |pid| Process.kill('KILL', pid) }
          end
        end
      end

      # @api private
      def block_on_max_workers
        @log.debug "block_on_max_workers workers=#{@pids.size}, max_workers=#{@max_workers}"
        if @pids.size >= @max_workers
          @log.info "Reached maximum number of workers (#{@max_workers}), waiting for some to finish"
          begin
            remove_completed_pids(true)
          end while @pids.size >= @max_workers
        end
        @log.debug "Available workers: #{@max_workers - @pids.size} out of #{@max_workers}"
      end

      private

      # Removes all child processes from @pids list that have finished.
      # Block for at least one child to finish if block argument is set to
      # `true`.
      # @api private
      def remove_completed_pids(block=false)
        @log.debug "Removing completed child processes"

        # waitpid2 throws an Errno::ECHILD if there are no child processes,
        # so we don't even call it if there aren't any pids to wait on.
        if @pids.empty?
          @log.debug "No child processes. Returning."
          return
        end

        @log.debug "Current child processes: #{@pids}"

        # Non-blocking wait only returns a non-null pid if the child process has exited.
        # This is the only part where we block if block=true.
        pid, status = Process.waitpid2(-1, block ? 0 : Process::WNOHANG)

        loop do

          # If nothing to reap, then exit
          break unless pid

          # We have something to reap
          @log.debug "Reaping child process=#{pid}"
          if status.success?
            @log.debug "Child process #{pid} exited successfully"
          else
            @log.error "Child process #{pid} exited with non-zero status code: #{status}"
          end

          # Reap
          @pids.delete(pid)

          # waitpid2 throws an Errno::ECHILD if there are no child processes,
          # so we don't even call it if there aren't any pids to wait on.
          break if @pids.empty?

          @log.debug "Current child processes: #{@pids}"
          
          # Contract is to block only once if block=true. Since we have potentially already
          # blocked once above, we only need to do a non blocking call to waitpid2 to see if
          # any other process is available to reap.
          pid, status = Process.waitpid2(-1, Process::WNOHANG)
        end
      end
    end

  end
end

module AWS
  module Flow
    module Templates

      # ResultWorker is responsible for processing the results of the background
      # jobs. It starts an ActivityWorker to process the ActivityTasks for
      # FlowDefaultResultActivityRuby.run activity. It either returns futures or
      # or actual results themselves back to the user
      class ResultWorker

        # Wrapper around a ruby hash to provide synchronization around making
        # changes to the encapsulated hash.
        class SynchronizedHash
          attr_reader :hash

          def initialize
            @semaphore = Mutex.new
            @hash = {}
          end

          def method_missing(method, *args)
            # Not very efficient but ruby structures are not thread
            # safe in MRI.
            @semaphore.synchronize{ return @hash.send(method, *args) }
          end
        end

        class << self
          attr_reader :results
        end

        # Controls synchronization around creation of the ActivityWorker to
        # ensure singleton
        @semaphore = Mutex.new

        # Starts ResultWorker and ensures that a single ActivityWorker is
        # started for this process. Initializes all class instance variables.
        def self.start(domain)

          # If already initiated, return
          return @task_list if @task_list

          # Acquire the lock to ensure only 1 copy of the worker is created
          @semaphore.synchronize do
            # If multiple threads were waiting on the lock, then we should
            # return if the worker was created by the previous thread
            return @task_list if @task_list
            # Initiate all class instance variables. @semaphore around this
            # block ensures a singleton
            self.init
          end

          # Create pipes for IPC
          reader, writer = IO.pipe

          # Start the ForkingExecutor with the ActivityWorker
          self.start_executor(reader, writer, domain)

          # Close one end of the writer pipe
          writer.close

          # Start the listener thread
          self.start_listener(reader)

          # Register signal handlers for this process
          self.handle_signals

          return @task_list

        end

        private

        # Initiates the class instance variables
        # @api private
        def self.init
          # We want the result to be sent to a specific tasklist so that no other
          # worker gets the result of this workflow.
          @task_list ||= "#{Socket.gethostname}:#{Process.pid}:#{SecureRandom.uuid}"
          # Results will be stored in this hash
          @results ||= SynchronizedHash.new
          # Create a new forking executor
          @executor ||= ForkingExecutor.new
        end

        # Start the ActivityWorker using the ForkingExecutor
        # @api private
        def self.start_executor(reader, writer, domain)
          # Create a child process and start an ActivityWorker
          @executor.execute do
            $0 = 'result-worker'
            # Close one end of the reader pipe
            reader.close

            # Create a new instance of the FlowDefaultResultActivityRuby
            # class and add it to the ActivityWorker. We instantiate the
            # activity with the writer pipe so that the activity instance
            # can report results back to the parent process.
            activity = AWS::Flow::Templates.result_activity.new(writer)

            # Start the activity worker. In case of UnknownResourceFault,
            # register the types and start it again.
            AWS::Flow::Templates::Utils.register_on_failure(domain) do |x|
              swf = AWS::SimpleWorkflow.new
              x = swf.domains[x]
              AWS::Flow::ActivityWorker.new(x.client, x, @task_list, activity).start(false)
            end
          end
        end

        # Starts a listener thread that reads data from a reader pipe and
        # updates the result hash
        # @api private
        def self.start_listener(reader)
          @listener_t = Thread.new do
            Thread.current[:name] = "listener_t"
            while true
              data = reader.gets
              result = Marshal.load(data)
              # Only update the result if an unset Future is present at the
              # given location in the hash.
              future = @results[result[:key]]
              if future && !future.set?
                future.set(result[:result])
              end
            end
          end
        end

        # Resets all the class instance variables for ResultWorker
        # @api private
        def self.reset
          @listener_t = nil
          @results = nil
          @task_list = nil
          @executor = nil
        end

        # Stops the ResultWorker, i.e., terminates the listener thread and
        # shutdowns the executor.
        # @api private
        def self.stop
          @listener_t.terminate if @listener_t
          @executor.shutdown(0) if @executor
          self.reset
        end

        # Registers the signal handlers
        # @api private
        def self.handle_signals
          at_exit {
            self.stop
          }
          %w{ TERM INT }.each do |s|
            Signal.trap(s) do
              self.stop
              Kernel.exit
            end
          end
        end

        # Gets the result of the background job. The job is identified by the
        # unique key which was assigned to it during scheduling.
        # The method returns a future which the users can wait on to get the
        # result.
        # @api private
        def self.get_result_future(key)

          # Get the future from the results hash
          future = self.results[key]

          # Self delete the future from the results hash when it is set
          future.on_set { |x| self.results.delete(key) }

          return future
        end

      end

    end

  end
end

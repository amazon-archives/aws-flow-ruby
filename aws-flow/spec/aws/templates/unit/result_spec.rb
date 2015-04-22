require 'spec_helper'

describe AWS::Flow::Templates::ResultWorker do

  context "#semaphore" do

    it "is a class instance variable" do
      semaphore = AWS::Flow::Templates::ResultWorker.instance_variable_get("@semaphore")
      semaphore.is_a?(Mutex)
    end

  end

  context "#start" do

    it "ensure that ResultWorker is started only once per process" do

      AWS::Flow::Templates::ResultWorker.stub(:start_executor)
      AWS::Flow::Templates::ResultWorker.start("domain")

      task_list = AWS::Flow::Templates::ResultWorker.instance_variable_get("@task_list")
      results = AWS::Flow::Templates::ResultWorker.instance_variable_get("@results")
      executor = AWS::Flow::Templates::ResultWorker.instance_variable_get("@executor")

      AWS::Flow::Templates::ResultWorker.start("domain")
      AWS::Flow::Templates::ResultWorker.instance_variable_get("@task_list").should == task_list
      AWS::Flow::Templates::ResultWorker.instance_variable_get("@results").should == results
      AWS::Flow::Templates::ResultWorker.instance_variable_get("@executor").should == executor

      AWS::Flow::Templates::ResultWorker.stop
    end

  end

  context "#shutdown" do

    it "tests clean exit of result worker" do
      AWS::Flow::Templates::ResultWorker.start("FlowDefault")
      sleep 1
      Thread.list.count.should == 2
      AWS::Flow::Templates::ResultWorker.stop
      sleep 1 if Thread.list.count == 2
      Thread.list.count.should == 1
      AWS::Flow::Templates::ResultWorker.instance_variable_get("@executor").should be_nil
    end

  end

  context "#start_listener" do

    it "starts the listener thread and correctly sets the futures" do

      class AWS::Flow::Templates::ResultWorker
        class << self
          alias_method :start_copy, :start
          def start
            @results = SynchronizedHash.new
            self.init
            @listener_t = nil
          end
        end
      end

      AWS::Flow::Templates::ResultWorker.start

      AWS::Flow::Templates::ResultWorker.results[:key1] = ExternalFuture.new
      AWS::Flow::Templates::ResultWorker.results[:key2] = ExternalFuture.new
      AWS::Flow::Templates::ResultWorker.results[:key3] = ExternalFuture.new

      reader, writer = IO.pipe

      AWS::Flow::Templates::ResultWorker.start_listener(reader)
      writer.puts Marshal.dump({key: :key1, result: "result1"})
      writer.puts Marshal.dump({key: :key2, result: "result2"})
      writer.puts Marshal.dump({key: :key3, result: "result3"})

      AWS::Flow::Templates::ResultWorker.results[:key1].get.should == "result1"
      AWS::Flow::Templates::ResultWorker.results[:key2].get.should == "result2"
      AWS::Flow::Templates::ResultWorker.results[:key3].get.should == "result3"

      writer.close
      reader.close

      class AWS::Flow::Templates::ResultWorker
        class << self
          alias_method :start_copy, :start
        end
      end

    end

  end

  context "#get_result_future" do

    it "returns a future and the future is removed from the hash when it is set" do

      class AWS::Flow::Templates::ResultWorker
        class << self
          alias_method :start_copy, :start
          def start
            @results = SynchronizedHash.new
          end
        end
      end

      key = "foo"
      future = ExternalFuture.new

      AWS::Flow::Templates::ResultWorker.results[key] = future

      # Call get_result and check that the result is set
      result = AWS::Flow::Templates::ResultWorker.get_result_future(key)

      result.should == future

      future.set(nil)

      AWS::Flow::Templates::ResultWorker.results[key].should be_nil

      class AWS::Flow::Templates::ResultWorker
        class << self
          alias_method :start_copy, :start
        end
      end

    end

  end

end

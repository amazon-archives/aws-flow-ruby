require_relative 'setup'

describe ActivityTaskPoller do

  context "#respond_activity_task_failed" do

    # This test checks a fix for Github issue #90
    it "correctly calls #respond_activity_task_failed_with_retry on failures" do

      # Set up test mock
      swf = double
      allow(swf).to receive(:config).and_return(swf)
      allow(swf).to receive(:to_h).and_return({})

      # Instantiate the poller
      poller = ActivityTaskPoller.new(swf, nil, nil, nil, nil, nil)

      # Set up mock logger to avoid test failures
      logger = double
      allow(logger).to receive(:debug)
      allow(logger).to receive(:error)
      poller.instance_variable_set(:@logger, logger)

      # Ensure @service.respond_activity_task_failed method is called. Raise
      # ValidationException to trigger the correct handler
      allow(swf).to receive(:respond_activity_task_failed).with(
        task_token: "task_token",
        reason: "reason",
        details: "details"
      ).and_raise(
        AWS::SimpleWorkflow::Errors::ValidationException,
        "failed to satisfy constraint: Member must have length less than or equal to"
      )

      # Final check - ensure respond_activity_task_failed_with_retry is called
      # with the correct task_token string
      expect(poller).to receive(:respond_activity_task_failed_with_retry).with(
        "task_token",
        Utilities.validation_error_string("Activity"),
        AWS::SimpleWorkflow::Errors::ValidationException.to_s
      )

      # Start the test
      poller.respond_activity_task_failed("task_token", "reason", "details")

    end

  end

end

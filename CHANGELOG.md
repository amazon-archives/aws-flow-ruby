3.1.0 (2015-04-24)
----------------------
* Fix - State machine bug fixes
* Fix - NoContextException bug fix. [Issue#90](issues/90)
* Fix - File encoding issue in aws-flow-utils. [Issue#89](issues/89)
* Fix - Incompatibility issue introduced by Symbol GC in ruby 2.2.0. [Issue#88](issues/88)

3.0.0 (2015-02-20)
----------------------

* Changes -
  Made AWS::Flow.start call non blocking and removed :wait and :wait_timeout
  options in favor of :get_result option

* Feature - Starting Background Jobs will now return a future if :get_result
  is true.
  Usage:
  ```ruby
  future = AWS::Flow.start("MyJobs.hello", {name: "aws"}, {get_result: true})
  ```

  The future can be waited on till it is ready.
  ```ruby
  # block till the future is ready
  future.get
  # block till the future is ready or till the timeout expires
  future.get(10)

  # Wait on multiple futures at once
  AWS::Flow.wait_for_all(future_a, future_b)

  # specify timeout in seconds
  AWS::Flow.timed_wait_for_all(10, future_a, future_b)
  ```

  Similarly AWS::Flow.wait_for_any and AWS::Flow.timed_wait_for_any

* Feature - Starting Background Jobs will now start one result background
  worker per process instead of one per thread.

* Issue - Changed the value returned by `Future#set?` for an unset Future from
  nil to false.

* Documentation - Added Changelog and backfilled 2.4.0 release in changelog

* Documentation - Updated README

2.4.0 (2015-01-22)
----------------------

* Feature - Added support Background Jobs in aws-flow. Check out
  [README](README.md) for usage.

* Feature - Added an S3DataConverter to allow transfer of large amounts of data
  between SWF and Flow. You can enable the S3DataConverter by setting the
  following environment variable -
  `export AWS_SWF_BUCKET_NAME=<your S3bucket name>`

1.0.0 - 2.3.1
----------------------

* No changelog entries.

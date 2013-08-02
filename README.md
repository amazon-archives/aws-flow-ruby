# AWS Flow Framework for Ruby

Using the AWS Flow Framework for Ruby, you write code in a programming model that is natural for Ruby programmers while the framework’s pre-built objects and classes handle the details of the Amazon Simple Workflow APIs. AWS Flow Framework for Ruby makes it easy to build applications that perform work across many machines. The framework lets you quickly create tasks, coordinate them, and express how these tasks depend on each other -- as you would do in a typical program. For example, you can run a method in an application on a “remote” computer simply by calling a method in your application logic that is hosted on a separate “local” computer.  The AWS Flow Framework takes care of the complex back-and-forth needed to execute the remote method and returns its result to the local application by using information that is stored by the Amazon Simple Workflow service. The output of any executed method can be used to connect separate parts of your logic that depend on each other. The framework lets you use straightforward syntax to express dependencies between methods with a simple “block and wait for a callback” approach.  The framework also lets you handle a failure on a remote machine as if it were a local error and gives you easy ways to define how you’d like to retry important methods in your application if they happen to fail.


For general information about the AWS Flow Framework for Ruby, including information about installing the Framework, prerequisites for use, getting started with the Framework and how to code common scenarios, see the [AWS Flow Framework for Ruby Developer Guide](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/).


## Links

* [Ruby Gems](http://rubygems.org/gems/aws-flow)
* [Developer Guide](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/)
* [Code Samples](http://aws.amazon.com/code/Amazon-SWF/3015904745387737)
* [API Reference](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowapi/)
* [Getting Started Video](http://www.youtube.com/watch?v=Z_dvXy4AVEE)

## Install the AWS Flow Framework for Ruby

     gem install aws-flow

## License

The AWS Flow Framework for Ruby is distributed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Copyright 2013. Amazon Web Services, Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[aws-sdk-ruby]: http://aws.amazon.com/sdkforruby/

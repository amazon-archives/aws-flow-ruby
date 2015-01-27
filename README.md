# AWS Flow Framework for Ruby
AWS Flow Framework is a library for creating background jobs and multistep workflows using [Amazon Simple
Workflow](http://aws.amazon.com/swf/) (Amazon SWF).

## Basic usage
To create a simple background job, first implement your logic for processing the job. Each job can be implemented as a method in a class, for example:

```ruby
class MyJobs
  def hello input
    "Hello #{input[:name]}!"
  end
end
```

Then use aws-flow-utils to generate worker configuration. 

    aws-flow-utils -c local -n MyHelloWorldApp -a <your_ruby_file_name> -A <your_ruby_class_name>

Start your workers.

    aws-flow-ruby -f worker.json

That's it, you can now create background jobs using `AWS::Flow.start` and they will be executed on your workers.

```ruby
require 'aws/decider'
AWS::Flow::start("MyJobs.hello", { name: "AWS Flow Framework" })
```

### Running workers in Amazon EC2
You can deploy your workers on AWS Elastic Beanstalk with a just a few additional steps. See our [developer guide](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/eb-howto.html) for a step-by-step walkthrough.

## More Information

For additional information, including installation instructions and how to implement multistep workflows, see the [AWS Flow Framework
for Ruby Developer Guide](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/).

## Links

* [Ruby Gems](http://rubygems.org/gems/aws-flow)
* [Developer Guide](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/)
* [Code Samples](http://aws.amazon.com/code/Amazon-SWF/3015904745387737)
* [API Reference](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowapi/frames.html)
* [Getting Started Video](http://www.youtube.com/watch?v=Z_dvXy4AVEE)

## License

Copyright 2013. Amazon Web Services, Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

* <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.

[aws-sdk-ruby]: http://aws.amazon.com/sdkforruby/

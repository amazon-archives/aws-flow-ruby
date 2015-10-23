# AWS Flow Framework for Ruby

The *AWS Flow Framework* is a library for creating background jobs and multistep
workflows using [Amazon Simple Workflow][swf] (Amazon SWF).

[swf]: http://aws.amazon.com/swf/


## Basic Usage

To create a simple background job, first implement your logic for processing the
job. Each job can be implemented as a method in a class. For example:

```ruby
class MyJobs
  def hello input
    "Hello #{input[:name]}!"
  end
end
```

Next, use `aws-flow-utils` to generate a worker configuration:

    aws-flow-utils -c local -n MyHelloWorldApp -a <your_ruby_file_name> -A <your_ruby_class_name>

Finally, start your workers with `aws-flow-ruby`:

    aws-flow-ruby -f worker.json

That's it. You can now create background jobs using `AWS::Flow.start` and they
will be executed on your workers:

```ruby
require 'aws/decider'
AWS::Flow::start("MyJobs.hello", { name: "AWS Flow Framework" })
```

### Results
You can also get the result of the background job if desired.

``` ruby
future = AWS::Flow::start("MyJobs.hello", { name: "AWS Flow Framework" }, {result:true})
# wait till ready
future.get
```

## Running Workers in Amazon EC2

You can deploy your workers on [AWS Elastic Beanstalk][eb] with just a few
additional steps. See our developer guide for a [step-by-step
walkthrough][eb-howto].

[eb]: https://aws.amazon.com/elasticbeanstalk/
[eb-howto]: http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/eb-howto.html


## Links

* [Developer Guide](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/)
* [API Reference](http://docs.aws.amazon.com/amazonswf/latest/awsrbflowapi/frames.html)
* [Code Samples](http://aws.amazon.com/code/Amazon-SWF/3015904745387737)
* [Amazon SWF Forums][forums] (requires an AWS account and login)
* [Ruby Gems](http://rubygems.org/gems/aws-flow)

[forums]: https://forums.aws.amazon.com/forum.jspa?forumID=133


## License

Copyright 2015, Amazon Web Services, Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at:

* <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

require './lib/aws/decider/version'

Gem::Specification.new do |s|
  s.name        = 'aws-flow'
  s.version     = AWS::Flow::version
  s.date        = Time.now
  s.summary     = "AWS Flow Decider package decider"
  s.description = "Library to provide the AWS Flow Framework for Ruby"
  s.authors     = "Michael Steger"
  s.email       = ''
  s.files       = `git ls-files`.split("\n").reject {|file| file =~ /aws-flow-core/}
  s.require_paths << "lib/aws/"
  s.add_dependency "aws-sdk", "~> 1"
  s.add_dependency "aws-flow-core", "~> 1"
end

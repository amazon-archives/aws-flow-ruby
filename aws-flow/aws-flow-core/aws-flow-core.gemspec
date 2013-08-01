Gem::Specification.new do |s|
  s.name        = 'aws-flow-core'
  s.version     = '1.0.0'
  s.date        = Time.now
  s.summary     = "AWS Flow Core"
  s.description = "Library to provide all the base asynchronous constructs that aws-flow uses"
  s.authors     = "Michael Steger"
  s.email       = ""
  s.files       = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.require_paths << "lib/aws/"
end

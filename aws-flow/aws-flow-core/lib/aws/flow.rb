def require_all(path)
  glob = File.join(File.dirname(__FILE__), path, "*.rb")
  Dir[glob].each { |f| require f}
  Dir[glob].map { |f| File.basename(f) }
end


# Everything depends on fiber, so we have to require that before anything else
require 'aws/flow/fiber'

$RUBY_FLOW_FILES = require_all 'flow/'

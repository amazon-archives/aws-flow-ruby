module AWS
  module Flow
    module Utils

      require 'optparse'
      require 'fileutils'
      require 'json'

      # Interprets the command-line paramters pased in from the shell.
      #
      # @api private
      def self.parse_command_line(argv = ARGV)
        options = {}
        optparse = OptionParser.new do |opts|
          opts.banner = "Usage: aws-flow-utils -c <command> [options]"

          opts.separator ""
          options[:deploy] = {enabled: false}
          opts.on('-c', '--command COMMAND', [:eb,:local], "Specify the command to run. (eb, local)") do |f|
            options[:deploy][:enabled] = true
            options[:deploy][:eb] = true if f == :eb
            options[:deploy][:local] = true if f == :local
          end

          opts.separator ""
          opts.separator "Commands"
          opts.separator ""
          opts.separator "\tlocal: Generates an AWS Flow Framework for Ruby "\
            "application skeleton."
          opts.separator "\t   eb: Generates an AWS Flow Framework for Ruby "\
            "application skeleton compatible with AWS Elastic Beanstalk."
          opts.separator ""
          opts.separator "Specific options"
          opts.separator ""

          opts.on('-n', '--name NAME', "Set the name of the application") do |f|
            options[:deploy][:name] = f
          end

          opts.on('-r', '--region [REGION]', "Set the AWS Region. Default "\
                  "value is taken from environment variable AWS_REGION if "\
                  "it is set.") do |f|
            options[:deploy][:region] = f.downcase
          end

          opts.on('-p', '--path [PATH]', "Set the location where the AWS Flow "\
                  "for Ruby application will be created. (Default is '.')") do |f|
            options[:deploy][:path] = f
          end

          opts.on('-a', '--act_path [PATH]', "Set the location where activity "\
                  "classes reside.") do |f|
            options[:deploy][:act] = f
          end

          opts.on('-w', '--wf_path [PATH]', "Set the location where workflow "\
                  "classes reside.") do |f|
            options[:deploy][:wf] = f
          end

          opts.on('-A', "--activities x,y,z", Array, "Set the names of Activity "\
                  "classes") do |f|
            options[:deploy][:activities] = f
          end

          opts.on('-W', "--workflows x,y,z", Array, "Set the names of Workflow "\
                  "classes") do |f|
            options[:deploy][:workflows] = f
          end

        end

        optparse.parse!(argv)

        return options
      end

      class FlowUtils
        class << self
          attr_accessor :utils
        end
        self.utils ||= []
      end

      # Initializes an AWS Flow Framework for Ruby application
      class InitConfig
        # Self register with the FlowUtils class
        FlowUtils.utils << self

        # Generates the appropriate 1-Click URL for beanstalk deployments based on
        # the options passed by the user
        def self.generate_url(options)
          url = "https://console.aws.amazon.com/elasticbeanstalk/"
          url = "#{url}?region=#{options[:region]}#/newApplication?"
          url = "#{url}applicationName=#{options[:name]}"
          url = "#{url}&solutionStackName=Ruby"
          url = "#{url}&instanceType=m1.large"
          puts "AWS Elastic Beanstalk 1-Click URL: #{url}"
        end

        # Generates the necessary file structure for a valid AWS Flow Ruby
        # application that can be deployed to Elastic Beanstalk
        def self.generate_files(options)
          dirname = "#{options[:path]}/#{options[:name]}"
          puts "Your AWS Flow Framework for Ruby application will be located at: "\
            "#{File.absolute_path(dirname)}/"
          create_app_dir(dirname)
          create_flow_dir(options)
          write_rack_config(dirname) if options[:eb]
          write_worker_config(options, dirname)
          write_gemfile(dirname)
        end

        # Creates the toplevel application directory
        def self.create_app_dir(dirname)
          FileUtils.mkdir_p(dirname)
        end

        # Creates the flow/ directory structure for the user applicaiton and
        # copies the activity and workflow files if a location is provided.
        def self.create_flow_dir(options)
          dirname = "#{options[:path]}/#{options[:name]}/flow"
          FileUtils.mkdir_p(dirname)

          str = require_files(options[:act], dirname)
          # Default to a helpful string if no activity files are given
          str = str.empty? ? "# You can require your activity files here." : str
          # Write the generated string to the activities.rb file
          write_to_file("#{dirname}/activities.rb", str)

          str = require_files(options[:wf], dirname)
          # Default to a helpful string if no workflow files are given
          str = str.empty? ? "# You can require your workflow files here." : str
          # Write the generated string to the workflows.rb file
          write_to_file("#{dirname}/workflows.rb", str)

        end

        # Helper method to copy files recursively into a directory and generate
        # a require string
        # @api private
        def self.require_files(src, dest)
          str = ""
          return str if src.nil?
          # If a valid location is passed in, copy the file(s) into the flow/
          # directory.
          FileUtils.cp_r(src, dest)
          if File.directory?(src)
            # If the given path is a directory, recursively get the relative
            # paths of all the files in the directory and generated the
            # require_relative string.
            Dir.glob("#{dest}/#{File.basename src}/**/*.rb").each do |x|
              a = Pathname.new(x)
              rel_path = a.relative_path_from(Pathname.new("#{dest}"))
              rel_path = rel_path.to_s.split(".rb").first
              str = "require_relative '#{rel_path}'\n#{str}"
            end
          else
            # If the given path is a file, just require the basename of the file
            str = "require_relative '#{File.basename src}'"
          end
          str
        end

        # Creates a rack config for the beanstalk deployment
        # @api private
        def self.write_rack_config(dirname)
          write_to_file(
            "#{dirname}/config.ru",
            "# Rack config file.\n\n"\
            "system(\"pkill -9 ruby\")\n\nsystem(\"aws-flow-ruby -f worker.json&\")\n\n"\
            "run Proc.new { |env| [200, {'Content-Type' => 'text/html'}, ['Done!']] }"
          )
        end

        # Creates the json worker config for the user application
        def self.write_worker_config(options, dirname)
          spec = {}
          spec.merge!(generate_worker_spec("activity", :activities, options))
          spec.merge!(generate_worker_spec("workflow", :workflows, options))

          spec = spec.empty? ? "" : JSON.pretty_generate(spec)
          write_to_file("#{dirname}/worker.json", spec)
        end

        # Helper method to generate a worker config
        # @api private
        def self.generate_worker_spec(name, key, options)
          spec = {}
          if options[key]
            spec = {
              "#{name}_workers" => [
                {
                  "#{name}_classes" => options[key],
                  "number_of_workers" => 10
                }
              ]
            }
          end
          return spec
        end

        # Creates the Gemfile for the user application
        def self.write_gemfile(dirname)
          write_to_file(
            "#{dirname}/Gemfile",
            "source 'http://www.rubygems.org'\n\ngem 'aws-flow', '~> 2.4.0'"
          )
        end

        # Helper method to write a string to a file
        def self.write_to_file(name, string)
          File.open(name, 'wb') { |f| f.write(string) }
        end

        # Validates various options
        def self.check_options(options)
          raise ArgumentError, "You must specify a name for your application" unless options[:name]
          options[:region] ||= ENV['AWS_REGION'].downcase if ENV['AWS_REGION']
          options[:path] ||= "."

          # We need the region for the 1-Click URL
          if options[:eb]
            raise ArgumentError, "You must specify an AWS region argument or export a "\
              "variable AWS_REGION in your shell" unless options[:region]
          end

          # If a location for activity classes is provided, then ensure that it
          # exists
          if options[:act] && !File.exist?(options[:act])
            raise ArgumentError, "Please provide a valid location where your activity "\
              "classes are located."
          end

          # If a location for workflow classes is provided, then ensure that it
          # exists
          if options[:wf] && !File.exist?(options[:wf])
            raise ArgumentError, "Please provide a valid location where your workflow "\
              "classes are located."
          end

        end

        # Main method that will be called by the FlowUtils utility.
        def self.generate(options)
          return unless options[:deploy][:enabled]
          opts = options[:deploy]
          check_options(opts)
          puts "---"
          generate_files(opts)
          generate_url(opts) if opts[:eb]
          puts "---"
        end

      end

      # Invoked from the shell.
      #
      # @api private
      def self.main
        FlowUtils.utils.each { |x| x.generate(parse_command_line) }
      end

    end
  end
end

if __FILE__ == $0
  AWS::Flow::Utils.main
end

module AWS
  module Flow
    module Utils

      require 'optparse'
      require 'json'

      # Interprets the command-line paramters pased in from the shell.
      #
      # The parameter `--name` (short: `-f`) is *required*, and must provide the
      # path to the runner configuration file.
      #
      # @api private
      def self.parse_command_line(argv = ARGV)
        options = {}
        optparse = OptionParser.new do |opts|
          opts.on('-r', '--region [REGION]', "AWS Region. Default value is taken "\
                  "from environment variable AWS_REGION if it is set.") do |f|
            options[:region] = f.downcase
          end
          opts.on('-n', '--name NAME', "Mandatory name for your application") do |f|
            options[:name] = f
          end
          opts.on('-p', '--path [PATH]', "Optional location where you want your "\
                  "AWS Flow Ruby application to be created. Default value is '.'") do |f|
            options[:path] = f
          end
          opts.on('-a', '--act [PATH]', "Optional location where your activity "\
                  "classes reside.") do |f|
            options[:act] = f
          end
        end

        optparse.parse!(argv)

        options[:region] ||= ENV['AWS_REGION'].downcase
        options[:path] ||= "."

        # We need the region for the 1-Click URL
        raise ArgumentError, "You must specify an AWS region argument or export a "\
          "variable AWS_REGION in your shell" unless options[:region]

        # If a location for activity classes is provided, then ensure that it
        # exists
        raise ArgumentError, "Please provide a valid location where your activity "\
          "classes are located." if (options[:act] && !File.exist?(options[:act]))

        return options
      end

      module BeanstalkConfig
        # Generates the appropriate 1-Click URL for beanstalk deployments based on
        # the options passed by the user
        def self.generate_url(options)
          url = "https://console.aws.amazon.com/elasticbeanstalk/"
          url = "#{url}?region=#{options[:region]}#/newApplication?"
          url = "#{url}applicationName=#{options[:name]}"
          url = "#{url}&solutionStackName=Ruby"
          url = "#{url}&instanceType=m1.large"
          puts "AWS Elastic Beanstalk 1-Click URL: #{url}"
          puts "---"
        end

        # Generates the necessary file structure for a valid AWS Flow Ruby
        # application that can be deployed to Elastic Beanstalk
        def self.generate_files(options)
          dirname = "#{options[:path]}/#{options[:name]}"
          puts "---"
          puts "Your AWS Flow Framework for Ruby application will be located at: "\
            "#{File.absolute_path(dirname)}/"
          create_app_dir(dirname)
          create_flow_dir(options)
          write_rack_config(dirname)
          write_worker_config(dirname)
          write_gemfile(dirname)
        end

        # Creates the toplevel application directory
        def self.create_app_dir(dirname)
          FileUtils.mkdir_p(dirname)
        end

        # Creates the flow/ directory structure for the user applicaiton and
        # copies the activity files if a location is provided. Otherwise creates
        # the activities.rb file with a helpful comment.
        def self.create_flow_dir(options)
          dirname = "#{options[:path]}/#{options[:name]}/flow"
          FileUtils.mkdir_p(dirname)

          str = ""
          if loc = options[:act]
            # If a valid location is passed in, copy the file(s) into the flow/
            # directory.
            FileUtils.cp_r(options[:act], dirname)
            if File.directory?(loc)
              # If the given path is a directory, recursively get the relative
              # paths of all the files in the directory and generated the
              # require_relative string.
              Dir.glob("#{dirname}/#{File.basename loc}/**.rb").each do |x|
                a = Pathname.new(x)
                rel_path = a.relative_path_from(Pathname.new("#{dirname}"))
                rel_path = rel_path.to_s.split(".rb").first
                str = "require_relative '#{rel_path}'\n#{str}"
              end
            else
              # If the given path is a file, just require the basename of the file
              str = "require_relative '#{File.basename loc}'"
            end

          else
            # Else just write a helpful comment in the activities.rb file
            str = "# You can write your Activity classes in this file.\n"
          end

          # Write the generated string to the activities.rb file
          write_to_file(
            "#{dirname}/activities.rb",
            str
          )
        end

        # Creates a rack config for the beanstalk deployment
        def self.write_rack_config(dirname)
          write_to_file(
            "#{dirname}/config.ru",
            "# Rack config file.\n\n"\
            "system(\"pkill -9 ruby\")\n\nsystem(\"aws-flow-ruby -f worker.json&\")\n\n"\
            "run Proc.new { |env| [200, {'Content-Type' => 'text/html'}, 'Done!'] }"
          )
        end

        # Creates the json worker config for the user application
        def self.write_worker_config(dirname)
          spec = {
            "activity_workers" => [
              {
                "activity_classes" => ["<enter your activity class name here>"],
                "number_of_workers" => 10
              }
            ]
          }
          write_to_file("#{dirname}/worker.json", JSON.pretty_generate(spec))
        end

        # Creates the Gemfile for the user application
        def self.write_gemfile(dirname)
          write_to_file(
            "#{dirname}/Gemfile",
            "source 'http://rubygems.org'\n\ngem 'aws-flow'"
          )
        end

        # Helper method to write a string to a file
        def self.write_to_file(name, string)
          File.open(name, 'w') { |f| f.write(string) }
        end

        def self.generate(options)
          generate_files(options)
          generate_url(options)
        end
      end

      # Invoked from the shell.
      #
      # @api private
      def self.main
        BeanstalkConfig.generate(parse_command_line)
      end

    end
  end
end

if __FILE__ == $0
  AWS::Flow::Utils.main
end

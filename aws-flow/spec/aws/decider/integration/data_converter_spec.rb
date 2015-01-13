require_relative 'setup'

describe S3DataConverter do

  it "tests regular sized input" do
    converter = FlowConstants.defaults[:data_converter]
    list = {
      input: "asdf",
      output: "ddd",
      test: 123,
    }
    s3_link = converter.dump(list)
    converter.load(s3_link).should == list
    expect_any_instance_of(AWS::S3).not_to receive(:buckets)
  end

  it "tests greater than 32k input" do
    converter = FlowConstants.defaults[:data_converter]
    list = {
      input: "asdf",
      test: "a"*33000,
    }
    s3_link = converter.dump(list)
    converter.load(s3_link).should == list
  end

  it "tests cache" do
    converter = FlowConstants.defaults[:data_converter]
    list = {
      input: "asdf",
      test: "a"*33000
    }
    s3_link = converter.dump(list)
    key = YAMLDataConverter.new.load(s3_link)

    converter.cache[key[:s3_filename]].should_not be_nil
  end

end

require_relative 'setup'

describe YAMLDataConverter do

  let(:converter) {YAMLDataConverter.new}

  %w{syck psych}.each do |engine|
    describe "ensures that x == load(dump(x)) is true using #{engine}" do
      before :all do
        if YAML.const_defined?(:ENGINE)
          YAML::ENGINE.yamler = engine
        end
      end

      {
        Fixnum => 5,
        String => "Hello World",
        Hash => {:test => "good"},
        Array => ["Hello", "World", 5],
        Symbol => :test,
        NilClass => nil,
      }.each_pair do |klass, exemplar|
        it "tests #{klass}" do
          1.upto(10).each do |i|
            converted_exemplar = exemplar
            i.times {converted_exemplar = converter.dump converted_exemplar}
            i.times {converted_exemplar = converter.load converted_exemplar}
            converted_exemplar.should == exemplar
          end
        end
      end

      it 'loads exception backtraces correctly' do
        exemplar = Exception.new('exception')
        exemplar.set_backtrace(caller)
        converted_exemplar = converter.load(converter.dump(exemplar))
        converted_exemplar.should == exemplar
      end
    end
  end
end

describe S3DataConverter do

  before(:all) do
    @bucket = ENV['AWS_SWF_BUCKET_NAME']
  end
  after(:all) do
    if @bucket
      ENV['AWS_SWF_BUCKET_NAME'] = @bucket
    else
      ENV.delete('AWS_SWF_BUCKET_NAME')
    end
  end

  let(:obj) { double }

  before(:each) do
    S3DataConverter.conv = nil
    allow(AWS::S3).to receive(:new).and_return(obj)
    allow(obj).to receive(:buckets).and_return(obj)
    allow(obj).to receive(:[]).and_return(obj)
    allow(obj).to receive(:exists?).and_return(true)
  end

  it "should not be used when AWS_SWF_BUCKET_NAME ENV variable is not set" do
    ENV['AWS_SWF_BUCKET_NAME'] = nil
    FlowConstants.data_converter.should be_kind_of(YAMLDataConverter)
  end

  it "should be used when AWS_SWF_BUCKET_NAME ENV variable is set" do
    ENV['AWS_SWF_BUCKET_NAME'] = 'foo'
    FlowConstants.data_converter.should be_kind_of(S3DataConverter)
  end

  it "uses YAMLDataConverter internally" do
    ENV['AWS_SWF_BUCKET_NAME'] = 'foo'
    FlowConstants.data_converter.converter.should be_kind_of(YAMLDataConverter)
  end

  context "#put_to_s3" do

    it "writes string to s3" do
      ENV['AWS_SWF_BUCKET_NAME'] = 'foo'
      allow(obj).to receive(:objects).and_return(obj)
      allow(obj).to receive(:create) do |filename, string|
        string.should == "foo"
      end

      converter = FlowConstants.data_converter
      converter.send(:put_to_s3, "foo")
    end
  end

  context "#get_from_s3" do

    it "reads data from s3" do
      ENV['AWS_SWF_BUCKET_NAME'] = 'foo'
      allow(obj).to receive(:objects).at_least(:once).and_return(obj)
      allow(obj).to receive(:[]).and_return(obj)
      allow(obj).to receive(:read).and_return("foo")

      converter = FlowConstants.data_converter
      converter.send(:get_from_s3, "foo_filename").should == "foo"
    end

  end

  context "#dump, #load" do

    it "dumps and loads regular sized input correctly" do
      ENV['AWS_SWF_BUCKET_NAME'] = 'foo'
      expect_any_instance_of(S3DataConverter).not_to receive(:put_to_s3)
      converter = S3DataConverter.converter
      list = {
        input: "asdf",
        output: "ddd",
        test: 123,
      }
      s3_link = converter.dump(list)
      converter.load(s3_link).should == list
    end

    it "dumps large input correctly" do
      ENV['AWS_SWF_BUCKET_NAME'] = 'foo'
      expect_any_instance_of(S3DataConverter).to receive(:put_to_s3) do |str|
        conv = YAMLDataConverter.new
        conv.load(str).should include(
          input: "asdf",
          test: "a"*33000
        )
      end
      converter = S3DataConverter.converter
      list = {
        input: "asdf",
        test: "a"*33000,
      }
      converter.dump(list)
    end

    it "loads large input correctly" do
      ENV['AWS_SWF_BUCKET_NAME'] = 'foo'
      list = {
        input: "asdf",
        test: "a"*33000,
      }
      expect_any_instance_of(S3DataConverter).to receive(:get_from_s3) do |filename|
        YAMLDataConverter.new.dump(list)
      end
      converter = S3DataConverter.converter
      filename = YAMLDataConverter.new.dump(s3_filename: "foo")
      converter.load(filename).should == list
    end
  end

  context "#cache" do

    context "#write" do

      it "ensures basic cache read/write works" do

        converter = S3DataConverter.converter
        msg = "a"*33000

        allow(obj).to receive(:objects).and_return(obj)
        allow(obj).to receive(:create)

        s3_link = converter.dump(msg)
        key = YAMLDataConverter.new.load(s3_link)

        converter.cache[key[:s3_filename]].should_not be_nil
        converter.cache[key[:s3_filename]].should == YAMLDataConverter.new.dump(msg)

        data = converter.load(s3_link)
        data.should == msg

      end

      it "ensures eviction" do

        converter = S3DataConverter.converter
        msg = "a"*33000

        allow(obj).to receive(:objects).and_return(obj)
        allow(obj).to receive(:create)

        first = YAMLDataConverter.new.load(converter.dump(msg))

        # Add 1000 more entries to evict the first one
        (1..1000).each { |x| converter.dump(msg) }

        # Ensure cache doesn't contain the first entry
        converter.cache[first[:s3_filename]].should be_nil

      end

    end

    context "#hit" do

      it "returns the entry and doesn't call S3" do

        converter = S3DataConverter.converter
        msg = "a"*33000

        allow(obj).to receive(:objects).and_return(obj)
        allow(obj).to receive(:create)

        s3_link = converter.dump(msg)

        # The following line confirms the file is not read from S3.
        expect(obj).not_to receive(:read)

        # Ensure the entry is correct
        converter.load(s3_link).should == msg

      end

      it "ensures lru behavior of cache" do

        converter = S3DataConverter.converter
        msg = "a"*33000

        allow(obj).to receive(:objects).and_return(obj)
        allow(obj).to receive(:create)

        first = YAMLDataConverter.new.load(converter.dump(msg))

        (1..999).each { |x| converter.dump(msg) }

        # Use the first entry to bring it at the front of the queue
        converter.cache[first[:s3_filename]]

        # Add a few more entries to the cache
        converter.dump(msg)
        converter.dump(msg)

        # Ensure cache still contains the entry
        converter.cache[first[:s3_filename]].should_not be_nil

      end

    end

    context "#miss" do

      it "calls S3 to get the object and adds it to the cache" do
        converter = S3DataConverter.converter
        s3_link = { s3_filename: "foo" }
        s3_link = YAMLDataConverter.new.dump(s3_link)

        # This following 2 lines confirm that we call S3 in case of a cache miss
        allow(obj).to receive(:objects).and_return(obj)
        expect(obj).to receive(:read).and_return("bar")

        # Expect the cache to get populated with a new entry
        expect(converter.cache).to receive(:[]=).with("foo", "bar")

        ret = converter.load(s3_link)
      end

    end

    it "tests max size" do
      converter = S3DataConverter.converter
      msg = "a"*33000

      allow(obj).to receive(:objects).and_return(obj)
      allow(obj).to receive(:create)

      (1..1010).each { |x| converter.dump(msg) }

      converter.cache.cache.to_a.size.should == 1000
      converter.cache.cache.clear
    end

  end
end

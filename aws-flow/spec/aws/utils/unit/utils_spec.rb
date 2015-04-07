require 'spec_helper'

describe AWS::Flow::Utils do

  # This tests a fix for Github issue #89
  it "aws-flow-utils writes files in binary mode" do
    expect(File).to receive(:open).with("name", "wb")
    AWS::Flow::Utils::InitConfig.write_to_file("name", "string")
  end

end

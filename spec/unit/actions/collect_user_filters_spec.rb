# encoding: UTF-8
#
# Copyright (c) 2010-2017 GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

describe GoodData::LCM2::CollectUserFilters do
  let(:gdc_client) { double(GoodData::Rest::Client) }
  let(:domain) { double(GoodData::Domain) }
  let(:data_source) { double(GoodData::Helpers::DataSource) }
  let(:client) { double(GoodData::Client) }
  let(:project) { double(GoodData::Project) }
  let(:params) do
    params = {
      gdc_gd_client: gdc_client,
      filters_config: { labels: [] },
      input_source: {},
      multiple_projects_column: 'client_id'
    }
    GoodData::LCM2.convert_to_smart_hash params
  end

  before do
    allow(gdc_client).to receive(:domain).and_return(domain)
    allow(domain).to receive(:clients).with('123456789', nil).and_return(client)
    allow(client).to receive(:project_uri).and_return('gdc/asd/123')
    allow(client).to receive(:project).and_return(project)
    allow(GoodData::Helpers::DataSource).to receive(:new).and_return(data_source)
    allow(data_source).to receive(:realize).and_return('filepath')
    allow(File).to receive(:open).and_return <<~EOF
      client_id
      123456789
      EOF
  end

  it 'collects filters' do
    result = subject.class.call(params)
    expect(result[:params][:user_filters]).to eq(client => [{ login: '123456789', filters: [] }])
  end
end

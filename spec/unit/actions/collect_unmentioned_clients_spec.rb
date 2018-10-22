describe GoodData::LCM2::CollectUnmentionedClients do
  let(:gdc_client) { double(GoodData::Rest::Client) }
  let(:domain) { double(GoodData::Domain) }
  let(:client) { double(GoodData::Client) }
  let(:second_client) { double(GoodData::Client) }
  let(:project) { double(GoodData::Project) }
  let(:user_filters) {
    { client => [1,2] }
  }
  let(:params) do
    params = {
      gdc_gd_client: gdc_client,
      prune_unmentioned_projects: true,
      user_filters: user_filters
    }
    GoodData::LCM2.convert_to_smart_hash params
  end

  before do
    allow(gdc_client).to receive(:domain).and_return(domain)
    allow(domain).to receive(:clients).with(:all, nil).and_return([client, second_client])
    allow(client).to receive(:project_uri).and_return('gdc/asd/123')
    allow(client).to receive(:project).and_return(project)
  end

  it 'collects users not mentioned in the user filters input' do
    result = subject.class.call(params)
    expect(result[:params][:clients_to_prune]).to eq [second_client]
  end
end

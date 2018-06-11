# encoding: UTF-8
#
# Copyright (c) 2010-2017 GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

require 'gooddata'

describe GoodData::Report, :constraint => 'slow' do
  before(:all) do
    @client = ConnectionHelper.create_default_connection
    @project, @blueprint = ProjectHelper.load_full_project_implementation(@client)

    m = @project.facts.first.create_metric
    metric_name = Dir::Tmpname.make_tmpname ['metric.some_metric'], nil
    metric_name.delete!('-')
    m.identifier = metric_name
    m.save

    test_data = [
      %w(lines_changed committed_on dev_id repo_id),
      [3, "05/01/2012", 1_012, 75],
      [2, "11/10/2014", 5_432, 23],
      [5, "01/10/2014", 45_212, 87_163],
      [1, "12/02/2017", 753, 11]
    ]
    @project.upload(test_data, @blueprint, 'dataset.commits')

    m = @project.metrics.first
    @report = @project.create_report(top: [m], title: 'Report to export')
    @report.save
  end

  after(:all) do
    @project.delete unless @project.nil?
    @client.disconnect
  end

  describe 'raw export' do
    before :each do
      @filename = Dir::Tmpname.make_tmpname([File.join(Dir::Tmpname.tmpdir, 'test_raw_export'), '.csv'], nil)
    end

    after :each do
      File.delete(@filename)
    end

    it "exports raw report" do
      @report.export_raw(@filename)
      expect(File).to exist(@filename)
      expect(File.read(@filename)).to eq("\"sum of Lines Changed\"\r\n\"11.00\"\r\n")
    end
  end

  describe 'chart report' do
    before do
      blueprint = GoodData::Model::ProjectBlueprint.build('chart report prp') do |p|
        p.add_date_dimension('committed_on')
        p.add_dataset('devs') do |d|
          d.add_anchor('attr.dev')
          d.add_label('label.dev_id', :reference => 'attr.dev')
          d.add_label('label.dev_email', :reference => 'attr.dev')
        end
        p.add_dataset('commits') do |d|
          d.add_anchor('attr.commits_id')
          d.add_fact('fact.lines_changed')
          d.add_date('committed_on')
          d.add_reference('devs')
        end
      end
      project = GoodData::Project.create_from_blueprint(blueprint, auth_token: ConnectionHelper::GD_PROJECT_TOKEN, client: @client)

      # Load data
      commits_data = [
        ['fact.lines_changed', 'committed_on', 'devs'],
        [1, '01/01/2014', 1],
        [3, '01/02/2014', 2],
        [5, '05/02/2014', 3]]
      project.upload(commits_data, blueprint, 'commits')

      devs_data = [
        ['label.dev_id', 'label.dev_email'],
        [1, 'tomas@gooddata.com'],
        [2, 'petr@gooddata.com'],
        [3, 'jirka@gooddata.com']]
      project.upload(devs_data, blueprint, 'devs')

      # create a metric
      @metric = project.facts('fact.lines_changed').create_metric
      @metric.lock
      @metric.save

      @project = project
    end
    it 'can create one' do
      label = @project.labels.to_a.find { |l| l.meta['identifier'] == 'label.dev_email' }

      report = @project.create_report(title: 'Who did the most commits?', top: [@metric], left: ['label.dev_email'], format: 'chart', chart_part: { value_uri: label.uri })
      expect(report).to be
    end

    after do
      @project.delete
    end
  end
end

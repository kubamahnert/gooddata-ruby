# encoding: UTF-8
#
# Copyright (c) 2010-2017 GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

require_relative 'base_action'

module GoodData
  module LCM2
    class CollectUserFilters < BaseAction
      DESCRIPTION = 'Enriches parameters with user filters for User Filters brick execution.'

      PARAMS = define_params(self) do
        description 'Client Used For Connecting To GD'
        param :gdc_gd_client, instance_of(Type::GdClientType), required: true

        description 'Column That Contains Target Project IDs'
        param :multiple_projects_column, instance_of(Type::StringType), required: true

        description 'Input Source'
        param :input_source, instance_of(Type::HashType), required: false

        description 'Does the input contain headers?'
        param :csv_header, instance_of(Type::StringType), required: false

        description 'Filter configuration'
        param :filters_config, instance_of(Type::HashType), required: true
      end

      class << self
        def call(params)
          filters = []
          headers_in_options = params.csv_headers == 'false' || true
          domain_name = params.domain
          data_product = params.data_product
          client = params.gdc_gd_client
          domain = client.domain(domain_name)
          multiple_projects_column = params.multiple_projects_column
          data_source = GoodData::Helpers::DataSource.new(params.input_source)

          symbolized_config = GoodData::Helpers.symbolize_keys_recursively(params.filters_config)
          csv_with_headers = GoodData::UserFilterBuilder.row_based?(symbolized_config) ? false : headers_in_options

          without_check(PARAMS, params) do
            CSV.foreach(File.open(data_source.realize(params), 'r:UTF-8'), headers: csv_with_headers, return_headers: false, encoding: 'utf-8') do |row|
              filters << row.to_hash.merge(ref_id: row[multiple_projects_column])
            end
          end

          fail 'The filter set can not be empty!' if filters.empty?

          grouped_filters = filters.group_by { |u| u[:ref_id] }
          built_filters = Hash[grouped_filters.map do |ref_id, v|
            fail "Filter #{v} is lacking reference to client" unless ref_id

            client = domain.clients(ref_id, data_product)
            fail "Client #{ref_id} does not have a project" unless client.project_uri

            [client, v.map { |f| GoodData::UserFilterBuilder.get_filters(f, symbolized_config) }.flatten]
          end]

          {
            # TODO; TMA-989 return the real results when print of results is fixed for large sets
            results: [{ status: 'ok' }],
            params: {
              user_filters: built_filters,
            }
          }
        end

      end
    end
  end
end

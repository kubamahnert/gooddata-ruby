# encoding: UTF-8
#
# Copyright (c) 2010-2017 GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

require_relative 'base_action'

module GoodData
  module LCM2
    class ProvisionUsers < BaseAction
      DESCRIPTION = 'Provision users and MUFs to the platform'

      PARAMS = define_params(self) do
        description 'Client Used For Connecting To GD'
        param :gdc_gd_client, instance_of(Type::GdClientType), required: true

        description 'Filters Config'
        param :filters_config, instance_of(Type::HashType), required: true

        description 'Makes the brick run without altering user filters'
        param :dry_run, instance_of(Type::StringType), required: false, default: false

        description 'User brick users'
        param :users_brick_users, instance_of(Type::ObjectType), required: false, default: []

        description 'Prune projects that are not mentioned in the input? (also called "declarative mode")'
        param :prune_unmentioned_projects, instance_of(Type::BooleanType), required: false, default: false

        description 'Unmentioned clients to be pruned to achieve the declarative mode'
        param :clients_to_prune, instance_of(Type::ArrayType), required: false, default: []

        description 'User filters prepared from CollectUserFilters'
        param :user_filters, instance_of(Type::HashType), required: true
      end

      class << self
        def call(params)
          prune_unmentioned_projects = true

          user_filters = params.user_filters
          users = load_users_data(params)

          users_by_project = params.users_brick_users.group_by { |u| u[:pid] }
          results = user_filters.pmap do |client, new_filters|
            users = users_by_project[client_id]
            current_project = client.project

            partial_results = sync_user_filters(current_project, new_filters, run_params.merge(users_brick_input: users), symbolized_config)
            partial_results[:results]
          end

          if prune_unmentioned_projects
            params.clients_to_prune.peach do |c|
              begin
                current_project = c.project
                users = users_by_project[c.client_id]
                params.gdc_logger.info "Delete all filters in project #{current_project.pid} of client #{c.client_id}"
                current_results = sync_user_filters(current_project, [], run_params.merge(users_brick_input: users), symbolized_config)

                results.concat(current_results[:results])
              rescue StandardError => e
                params.gdc_logger.error "Failed to clear filters of  #{c.client_id} due to: #{e.inspect}"
              end
            end
          end

          {
            results: results
          }

        end

        def filter_run_params(params)
          domain = client.domain(domain_name)

          {
            restrict_if_missing_all_values: true,
            ignore_missing_values: true,
            do_not_touch_filters_that_are_not_mentioned: false,
            domain: domain,
            dry_run: params[:dry_run].to_b,
            users_brick_input: params.users_brick_users
          }
        end

      end
    end
  end
end

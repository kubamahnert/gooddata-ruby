# encoding: UTF-8
#
# Copyright (c) 2018 GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

require_relative 'base_action'
require_relative '../user_bricks_helper'


module GoodData
  module LCM2
    class ProvisionUsers < BaseAction
      DESCRIPTION = 'Synchronizes users and user filters between projects'

      PARAMS = define_params(self) do
      #   todo
      end

      class << self
        MODES = %w(
          add_to_organization,
          sync_project,
          sync_domain_and_project,
          sync_one_project_based_on_pid,
          sync_multiple_projects_based_on_pid,
          sync_one_project_based_on_custom_id,
          sync_multiple_projects_based_on_custom_id,
          append_to_project
        )

        def version
          '0.0.1'
        end

        def call(params)
          client = params.gdc_gd_client
          domain_name = params.organization || params.domain
          fail "Either organisation or domain has to be specified in params" unless domain_name
          domain = client.domain(domain_name)
          project = client.projects(params.gdc_project) || client.projects(params.gdc_project_id)
          data_source = GoodData::Helpers::DataSource.new(params.input_source)
          data_product = params.data_product
          unless MODES.include?(params.mode)
            fail "The parameter \"sync_mode\" has to have one of the values #{MODES.map(&:to_s).join(', ')} or has to be empty."
          end

          fail 'Provisioning brick requires configuration for both user and user filters bricks' if params.filters_config.blank? || params.user_filters_config.blank?

          symbolized_config = GoodData::Helpers.symbolize_keys(GoodData::Helpers.deep_dup(config))

          user_whitelists = Set.new(params.whitelists || []) + Set.new((params.regexp_whitelists || []).map { |r| /#{r}/ }) + Set.new([client.user.login])

          [domain_name, data_source].each do |param|
            fail param + ' is required in the block parameters.' unless param
          end

          ignore_failures = GoodData::Helpers.to_boolean(params.ignore_failures)
          remove_users_from_project = GoodData::Helpers.to_boolean(params.remove_users_from_project)
          do_not_touch_users_that_are_not_mentioned = GoodData::Helpers.to_boolean(params.do_not_touch_users_that_are_not_mentioned)
          create_non_existing_user_groups = GoodData::Helpers.to_boolean(params.create_non_existing_user_groups || true)

          filters = load_user_filter_data(params, symbolized_config, project)
          users = load_user_data(params, data_source).compact

          filters_run_params = {
            restrict_if_missing_all_values: params.restrict_if_missing_all_values == 'true',
            ignore_missing_values: params.ignore_missing_values == 'true',
            do_not_touch_filters_that_are_not_mentioned: params.do_not_touch_filters_that_are_not_mentioned == 'true',
            domain: domain,
            dry_run: false,
            users_brick_input: params.users_brick_users
          }

          users_run_params = {
            domain: domain,
            whitelists: user_whitelists,
            ignore_failures: ignore_failures,
            remove_users_from_project: remove_users_from_project,
            do_not_touch_users_that_are_not_mentioned: do_not_touch_users_that_are_not_mentioned,
            create_non_existing_user_groups: create_non_existing_user_groups
          }

          users_results = []
          filters_results = []

          case mode
          when 'add_to_organization'
            users_results = domain.create_users(users.uniq { |u| u[:login] || u[:email] })
          when 'sync_project'
            users_results = project.import_users(users, users_run_params)

            filters_to_load = GoodData::UserFilterBuilder.get_filters(filters, symbolized_config)
            filters_results = project.add_data_permissions(filters_to_load, filters_run_params)
          when 'sync_domain_and_project'
            users_results = domain.create_users(users, ignore_failures: ignore_failures)
            users_results.merge project.import_users(users, users_run_params)
          when 'sync_one_project_based_on_pid'
            filtered_users = users.select { |u| u[:pid] == project.pid }
            users_results = project.import_users(filtered_users, users_run_params)
            filters_results = project.add_data_permissions(filters_to_load, filters_run_params)
          when 'sync_multiple_projects_based_on_pid'
            users.group_by { |u| u[:pid] }.flat_map do |project_id, users|
              begin
                project = client.projects(project_id)
                fail "You (user executing the script - #{client.user.login}) is not admin in project \"#{project_id}\"." unless project.am_i_admin?
                project.import_users(users, users_run_params)
              rescue RestClient::ResourceNotFound
                fail "Project \"#{project_id}\" was not found. Please check your project ids in the source file"
              rescue RestClient::Gone
                fail "Seems like you (user executing the script - #{client.user.login}) do not have access to project \"#{project_id}\""
              rescue RestClient::Forbidden
                fail "User #{client.user.login} is not enabled within project \"#{project_id}\""
              end
            end
            if filters.empty?
              fail 'The filter set can not be empty when using sync_multiple_projects_* mode as the filters contain \
                    the project ids in which the permissions should be changed'
            end
            filters.group_by { |u| u[multiple_projects_column(params.multiple_projects_column)] }.flat_map do |project_id, new_filters|
              fail "Project id cannot be empty" if project_id.blank?
              project = client.projects(project_id)
              filters_to_load = GoodData::UserFilterBuilder.get_filters(new_filters, symbolized_config)
              puts "Synchronizing #{filters_to_load.count} filters in project #{project.pid}"
              project.add_data_permissions(filters_to_load, filters_run_params)
            end
            when 'sync_one_project_based_on_custom_id'
              filter_value = UserBricksHelper.resolve_client_id(domain, project, data_product)

              filtered_users = users.select do |u|
                fail "Column for determining the project assignement is empty for \"#{u[:login]}\"" if u[:pid].blank?
                client_id = u[:pid].to_s
                client_id == filter_value
              end

              if filtered_users.empty?
                params.gdc_logger.warn(
                  "Project \"#{project.pid}\" does not match " \
                          "any client ids in input source (both " \
                          "GOODOT_CUSTOM_PROJECT_ID and SEGMENT/CLIENT). " \
                          "We are unable to get the value to filter users."
                )
              end

              puts "Project #{project.pid} will receive #{filtered_users.count} from #{users.count} users"
              project.import_users(filtered_users, users_run_params)

              if filters.empty?
                params.gdc_logger.warn "Project \"#{project.pid}\" does not match with any client ids in input source (both GOODOT_CUSTOM_PROJECT_ID and SEGMENT/CLIENT). \
                                      Unable to get the value to filter users."
              end

              filters_to_load = GoodData::UserFilterBuilder.get_filters(filters, symbolized_config)
              project.add_data_permissions(filters_to_load, filters_run_params)
            when 'sync_multiple_projects_based_on_custom_id'
              users.group_by { |u| u[:pid] }.flat_map do |client_id, users|
                fail "Client id cannot be empty" if client_id.blank?
                begin
                  project = domain.clients(client_id, data_product).project
                rescue RestClient::BadRequest => e
                  raise e unless /does not exist in data product/ =~ e.response
                  fail "The client \"#{client_id}\" does not exist in data product \"#{data_product.data_product_id}\""
                end
                fail "Client #{client_id} does not have project." unless project
                puts "Project #{project.pid} of client #{client_id} will receive #{users.count} users"
                project.import_users(users, users_run_params)
              end
              if filters.empty?
                fail 'The filter set can not be empty when using sync_multiple_projects_* mode as the filters contain \
                    the project ids in which the permissions should be changed'
              end
              filters.group_by { |u| u[multiple_projects_column(params.multiple_projects_column)] }.flat_map do |client_id, new_filters|
                fail "Client id cannot be empty" if client_id.blank?
                project = domain.clients(client_id, data_product).project
                fail "Client #{client_id} does not have project." unless project
                filters_to_load = GoodData::UserFilterBuilder.get_filters(new_filters, symbolized_config)
                project.add_data_permissions(filters_to_load, filters_run_params)
              end
          end
        end
      end

      private
      def load_user_filter_data(params, symbolized_config, project)
        filters = []
        headers_in_options = params.csv_headers == 'false' || true
        csv_with_headers = if GoodData::UserFilterBuilder.row_based?(symbolized_config)
                             false
                           else
                             headers_in_options
                           end

        multiple_projects_column = multiple_projects_column(params.multiple_projects_column)

        client_id_filter = UserBricksHelper.resolve_client_id(domain, project, data_product)

        without_check(PARAMS, params) do
          CSV.foreach(File.open(data_source.realize(params), 'r:UTF-8'), headers: csv_with_headers, return_headers: false, encoding: 'utf-8') do |row|
            case params.mode
              when 'sync_project'
                filters << row
              when 'sync_one_project_based_on_pid'
                filters << row if row[multiple_projects_column] == project.pid
              when 'sync_one_project_based_on_custom_id'
                client_id = row[multiple_projects_column].to_s
                filters << row if client_id == client_id_filter
              when 'sync_multiple_projects_based_on_pid'
              when 'sync_multiple_projects_based_on_custom_id'
              when 'sync_domain_client_workspaces'
                filters << row.to_hash
            end
          end
        end

        return filters
      end

      def load_user_data(params, data_source)
        first_name_column           = params.first_name_column || 'first_name'
        last_name_column            = params.last_name_column || 'last_name'
        login_column                = params.login_column || 'login'
        password_column             = params.password_column || 'password'
        email_column                = params.email_column || 'email'
        role_column                 = params.role_column || 'role'
        sso_provider_column         = params.sso_provider_column || 'sso_provider'
        authentication_modes_column = params.authentication_modes_column || 'authentication_modes'
        user_groups_column          = params.user_groups_column || 'user_groups'
        language_column             = params.language_column || 'language'
        company_column              = params.company_column || 'company'
        position_column             = params.position_column || 'position'
        country_column              = params.country_column || 'country'
        phone_column                = params.phone_column || 'phone'
        ip_whitelist_column         = params.ip_whitelist_column || 'ip_whitelist'
        mode                        = params.sync_mode

        sso_provider = params.sso_provider
        authentication_modes = params.authentication_modes || []

        multiple_projects_column = multiple_projects_column(params.multiple_projects_column)

        dwh = params.ads_client
        if dwh
          data = dwh.execute_select(params.input_source.query)
        else
          tmp = without_check(PARAMS, params) do
            File.open(data_source.realize(params), 'r:UTF-8')
          end
          data = CSV.read(tmp, headers: true)
        end

        data.map do |row|
          params.gdc_logger.debug("Processing row: #{row}")

          modes = if authentication_modes.empty?
                    row[authentication_modes_column] || row[authentication_modes_column.to_sym] || []
                  else
                    authentication_modes
                  end

          modes = modes.split(',').map(&:strip).map { |x| x.to_s.upcase } unless modes.is_a? Array

          user_group = row[user_groups_column] || row[user_groups_column.to_sym]
          user_group = user_group.split(',').map(&:strip) if user_group

          ip_whitelist = row[ip_whitelist_column] || row[ip_whitelist_column.to_sym]
          ip_whitelist = ip_whitelist.split(',').map(&:strip) if ip_whitelist

          {
            :first_name => row[first_name_column] || row[first_name_column.to_sym],
            :last_name => row[last_name_column] || row[last_name_column.to_sym],
            :login => row[login_column] || row[login_column.to_sym],
            :password => row[password_column] || row[password_column.to_sym],
            :email => row[email_column] || row[login_column] || row[email_column.to_sym] || row[login_column.to_sym],
            :role => row[role_column] || row[role_column.to_sym],
            :sso_provider => sso_provider || row[sso_provider_column] || row[sso_provider_column.to_sym],
            :authentication_modes => modes,
            :user_group => user_group,
            :pid => multiple_projects_column.nil? ? nil : (row[multiple_projects_column] || row[multiple_projects_column.to_sym]),
            :language => row[language_column] || row[language_column.to_sym],
            :company => row[company_column] || row[company_column.to_sym],
            :position => row[position_column] || row[position_column.to_sym],
            :country => row[country_column] || row[country_column.to_sym],
            :phone => row[phone_column] || row[phone_column.to_sym],
            :ip_whitelist => ip_whitelist
          }
        end
      end

      def multiple_projects_column(param_value)
        multiple_projects_column = param_value
        unless multiple_projects_column
          client_modes = %w(sync_domain_client_workspaces sync_one_project_based_on_custom_id sync_multiple_projects_based_on_custom_id)
          multiple_projects_column = if client_modes.include?(mode)
                                       'client_id'
                                     else
                                       'project_id'
                                     end
        end
        return multiple_projects_column
      end
    end
  end
end

class GoodData
  module LCM2
    class CollectUsers < BaseAction
      DESCRIPTION = 'Collect users for user provisioning'

      PARAMS = define_params(self) do
        description 'Input Source'
        param :input_source, instance_of(Type::HashType), required: true

        description 'Column That Contains Target Project IDs'
        param :multiple_projects_column, instance_of(Type::StringType), required: true

        description 'Authentication modes'
        param :authentication_modes, instance_of(Type::StringType), required: false

        description 'First name column'
        param :first_name_column, instance_of(Type::StringType), required: false

        description 'Last name column'
        param :last_name_column, instance_of(Type::StringType), required: false

        description 'Login column'
        param :login_column, instance_of(Type::StringType), required: false

        description 'Password column'
        param :password_column, instance_of(Type::StringType), required: false

        description 'Email column'
        param :email_column, instance_of(Type::StringType), required: false

        description 'Role column'
        param :role_column, instance_of(Type::StringType), required: false

        description 'Sso provider column'
        param :sso_provider_column, instance_of(Type::StringType), required: false

        description 'Authentication modes column'
        param :authentication_modes_column, instance_of(Type::StringType), required: false

        description 'User groups column'
        param :user_groups_column, instance_of(Type::StringType), required: false

        description 'Language column'
        param :language_column, instance_of(Type::StringType), required: false

        description 'Company column'
        param :company_column, instance_of(Type::StringType), required: false

        description 'Position column'
        param :position_column, instance_of(Type::StringType), required: false

        description 'Country column'
        param :country_column, instance_of(Type::StringType), required: false

        description 'Phone column'
        param :phone_column, instance_of(Type::StringType), required: false
      end
    end

    class << self
      def call(params)
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

        sso_provider = params.sso_provider
        authentication_modes = params.authentication_modes || []

        data_source = GoodData::Helpers::DataSource.new(params.input_source)

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
            :pid => params.multiple_projects_column.nil? ? nil : (row[params.multiple_projects_column] || row[params.multiple_projects_column.to_sym]),
            :language => row[language_column] || row[language_column.to_sym],
            :company => row[company_column] || row[company_column.to_sym],
            :position => row[position_column] || row[position_column.to_sym],
            :country => row[country_column] || row[country_column.to_sym],
            :phone => row[phone_column] || row[phone_column.to_sym],
            :ip_whitelist => ip_whitelist
          }
        end
      end
    end
  end
end

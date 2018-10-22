module GoodData
  module LCM2
    class CollectUnmentionedClients < BaseAction
      DESCRIPTION = 'Collects clients that are present in the domain but not present in the user filter input'

      PARAMS = define_params(self) do
        description 'Prune projects that are not mentioned in the input? (also called "declarative mode")'
        param :prune_unmentioned_projects, instance_of(Type::BooleanType), required: false, default: false

        description 'User filters prepared from CollectUserFilters'
        param :user_filters, instance_of(Type::HashType), required: true
      end

      class << self
        def call(params)
          client = params.gdc_gd_client
          domain_name = params.domain
          domain = client.domain(domain_name)
          data_product = params.data_product

          if params.prune_unmentioned_projects
            domain_clients = domain.clients(:all, data_product)
            if params.segments
              segment_uris = params.segments.map(&:uri)
              domain_clients.select! { |c| segment_uris.include?(c.segment_uri) }
            end

            clients = domain_clients - params.user_filters.keys
          else
            clients = []
          end


          {
            results: [{ clients: clients }],
            params: {
              clients_to_prune: clients
            }
          }
        end
      end
    end
  end
end

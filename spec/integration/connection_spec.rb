# encoding: UTF-8
#
# Copyright (c) 2010-2018 GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

require_relative '../helpers/connection_helper'

describe GoodData do
  let(:login) { 'rubydev+admin@gooddata.com' }

  it 'can use SSO' do
    rest_client = GoodData.connect_sso(login, 'test-ruby', {
      server: GoodData::Environment::ConnectionHelper::DEFAULT_SERVER,
      verify_ssl: false
    })
    user = rest_client.domain('staging2-lcm-prod').users(login, client: rest_client)
    expect(user).to be_truthy
  end

end


#!/opt/puppetlabs/puppet/bin/ruby
# frozen_string_literal: true

require 'json'
require 'uri'
require 'net/http'
require 'puppet'

# getLegacyConfig task class
class GetLegacyConfig
  def initialize(params); end

  def execute!
    query = 'inventory[certname] { '\
            '  trusted.extensions."1.3.6.1.4.1.34380.1.1.9812" = "' + role + '" and ' \
            '  trusted.extensions."1.3.6.1.4.1.34380.1.1.9813" = "' + letter + '" and ' \
            '  certname in ' + certname_array.to_json + '}'

    server = pdb_query(query).map { |n| n['certname'] }
    raise "More than one #{letter} #{role} server found!" unless server.size <= 1
    server.first
  end

  def https(port)
    https = Net::HTTP.new('localhost', port)
    https.use_ssl = true
    https.cert = @cert ||= OpenSSL::X509::Certificate.new(File.read(Puppet.settings[:hostcert]))
    https.key = @key ||= OpenSSL::PKey::RSA.new(File.read(Puppet.settings[:hostprivkey]))
    https.verify_mode = OpenSSL::SSL::VERIFY_NONE
    https
  end

  def pdb_query(query)
    pdb = https(8081)
    pdb_request = Net::HTTP::Get.new('/pdb/query/v4')
    pdb_request.set_form_data({ 'query' => query })
    JSON.parse(pdb.request(pdb_request).body)
  end
end

# Run the task unless an environment flag has been set, signaling not to. The
# environment flag is used to disable auto-execution and enable Ruby unit
# testing of this task.
unless ENV['RSPEC_UNIT_TEST_MODE']
  Puppet.initialize_settings
  task = GetLegacyConfig.new(JSON.parse(STDIN.read))
  task.execute!
end

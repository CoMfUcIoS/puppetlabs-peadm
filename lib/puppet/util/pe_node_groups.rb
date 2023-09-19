require 'puppet'
require 'puppet/http'
require 'json'

# Provides a class for interacting with PE's node classifier node group
# api.
#
# For indepth documentation, including expected input, output and
# error responses, see:
# https://docs.puppetlabs.com/pe/latest/nc_groups.html
#
# The code for this class is based primarily off the classifier node_terminus
# that is shipped with the pe-console-services-termni package.
# https://github.com/puppetlabs/classifier/blob/master/puppet/lib/puppet/indirector/node/classifier.rb
#
# In order to support the `pe_node_group` provider being called from either a
# manifest or command line via `puppet resource pe_node_group`, the initialization
# will try to discover the NC in the following order:
#
# * Will use whatever is passed in
# * Reading from Puppet[:confdir]/classifier.yaml
# ** However this pacakge only exists on the master node
# * Use hard coded defaults, with the agents cert name as the server
#
# Note: the class currently does not handle multiple Node Classifiers.
class Puppet::Util::Pe_node_groups < Object
  attr_reader :config

  API_VERSION = 'v1'
  GROUPS_ENDPOINT = 'groups'
  UPDATE_CLASSES_ENDPOINT = 'update-classes'
  CLASSIFICATION_ENDPOINT = 'classified/nodes'

  def initialize(server = nil, port = nil, prefix = nil)
    server = default_config[:server] if server.to_s.empty?
    port   = default_config[:port] if port.to_s.empty?
    prefix = default_config[:prefix] if prefix.to_s.empty?

    @config = {
      'server' => server,
      'port'   => port,
      'prefix' => prefix.chomp('/'),
    }
  end

  # Retrieve a list of all node groups in the node classifier.
  def get_groups
    response = make_request(:get, GROUPS_ENDPOINT)
    JSON.parse(response.body)
  end

  # Retrieves classification a node would receive from the classifier
  # from all node groups it is in.
  def get_classification(certname, facts = {}, trusted_facts = {})
    payload = {}
    payload['fact'] = facts if !facts.nil? && !facts.keys.empty?
    payload['trusted'] = trusted_facts if !trusted_facts.nil? && !trusted_facts.keys.empty?
    response = make_request(:post, "#{CLASSIFICATION_ENDPOINT}/#{certname}", payload.to_json)
    JSON.parse(response.body)
  end

  # Creates a node group. If an ID is specified, any existing node group
  # with that ID will be overwritten.
  #
  # Otherwise, the new node groups ID will be returned.
  #
  # @param [Hash] group_info Hash to be passed to node classifier
  def create_group(group_info)
    group_id = group_info[:id]
    if group_id
      make_request(:put, "#{GROUPS_ENDPOINT}/#{group_id}", group_info.to_json)
      group_id
    else
      res = make_request(:post, GROUPS_ENDPOINT, group_info.to_json)
      res['location'].split("/")[-1]
    end
  end

  # Delete a node group with the given ID
  #
  # @param [String] id The node group ID to delete
  def delete_group(group_id)
    endpoint = "#{GROUPS_ENDPOINT}/#{group_id}"
    make_request(:delete, endpoint)
  end

  # Updates an existing node group
  def update_group(group_info_delta)
    endpoint = "#{GROUPS_ENDPOINT}/#{group_info_delta[:id]}"
    make_request(:post, endpoint, group_info_delta.to_json)
  end


  # Updates the node classifier's cached classes, or the cached classes for a
  # specific environment if given.
  #
  # @param environment [String] The environment to refresh. Defaults to nil
  #   which will refresh all environments.
  def refresh_classes(environment = nil)
    endpoint = environment.nil? ?
      UPDATE_CLASSES_ENDPOINT :
      "#{UPDATE_CLASSES_ENDPOINT}?environment=#{environment}"
    make_request(:post, endpoint)
  end

  # Atomically pins a list of nodes to a node group
  #
  # @param [String] id The node group ID to pin to
  # @param [Array] nodes A list of certificate names to pin
  def pin_nodes_to_group(id, nodes)
    endpoint = "#{GROUPS_ENDPOINT}/#{id}/pin"
    payload = { 'nodes' => nodes }
    make_request(:post, endpoint, payload.to_json)
  end

  # Atomically unpins a list of nodes from a node group
  #
  # @param [String] id The node group ID to unpin from
  # @param [Array] nodes A list of certificate names to unpin
  def unpin_nodes_from_group(id, nodes)
    endpoint = "#{GROUPS_ENDPOINT}/#{id}/unpin"
    payload = { 'nodes' => nodes }
    make_request(:post, endpoint, payload.to_json)
  end

  private

  def default_config
    @default_config ||= load_config
  end

  def load_config
    config_path = File.join(Puppet[:confdir], 'classifier.yaml')

    config = {}
    if File.exist?(config_path)
      config = YAML.load_file(config_path)
    end

    # classifier.yaml could contain multiple NCs
    if config.respond_to?(:to_ary)
      config = config.map do |nc|
        merge_defaults(nc)
      end
      config[0]
    else
      merge_defaults(config)
    end
  end

  def merge_defaults(service)
    {
      :server => service["server"] || Puppet[:certname],
      :port => service["port"] || 4433,
      :prefix => service["prefix"] || '/classifier-api',
    }
  end

  def self.default_ssldir
    '/etc/puppetlabs/puppet/ssl'
  end

  # @return [Puppet::HTTP::Client]
  def get_http_client
    if Puppet.runtime.instance_variable_get(:@runtime_services).keys.include? :http
      runtime_service = :http
    else
      runtime_service = 'http'
    end
    client = Puppet.runtime[runtime_service]
  end

  # If we are operating in the standard puppet-agent environment
  # with Puppet[:ssldir] set to /etc/puppetlabs/puppet/ssl, then
  # this will return nil, because the default ssl context that
  # the client will generate is the correct one.
  #
  # However, if we are running within a Bolt apply, where ssldir
  # has been set to a /tmp directory, we need to build a working ssl
  # context from the actual agent PKI under /etc/puppetlabs/puppet/ssl
  # in order to be able to interact with the classifier.
  #
  # @return [Puppet::SSL::SSLContext] or nil.
  def get_ssl_context
    if Puppet[:ssldir].start_with?('/tmp')
      ssldir = self.class.default_ssldir
      certname = Puppet[:certname]
      cert = Puppet::X509::CertProvider.new(
        capath: "#{ssldir}/certs/ca.pem",
        crlpath: "#{ssldir}/crl.pem",
        privatekeydir: "#{ssldir}/private_keys",
        certdir: "#{ssldir}/certs",
        hostprivkey: "#{ssldir}/private_keys/#{certname}.pem",
        hostcert: "#{ssldir}/certs/#{certname}.pem",
      )
      cacerts = cert.load_cacerts(required: true)
      crls = cert.load_crls(required: true)
      private_key = cert.load_private_key(certname, required: true, password: nil)
      client_cert = cert.load_client_cert(certname, required: true)
      ssl = Puppet::SSL::SSLProvider.new
      ssl.create_context(
        cacerts: cacerts,
        crls: crls,
        private_key: private_key,
        client_cert: client_cert,
        revocation: true
      )
    end
  end

  def make_request(type, endpoint, payload="")
    client = get_http_client
    headers = {'Content-Type' => 'application/json'}
    options = { ssl_context: get_ssl_context }

    api_url = "#{@config['prefix']}/#{API_VERSION}/#{endpoint}"
    full_uri = URI("https://#{@config['server']}:#{@config['port']}#{api_url}")
    if ENV['PE_NODE_GROUP_CLASSIFICATION_ATTEMPTS']
      max_attempts = ENV['PE_NODE_GROUP_CLASSIFICATION_ATTEMPTS'].to_i
    else
      max_attempts = 5
    end
    attempts = 0
    while attempts < max_attempts
      attempts += 1

      Puppet.debug("pe_node_group: requesting #{type} #{api_url}")
      case type
      when :delete
        response = client.delete(full_uri, headers: headers, options: options)
      when :get
        response = client.get(full_uri, headers: headers, options: options)
      when :post
        response = client.post(full_uri, payload, headers: headers, options: options)
      when :put
        response = client.put(full_uri, payload, headers: headers, options: options)
      else
        raise Puppet::Error, "pe_node_groups#make_request called with invalid request type #{type}"
      end

      case response.code
      when 200..399
        return response
      # PE-15108 Retry on 500 (Internal Server Error) and 400 (Bad request) errors
      when 500, 400
        if attempts < max_attempts
          Puppet.debug("Received #{response} error from #{service_url}, attempting to retry. (Attempt #{attempts} of #{max_attempts})")
          Kernel.sleep(10)
        else
          raise Puppet::Error, "Received #{attempts} server error responses from the Node Manager service at #{service_url}: #{response.code} #{response.body}"
        end
      else
        raise Puppet::Error, "Received an unexpected error response from the Node Manager service at #{service_url}: #{response.code} #{response.body}"
      end
    end
  end

  # Helper method for returning a user friendly url for the node classifier being used.
  def service_url
    "https://#{@config['server']}:#{@config['port']}#{@config['prefix']}"
  end

  def normalize_prefix(prefix)
    prefix.chomp('/')
  end
end
require 'puppet/util/pe_node_groups'

# Based off prosvcs-node_manager. The major difference
# is it does not rely on a gem for interacting with the classifier.
# https://github.com/puppetlabs/prosvcs-node_manager
Puppet::Type.type(:pe_node_group).provide(:ruby) do
  mk_resource_methods

  NODE_ID_REGEX = Regexp.new('^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$')

  def self.instances
    self.ngs.collect do |group|
      ngs_hash = { :ensure => :present }
      group.each do |key, value|
        if key == 'parent'
          # Replace parent ID with string name
          ngs_hash[:parent] = get_parent_name(group['parent'])
        elsif key == 'rule'
          ngs_hash[key.to_sym] = value
          ngs_hash[:pinned] = get_pinned_nodes_from_rule(value)
        else
          ngs_hash[key.to_sym] = value
        end
      end
      new(ngs_hash)
    end
  end

  # Utility method for extracting pinned nodes from a rule array.
  # Based off code from the classifier UI:
  # https://github.com/puppetlabs/pe-classifier-ui/blob/f08acd3ac0cd9dd3440395c8adad0293efa90bff/dev-resources/classifier-ui/app/pods/classifier/uiadapters/rules.js#L74
  def self.get_pinned_nodes_from_rule(rule)
    pinned = []
    inside_or = false
    rule.each do |condition|
      if inside_or
        if condition.length == 3
          operator = condition[0]
          field_name = condition[1]
          value = condition[2]

          if operator == '=' && field_name == 'name'
            pinned << value
          end
        end
      elsif condition == 'or'
        inside_or = true
      end
    end

    pinned
  end

  def self.prefetch(resources)
    refresh_environments = resources.values
      .select { |r| r[:refresh_classes] == true }
      .collect { |r| r[:environment] }
      .uniq

    addresses = resources.values.collect do |r|
      spp = [r[:server], r[:port], r[:prefix]]
      spp.compact.empty? ? nil : spp
    end.compact

    # If there are multiple addresses they must be the same.
    raise Puppet::Error, "Specifying different server, port or prefix parameters is not currently supported." if addresses.uniq.length > 1

    server, port, prefix = addresses.first
    @classifier = init_pe_node_groups(server, port, prefix)

    refresh_environments.each do |env|
      @classifier.refresh_classes(env)
    end

    node_groups = instances
    resources.keys.each do |group|
      if provider = node_groups.find{ |g| g.name.downcase == group.downcase }
        resources[group].provider = provider
      end
    end
  end

  def exists?
    @property_hash[:ensure] == :present
  end

  def create
    @noflush = true

    send_data = {}
    api_keys.each do |k|
      send_data[k] = @resource[k.to_sym] if @resource[k.to_sym] != nil
    end

    # If the parent is the name, convert it to the GUID
    unless send_data['parent'] =~ NODE_ID_REGEX
      send_data['parent'] = get_parent_id(send_data['parent'])
    end

    resp = self.class.classifier.create_group(send_data)

    if resp
      @resource.original_parameters.each_key do |k|
        if k == :ensure
          @property_hash[:ensure] = :present
        else
          @property_hash[k]       = @resource[k]
        end
      end
      # Add placeholder for ngs lookups
      self.class.ngs << { "name" => send_data['name'], "id" => resp }
      @property_hash[:id] = resp

      if @resource[:pinned] && !@resource[:pinned].empty?
        self.class.classifier.pin_nodes_to_group(resp, @resource[:pinned])
      end
    else
      fail("pe_node_groups was not able to create group")
    end

    exists? ? (return true) : (return false)
  end

  def destroy
    @noflush = true
    begin
      self.class.classifier.delete_group(@property_hash[:id])
      @property_hash.clear
    rescue Exception => e
      fail(e.message)
      debug(e.backtrace.inspect)
    end
    exists? ? (return false) : (return true)
  end

  # If ID is given, translate to string name
  def parent
    if @resource[:parent] =~ NODE_ID_REGEX
      self.class.get_parent_name(@resource[:parent])
    else
      @property_hash[:parent]
    end
  end

  def undef_to_nil(payload)
    if payload.is_a?(Hash)
      payload.each do |key, val|
        payload[key] = undef_to_nil(val)
      end
    elsif payload == :undef
      return nil
    end
    payload
  end

  def flush
    return if @noflush
    begin
      payload = {}
      api_keys.each do |k|
        payload[k.to_sym] = @property_hash[k.to_sym] if @property_hash[k.to_sym] != nil
      end
      payload[:id] = @property_hash[:id]
      # If parent is specified as the string name, convert it to the GUID
      unless payload[:parent] =~ NODE_ID_REGEX
        parent_id = get_parent_id(payload[:parent])
        if parent_id
          payload[:parent] = parent_id
        end
      end

      payload = undef_to_nil(payload)

      self.class.classifier.update_group(payload)

      if @property_hash[:pinned] && !@property_hash[:pinned].empty?
        self.class.classifier.pin_nodes_to_group(payload[:id], [@property_hash[:pinned]].flatten)
      end

      if @property_hash[:unpinned] && !@property_hash[:unpinned].empty?
        self.class.classifier.unpin_nodes_from_group(payload[:id],[@property_hash[:unpinned]].flatten)
      end
    rescue Exception => e
      fail(e.message)
      debug(e.backtrace.inspect)
    end
  end

  private

  def self.classifier
    @classifier ||= self.init_pe_node_groups
  end

  def self.init_pe_node_groups(server = nil, port = nil, prefix = nil)
    Puppet::Util::Pe_node_groups.new(server, port,prefix)
  end

  def self.ngs
    @ngs ||= classifier.get_groups
  end

  def self.get_parent_name(id)
    ngs_parent_index = self.ngs.index { |i| i['id'] == id }
    self.ngs[ngs_parent_index]['name']
  end

  def get_parent_id(name)
    parent_index = self.class.ngs.index { |i| i['name'] == name }
    if parent_index
      self.class.ngs[parent_index]['id']
    else
      nil
    end
  end

  # The console services API is strict when it comes to the body being passed in.
  # If it receives any key other then what it expects, it will return with an error.
  #
  # This is the list of common api parameters to use when
  def api_keys
    ['name', 'environment', 'environment_trumps', 'description', 'parent', 'rule', 'variables', 'classes']
  end
end
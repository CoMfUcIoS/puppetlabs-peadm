require 'puppet/parameter/boolean'
require 'puppet/property/boolean'

# Based off prosvcs-node_manager. The major difference
# is it does not rely on a gem for interacting with the classifier.
# https://github.com/puppetlabs/prosvcs-node_manager
Puppet::Type.newtype(:pe_node_group) do
  @doc = %q{
    Type for managing node groups in PE's node classifier.

    If no server, port or prefix parameter is specified, the type
    will first try to load the settings from classifier.yaml. If
    the file does not exist (in the event of a split install), the
    type will then fall back to using the agent's certname as the server.

    The parent parameter can be specified as either the GUID, or name.

    For more documentation, visit:
    https://docs.puppetlabs.com/pe/latest/nc_index.html

    Example:
      pe_node_group { 'PE Infrastructure':
        parent  => '00000000-0000-4000-8000-000000000000',
        refresh_classes => true,
        classes => {
          'puppet_enterprise' => {
            'certificate_authority_host'   => 'ca.example.vm',
            'puppet_master_host'           => 'master.example.vm',
            'console_host'                 => 'console.example.vm',
            'puppetdb_host'                => 'puppetdb.example.vm',
            'database_host'                => 'puppetdb.example.vm',
            'pcp_broker_host'              => 'master.example.vm',
          }
        },
      }

      pe_node_group { 'PE Certificate Authority':
        parent  => 'PE Infrastructure',
        rule    => ['or', ['=', 'name', $::pe_install::ca_certname]],
        classes => {
          'puppet_enterprise::profile::certificate_authority' => {},
        }
      }

  }
  ensurable

  newparam(:name, :namevar => true) do
    desc 'This is the common name for the node group'
    validate do |value|
      fail 'Node group must have a name' if value == ''
      fail 'name should be a String' unless value.is_a?(String)
    end
  end

  newproperty(:id) do
    desc 'The ID of the group'

    validate do |value|
      fail("ID is read-only")
    end
  end

  newproperty(:description) do
    desc 'The description of the node group'

    validate do |value|
      fail 'parent should be a String' unless value.is_a?(String)
    end

    def insync?(is)
      if self.resource[:create_only]
        true
      else
        super
      end
    end
  end

  newproperty(:environment_trumps, :boolean => false, :parent => Puppet::Property::Boolean) do
    desc "Whether this node group's environment should override those of other node
      groups at classification-time.

      This key is optional; if it's not provided, the default value of false will be used."

    def insync?(is)
      if self.resource[:create_only]
        true
      else
        super
      end
    end
  end

  newproperty(:parent) do
    desc 'The ID or name of the parent group'
    defaultto '00000000-0000-4000-8000-000000000000'

    validate do |value|
      fail 'parent should be a String' unless value.is_a?(String)
    end

    # The classifier API expects the parent parameter to be a GUID.
    # However to allow users to create parent and child groups in the manifest,
    # the provider has code to convert the name to GUID behind the scenes.
    #
    # In order to prevent change events from occuring due to the mismatch,
    # this custom insync method will check if the string based name
    # matches the corresponding node groups name that was retrieved
    # during prefetch.
    def insync?(is)
      return true if self.resource[:create_only]

      if should =~ /^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$/
        is == resource.provider.class.get_parent_name(should)
      else
        is == should
      end
    end
  end

  newproperty(:variables) do
    desc "Variables set this group's scope"

    validate do |value|
      fail("variables should be a Hash") unless value.is_a?(Hash)
    end

    def insync?(is)
      if self.resource[:create_only]
        true
      else
        super
      end
    end
  end

  newproperty(:rule, :array_matching => :all) do
    desc %q{The condition that must be satisfied for a node to be classified
      into this node group. The structure of this condition is described in
      the "Rule Condition Grammar" section at:
      https://docs.puppetlabs.com/pe/latest/nc_groups.html#rule-condition-grammar.}

    def insync?(is)
      if self.resource[:create_only]
        true
      else
        # Becaused the pinned property inserts a rule, we need to insert the pinned
        # nodes into the should value or else the rule and pinned properties will conflict.
        pinned = resource.provider.pinned
        unless pinned == :absent || pinned.empty?
          rules = ["or", should]
          pinned.each { |node| rules << ["=", "name", node] }
          @should = rules
        end
        super
      end
    end
  end

  newproperty(:pinned, :array_matching => :all) do
    desc %q{An array of certnames that should be pinned to this node group}

    validate do |value|
      if self.resource[:unpinned] && !([self.resource[:unpinned]].flatten & [value].flatten).empty?
        fail("Identical nodes specified in pinned and unpinned")
      end
    end
    # This property is considered insync if the list of pinned nodes
    # on the resource is included in the complete list of pinned nodes.
    # Or if there are no pinned nodes, and that differs from should.
    def insync?(is)
      return true if self.resource[:create_only]

      if (is.is_a?(Symbol) || should.is_a?(Symbol))
        return should == is || (is == :absent && (should.empty? || should.nil?))
      else
        return (should - is).empty?
      end
    end
  end

  newproperty(:unpinned, :array_matching => :all) do
    desc %q{An array of certnames that should not be pinned to this node group}

    validate do |value|
      if self.resource[:pinned] && !([self.resource[:pinned]].flatten & [value].flatten).empty?
        fail("Identical nodes specified in pinned and unpinned")
      end
    end

    def insync?(is)
      pinned = resource.provider.pinned
      return true if self.resource[:create_only] || should.empty? || should.nil? || pinned == :absent

      # Since this isn't a real property, and we aren't generating an
      # unpinned property in the provider .instances function, "is" will
      # always be :absent. Instead, check if the nodes in unpinned show up in pinned.
      return pinned.empty? || (pinned & [should].flatten).empty?
    end
  end

  newproperty(:environment) do
    desc 'Environment for this group'
    defaultto :production

    validate do |value|
      # Regex is from https://docs.puppetlabs.com/puppet/4.3/reference/environments_creating.html#allowed-environment-names
      fail("Invalid environment name") unless value =~ /\A[a-z0-9_]+\Z/ or value == 'agent-specified'
    end

    def insync?(is)
      if self.resource[:create_only]
        true
      else
        super
      end
    end
  end

  newproperty(:classes) do
    desc 'Classes applied to this group'
    defaultto {}
    validate do |value|
      fail("classes must be supplied as a hash") unless value.is_a?(Hash)
    end

    def deep_check(is, should)
      should.each do |key, val|
        # If the resource has set the parameter value to undef when we have
        # to check if the parameter exists before attempting to remove it
        # from classification. If the parameter doesn't exist we can continue
        # to check the remaining parameters.
        if val == :undef || val.nil?
          if is.key?(key)
            return false
          else
            next
          end
        elsif val.is_a?(Hash) && is[key].is_a?(Hash)
          return false if ! deep_check(is[key], val)
        elsif val != is[key]
          return false
        end
      end
      true
    end

    def insync?(is)
      return true if self.resource[:create_only]

      # Short circuit check if the top level keys which should exist do not
      # match between resource and classification.
      should_keys = should.keys.select do |key|
        should[key] != :undef && !should[key].nil?
      end
      return false if ! (should_keys - is.keys).empty?

      return deep_check(is, should)
    end
  end

  newparam(:refresh_classes, :boolean => true, :parent => Puppet::Parameter::Boolean) do
    desc 'Refresh classes before making call'
    defaultto :false
  end

  newparam(:server) do
    desc 'The url of the classifier server'
  end

  newparam(:port) do
    desc 'The port of the classifier server'
  end

  newparam(:prefix) do
    desc 'The prefix of the classifier server'
  end

  newparam(:create_only, :boolean => true, :parent => Puppet::Parameter::Boolean) do
    desc 'Only add this class if it doesn\'t already exist in the classifier'
    defaultto :false
  end

  autorequire(:pe_node_group) do
    self[:parent] if @parameters.include? :parent
  end
end
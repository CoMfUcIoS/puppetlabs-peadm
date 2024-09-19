plan peadm_spec::add_inventory_hostnames(
  String[1] $inventory_file
) {
  $t = get_targets('*')
  wait_until_available($t)

  $fqdn_results = $t.map |$target| {
    $fqdn = run_command('hostname -f', $target).first
    if $fqdn['exit_code'] != 0 {
      fail("Failed to get FQDN for target ${target.name}: ${fqdn['stderr']}")
    }
    $target.set_var('certname', $fqdn['stdout'].chomp)
  }

  $fqdn_results.each |$result| {
    $command = "yq eval '(.groups[].targets[] | select(.uri == \"${uri}\").name) = \"${certname}\"' -i ${inventory_file}"
    $result = run_command($command, 'localhost').first
    if $result['exit_code'] != 0 {
      fail("Failed to update inventory file for target ${uri}: ${result['stderr']}")
    }
    $command = "yq eval '(.groups[].targets[] | select(.uri == \"${uri}\").name) = \"${certname}\"' -i ${inventory_file}"
    run_command($command, 'localhost')
  }
}

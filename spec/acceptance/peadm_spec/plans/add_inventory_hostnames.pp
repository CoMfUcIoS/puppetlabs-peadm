plan peadm_spec::add_inventory_hostnames(
  String[1] $inventory_file
) {
  $t = get_targets('*')
  wait_until_available($t)

  $fqdn_results = await(
    parallelize($t) |$target| {
      $fqdn = run_command('hostname -f', $target)
      $target.set_var('certname', $fqdn.first['stdout'].chomp) { 'uri' => $target.uri, 'certname' => $target.vars['certname'] }
    }
  )

  $fqdn_results.each |$result| {
    $command = "yq eval '(.groups[].targets[] | select(.uri == \"${result.target.uri}\").name) = \"${result.value}\"' -i ${inventory_file}"
    run_command($command, 'localhost')
  }
  notify { 'Inventory updated': }
}

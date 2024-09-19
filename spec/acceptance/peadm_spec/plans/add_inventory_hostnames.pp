plan peadm_spec::add_inventory_hostnames(
  String[1] $inventory_file
) {
  $t = get_targets('*')
  wait_until_available($t)

  $fqdn_results = wait(
    parallelize($t) |$target| {
      $fqdn = run_command('hostname -f', $target)
      $target.set_var('certname', $fqdn.first['stdout'].chomp)
    }
  )

  $fqdn_results.each |$result| {
    $command = "yq eval '(.groups[].targets[] | select(.uri == \"${result.target.uri}\").name) = \"${result.value}\"' -i ${inventory_file}"
    run_command($command, 'localhost')
  }
}

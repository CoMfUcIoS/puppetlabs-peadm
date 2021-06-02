plan peadm_spec::install_test_cluster (
  $architecture,
  $version,
) {

  $t = get_targets('*')
  wait_until_available($t)

  parallelize($t) |$target| {
    $fqdn = run_command('hostname -f', $target)
    $target.set_var('certname', $fqdn.first['stdout'].chomp)
  }

  $common_params = {
    console_password => 'puppetlabs',
    download_mode    => 'direct',
    version          => $version,
  }

  $arch_params =
    case $architecture {
      'standard': {{
        primary_host => $t.filter |$n| { $n.vars['role'] == 'primary' },
      }}
      'standard-with-dr': {{
        primary_host         => $t.filter |$n| { $n.vars['role'] == 'primary' },
        primary_replica_host => $t.filter |$n| { $n.vars['role'] == 'replica' },
      }}
      'large': {{
        primary_host   => $t.filter |$n| { $n.vars['role'] == 'primary' },
        compiler_hosts => $t.filter |$n| { $n.vars['role'] == 'compiler' },
      }}
      'large-with-dr': {{
        primary_host         => $t.filter |$n| { $n.vars['role'] == 'primary' },
        primary_replica_host => $t.filter |$n| { $n.vars['role'] == 'replica' },
        compiler_hosts       => $t.filter |$n| { $n.vars['role'] == 'compiler' },
      }}
      'extra-large': {{
        primary_host           => $t.filter |$n| { $n.vars['role'] == 'primary' },
        puppetdb_database_host => $t.filter |$n| { $n.vars['role'] == 'primary-pdb-postgresql' },
        compiler_hosts         => $t.filter |$n| { $n.vars['role'] == 'compiler' },
      }}
      'extra-large-with-dr': {{
        primary_host                   => $t.filter |$n| { $n.vars['role'] == 'primary' },
        puppetdb_database_host         => $t.filter |$n| { $n.vars['role'] == 'primary-pdb-postgresql' },
        primary_replica_host           => $t.filter |$n| { $n.vars['role'] == 'replica' },
        puppetdb_database_replica_host => $t.filter |$n| { $n.vars['role'] == 'replica-pdb-postgresql' },
        compiler_hosts                 => $t.filter |$n| { $n.vars['role'] == 'compiler' },
      }}
    }

  $install_result =
    run_plan("peadm::provision", $arch_params + $common_params)

  return($install_result)
}

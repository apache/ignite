package org.apache.ignite.snippets;

import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.jupiter.api.Test;

public class BackupFilter {

	@Test
	void backupFilter() {
		
		//tag::backup-filter[]
		CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<Integer, String>("myCache");

		cacheCfg.setBackups(1);
		RendezvousAffinityFunction af = new RendezvousAffinityFunction();
		af.setAffinityBackupFilter(new ClusterNodeAttributeAffinityBackupFilter("AVAILABILITY_ZONE"));

		cacheCfg.setAffinity(af);
		//end::backup-filter[]
	}
}

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/** */
public class SnapshotTmpDirCleanupTest  extends AbstractSnapshotSelfTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (listeningLog != null)
            cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** */
    @Test
    public void testClusterSnapshotCheck() throws Exception {
        String snpDir = U.defaultWorkDirectory() + "/db/snapshot_" + getClass().getSimpleName() + "0/snp";

        String msg = String.format("Snapshot directory doesn't exist [snpName=%s, dir=%s]", SNAPSHOT_NAME, snpDir);

        IgniteEx ignite = startGridsWithCache(1, dfltCacheCfg, CACHE_KEYS_RANGE);

        LogListener lsnr = LogListener.matches(msg).build();

        listeningLog.registerListener(lsnr);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        assertFalse(lsnr.check());
    }
}

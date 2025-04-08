package org.apache.ignite.compatibility.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** */
public class NodeFileTreeCompatibilityTest extends IgniteNodeFileTreeCompatibilityAbstractTest {
    /** */
    @Parameter(5)
    public int nodesCnt;

    /** */
    private static final String OLD_WORK_DIR;

    static {
        try {
            OLD_WORK_DIR = String.format("%s-%s", U.defaultWorkDirectory(), OLD_IGNITE_VERSION);
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Restoring cache dump and any kind of snapshot is pointless.
     */
    @Parameters(name = "incSnp={0}, customConsId={1}, cacheDump={2}, customSnpPath={3}, testCacheGrp={4}, nodesCnt={5}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();

        for (boolean incSnp : Arrays.asList(true, false))
            for (boolean customConsId: Arrays.asList(true, false))
                for (boolean cacheDump : Arrays.asList(true, false))
                    for (boolean customSnpPath : Arrays.asList(true, false))
                        for (boolean testCacheGrp : Arrays.asList(true, false))
                            for (int nodesCnt : Arrays.asList(1, 3))
                                if (!incSnp || !cacheDump)
                                    data.add(new Object[]{incSnp, customConsId, cacheDump, customSnpPath, testCacheGrp, nodesCnt});

        return data;
    }

    /** */
    @Test
    public void testNodeFileTree() throws Exception {
        try {
            for (int i = 1; i < nodesCnt; ++i) {
                startGrid(
                        i,
                        OLD_IGNITE_VERSION,
                        new ConfigurationClosure(incSnp, consId(customConsId, i), customSnpPath, true, cacheGrpInfo, OLD_WORK_DIR)
                );
            }

            IgniteEx oldIgn = startGrid(
                    nodesCnt,
                    OLD_IGNITE_VERSION,
                    new ConfigurationClosure(incSnp, consId(customConsId, nodesCnt), customSnpPath, true, cacheGrpInfo, OLD_WORK_DIR),
                    new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpInfo)
            );

            stopAllGrids();

            IgniteEx curIgn = startGrids(nodesCnt);

            new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpInfo).apply(curIgn);

            NodeFileTree oldFt = new NodeFileTree()

            NodeFileTree curFt = curIgn.context().pdsFolderResolver().fileTree();
        }
        finally {
            stopAllGrids();

            cleanPersistenceDir(false);
        }
    }

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        new ConfigurationClosure(incSnp, consId(customConsId, 1), customSnpPath, true, cacheGrpInfo).apply(cfg);

        return cfg;
    }
}

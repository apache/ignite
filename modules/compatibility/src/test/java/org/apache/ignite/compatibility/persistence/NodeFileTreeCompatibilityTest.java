package org.apache.ignite.compatibility.persistence;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assume.assumeTrue;

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
        assumeTrue(nodesCnt == 3);

        try {
            ArrayList<IgniteEx> oldNodes = new ArrayList<>(nodesCnt);
            ArrayList<IgniteEx> curNodes = new ArrayList<>(nodesCnt);

            for (int i = 1; i < nodesCnt; ++i) {
                oldNodes.add(
                    startGrid(
                        i,
                        OLD_IGNITE_VERSION,
                        new ConfigurationClosure(incSnp, consId(customConsId, i), customSnpPath, true, cacheGrpInfo, OLD_WORK_DIR)
                    )
                );
            }

            oldNodes.add(
                startGrid(
                    nodesCnt,
                    OLD_IGNITE_VERSION,
                    new ConfigurationClosure(incSnp, consId(customConsId, nodesCnt), customSnpPath, true, cacheGrpInfo, OLD_WORK_DIR),
                    new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpInfo)
                )
            );

            stopAllGrids();

            cleanPersistenceDir();

            for (int i = 0; i < nodesCnt; ++i) {
                int finalI = i;

                curNodes.add(
                    startGrid(
                        i,
                        cfg -> {
                            try {
                                new ConfigurationClosure(
                                    incSnp,
                                    consId(customConsId, finalI),
                                    customSnpPath,
                                    true,
                                    cacheGrpInfo
                                ).apply(cfg);
                            } catch (IgniteCheckedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    )
                );
            }

            new CreateSnapshotClosure(incSnp, cacheDump, cacheGrpInfo).apply(curNodes.get(0));

            compareFileTrees(OLD_WORK_DIR, U.defaultWorkDirectory());
        }
        finally {
            stopAllGrids();

            cleanPersistenceDir(false);
        }
    }

    private void compareFileTrees(String oldWorkDirPath, String curWorkDirPath) {
        File oldWorkDir = new File(oldWorkDirPath);
        File curWorkDir = new File(curWorkDirPath);
    }
}

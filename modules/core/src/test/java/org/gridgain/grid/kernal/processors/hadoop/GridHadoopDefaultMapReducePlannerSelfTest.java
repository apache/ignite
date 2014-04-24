/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.hadoop2impl.*;
import org.gridgain.grid.kernal.processors.hadoop.planner.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * TODO: Add class description.
 */
public class GridHadoopDefaultMapReducePlannerSelfTest extends GridHadoopAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean ggfsEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setMapReducePlanner(new GridHadoopDefaultMapReducePlanner());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public GridGgfsConfiguration ggfsConfiguration() {
        GridGgfsConfiguration cfg = super.ggfsConfiguration();

        // Disable fragmentizer for this test.
        cfg.setFragmentizerEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapReducePlanSingleFile() throws Exception {
        GridEx grid = grid(0);

        GridGgfs ggfs = grid.ggfs(ggfsName);

        String fileName = "/testFile";

        long fileSize = 256 * 1024;

        prepareFile(ggfs, fileName, fileSize);

        Collection<GridGgfsBlockLocation> aff = ggfs.affinity(new GridGgfsPath(fileName), 0, fileSize);

        // Prepare hadoop file blocks.
        Collection<GridHadoopFileBlock> blocks = new ArrayList<>(aff.size());

        String[] hosts = new String[] {F.first(grid.localNode().hostNames())};

        for (GridGgfsBlockLocation loc : aff) {
            GridHadoopFileBlock block = new GridHadoopFileBlock(hosts, new URI("ggfs://ipc" + fileName), loc.start(),
                loc.length());

            blocks.add(block);
        }

        GridKernal kernal = (GridKernal)grid;

        GridHadoopMapReducePlanner planner = kernal.context().hadoop().context().planner();

        Configuration cfg = jobConfiguration();

        GridHadoopDefaultJobInfo info = new GridHadoopDefaultJobInfo(cfg);

        GridHadoopJob job = new GridHadoopV2JobImpl(new GridHadoopJobId(UUID.randomUUID(), 1), info);

        Collection<GridNode> nodes = grid.nodes();

        GridHadoopMapReducePlan plan = planner.preparePlan(blocks, nodes, job, null);

        int totalBlocks = 0;

        for (GridNode n : nodes)
            totalBlocks += plan.mappers(n.id()).size();

        assertEquals(aff.size(), totalBlocks);

        // Verify plan.
        for (GridGgfsBlockLocation loc : aff) {
            UUID primary = F.first(loc.nodeIds());

            Collection<GridHadoopFileBlock> mappers = plan.mappers(primary);

            assertTrue("Failed to find affinity block location in plan [loc=" + loc + ", mappers=" + mappers + ']',
                hasLocation(loc, fileName, mappers));
        }
    }

    private void prepareFile(GridGgfs ggfs, String fileName, long fileSize) throws Exception {
        try (OutputStream os = ggfs.create(new GridGgfsPath(fileName), true)) {

        }

    }

    /**
     * @param loc Location to find.
     * @param name File name.
     * @param mappers Mappers to search.
     * @return {@code True} if location was found.
     */
    private boolean hasLocation(GridGgfsBlockLocation loc, String name, Iterable<GridHadoopFileBlock> mappers) {
        for (GridHadoopFileBlock block : mappers) {
            if (block.file().getPath().equals(name) && loc.start() == block.start() && loc.length() == block.length())
                return true;
        }

        return false;
    }

    /**
     * @return Map-reduce job configuration.
     */
    private Configuration jobConfiguration() {
        Configuration cfg = new Configuration();

        cfg.setStrings("fs.ggfs.impl", GridGgfsHadoopFileSystem.class.getName());

        cfg.setInt(JobContext.NUM_REDUCES, 12);

        return cfg;
    }
}

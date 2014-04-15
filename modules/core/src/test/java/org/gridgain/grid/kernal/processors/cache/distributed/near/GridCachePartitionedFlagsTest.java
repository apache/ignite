package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.cache.GridCacheMode;
import org.gridgain.grid.kernal.processors.cache.GridCacheAbstractFlagsTest;

import java.text.*;
import java.util.*;

public class GridCachePartitionedFlagsTest extends GridCacheAbstractFlagsTest {

    @Override
    protected GridCacheMode cacheMode() {
        return GridCacheMode.PARTITIONED;
    }

    @Override
    public void testTestSyncCommitFlag() throws Exception {
        // Temporary disable test run.
        if (new Date().compareTo(new SimpleDateFormat("dd.MM.yyyy").parse("01.06.2012")) < 0)
            return;

        super.testTestSyncCommitFlag();
    }
}

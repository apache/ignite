package org.apache.ignite.internal.processors.cache.database;

import junit.framework.TestCase;

public class IgniteCacheDatabasePartitionManagerSelfTest extends TestCase {

    public void test() {
        IgniteCacheDatabasePartitionManager partMgr = new IgniteCacheDatabasePartitionManager();

        partMgr.onUpdateReceived(0, 0);
        partMgr.onUpdateReceived(0, 1);
        partMgr.onUpdateReceived(0, 4);

        assert partMgr.getLastAppliedUpdate(0) == 1;
        assert partMgr.getLastReceivedUpdate(0) == 4;

        partMgr.onUpdateReceived(0, 2);

        assert partMgr.getLastAppliedUpdate(0) == 2;
        assert partMgr.getLastReceivedUpdate(0) == 4;

        partMgr.onUpdateReceived(0, 3);

        assert partMgr.getLastAppliedUpdate(0) == 4;
        assert partMgr.getLastReceivedUpdate(0) == 4;
    }

}
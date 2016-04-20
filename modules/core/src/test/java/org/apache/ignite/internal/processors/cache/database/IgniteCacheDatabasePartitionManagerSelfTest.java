package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import junit.framework.TestCase;

public class IgniteCacheDatabasePartitionManagerSelfTest extends TestCase {

    public void test() {
        IgniteCacheDatabasePartitionManager partMgr = new IgniteCacheDatabasePartitionManager(10, ByteBuffer.allocateDirect(1024));

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

    public void testPersistence() {
        ByteBuffer buf = ByteBuffer.allocateDirect(1024);

        IgniteCacheDatabasePartitionManager partMgr = new IgniteCacheDatabasePartitionManager(10, buf);

        partMgr.onUpdateReceived(0, 1);
        partMgr.onUpdateReceived(0, 4);

        partMgr.onUpdateReceived(1, 2);
        partMgr.onUpdateReceived(1, 3);

        assert partMgr.getLastAppliedUpdate(0) == 1;
        assert partMgr.getLastAppliedUpdate(1) == 0;

        buf.position(0);

        partMgr = new IgniteCacheDatabasePartitionManager(10, buf);

        assert partMgr.getLastAppliedUpdate(0) == 1;
        assert partMgr.getLastAppliedUpdate(1) == 0;

        partMgr.onUpdateReceived(0, 2);
        partMgr.onUpdateReceived(0, 3);

        assert partMgr.getLastAppliedUpdate(0) == 4;
        assert partMgr.getLastAppliedUpdate(1) == 0;

        partMgr.onUpdateReceived(1, 1);

        assert partMgr.getLastAppliedUpdate(0) == 4;
        assert partMgr.getLastAppliedUpdate(1) == 3;
    }

}
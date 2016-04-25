package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import junit.framework.TestCase;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;

public class IgniteCacheDatabasePartitionManagerSelfTest extends TestCase {

    public void test() {
        IgniteCacheDatabasePartitionManager partMgr = new IgniteCacheDatabasePartitionManager(10, new DummyPage(ByteBuffer.allocateDirect(1024)));

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
        Page page = new DummyPage(ByteBuffer.allocateDirect(1024));

        IgniteCacheDatabasePartitionManager partMgr = new IgniteCacheDatabasePartitionManager(10, page);

        partMgr.onUpdateReceived(0, 1);
        partMgr.onUpdateReceived(0, 4);

        partMgr.onUpdateReceived(1, 2);
        partMgr.onUpdateReceived(1, 3);

        assert partMgr.getLastAppliedUpdate(0) == 1;
        assert partMgr.getLastAppliedUpdate(1) == 0;

        partMgr.flushCounters();

        partMgr = new IgniteCacheDatabasePartitionManager(10, page);

        assert partMgr.getLastAppliedUpdate(0) == 1;
        assert partMgr.getLastAppliedUpdate(1) == 0;

        partMgr.onUpdateReceived(0, 2);
        partMgr.onUpdateReceived(0, 3);

        assert partMgr.getLastAppliedUpdate(0) == 3;
        assert partMgr.getLastAppliedUpdate(1) == 0;

        partMgr.onUpdateReceived(1, 1);

        assert partMgr.getLastAppliedUpdate(0) == 3;
        assert partMgr.getLastAppliedUpdate(1) == 1;
    }

    private static class DummyPage implements Page {

        private final ByteBuffer buf;

        private DummyPage(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override public long id() {
            return 0;
        }

        @Override public FullPageId fullId() {
            return new FullPageId(0, 0);
        }

        @Override public int version() {
            return 0;
        }

        @Override public int incrementVersion() {
            throw new UnsupportedOperationException();
        }

        @Override public ByteBuffer getForRead() {
            buf.position(0);
            return buf;
        }

        @Override public void releaseRead() {
            // noop
        }

        @Override public ByteBuffer getForWrite() {
            buf.position(0);
            return buf;
        }

        @Override public ByteBuffer getForInitialWrite() {
            buf.position(0);
            return buf;
        }

        @Override public void releaseWrite(boolean markDirty) {
            // noop
        }

        @Override public boolean isDirty() {
            return false;
        }

        @Override public void close() {
            // noop
        }
    }

}
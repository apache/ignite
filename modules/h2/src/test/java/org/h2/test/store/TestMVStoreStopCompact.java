/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Random;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Test that the MVStore eventually stops optimizing (does not excessively opti
 */
public class TestMVStoreStopCompact extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.big = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        if (!config.big) {
            return;
        }
        for(int retentionTime = 10; retentionTime < 1000; retentionTime *= 10) {
            for(int timeout = 100; timeout <= 1000; timeout *= 10) {
                testStopCompact(retentionTime, timeout);
            }
        }
    }

    private void testStopCompact(int retentionTime, int timeout) throws InterruptedException {
        String fileName = getBaseDir() + "/testStopCompact.h3";
        FileUtils.createDirectories(getBaseDir());
        FileUtils.delete(fileName);
        // store with a very small page size, to make sure
        // there are many leaf pages
        MVStore s = new MVStore.Builder().
                fileName(fileName).open();
        s.setRetentionTime(retentionTime);
        MVMap<Integer, String> map = s.openMap("data");
        long start = System.currentTimeMillis();
        Random r = new Random(1);
        for (int i = 0; i < 4000000; i++) {
            long time = System.currentTimeMillis() - start;
            if (time > timeout) {
                break;
            }
            int x = r.nextInt(10000000);
            map.put(x, "Hello World " + i * 10);
        }
        s.setAutoCommitDelay(100);
        long oldWriteCount = s.getFileStore().getWriteCount();
        // expect background write to stop after 5 seconds
        Thread.sleep(5000);
        long newWriteCount = s.getFileStore().getWriteCount();
        // expect that compaction didn't cause many writes
        assertTrue(newWriteCount - oldWriteCount < 30);
        s.close();
    }

}

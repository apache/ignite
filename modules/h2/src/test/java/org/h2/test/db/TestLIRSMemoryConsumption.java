/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import java.util.Random;

/**
 * Class TestLIRSMemoryConsumption.
 * <UL>
 * <LI> 8/5/18 10:57 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class TestLIRSMemoryConsumption extends TestDb {

    /**
     * Run just this test.
     *
     * @param a
     *              ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() {
        testMemoryConsumption();
        System.out.println("-----------------------");
        testMemoryConsumption();
        System.out.println("-----------------------");
        testMemoryConsumption();
    }

    private void testMemoryConsumption() {
        int size = 1_000_000;
        Random rng = new Random();
        CacheLongKeyLIRS.Config config = new CacheLongKeyLIRS.Config();
        for (int mb = 1; mb <= 16; mb *= 2) {
            config.maxMemory = mb * 1024 * 1024;
            CacheLongKeyLIRS<Object> cache = new CacheLongKeyLIRS<>(config);
            long memoryUsedInitial = getMemUsedKb();
            for (int i = 0; i < size; i++) {
                cache.put(i, createValue(i), getValueSize(i));
            }
            for (int i = 0; i < size; i++) {
                int key;
                int mode = rng.nextInt(4);
                switch(mode) {
                    default:
                    case 0:
                        key = rng.nextInt(10);
                        break;
                    case 1:
                        key = rng.nextInt(100);
                        break;
                    case 2:
                        key = rng.nextInt(10_000);
                        break;
                    case 3:
                        key = rng.nextInt(1_000_000);
                        break;
                }
                Object val = cache.get(key);
                if (val == null) {
                    cache.put(key, createValue(key), getValueSize(key));
                }
            }

            eatMemory(1);
            freeMemory();
            cache.trimNonResidentQueue();
            long memoryUsed = getMemUsedKb();

            int sizeHot = cache.sizeHot();
            int sizeResident = cache.size();
            int sizeNonResident = cache.sizeNonResident();
            long hits = cache.getHits();
            long misses = cache.getMisses();
            System.out.println(mb + " | " +
                    (memoryUsed - memoryUsedInitial + 512) / 1024 + " | " +
                    (sizeResident+sizeNonResident) + " | " +
                    sizeHot + " | " + (sizeResident - sizeHot) + " | " + sizeNonResident +
                    " | " + (hits * 100 / (hits + misses)) );
        }
    }

    private static Object createValue(long key) {
//        return new Object();
        return new byte[2540];
    }

    private static int getValueSize(long key) {
//        return 16;
        return 2560;
    }

    private static long getMemUsedKb() {
        Runtime rt = Runtime.getRuntime();
        long memory = Long.MAX_VALUE;
        for (int i = 0; i < 8; i++) {
            rt.gc();
            long memNow = (rt.totalMemory() - rt.freeMemory()) / 1024;
            if (memNow >= memory) {
                break;
            }
            memory = memNow;
            try { Thread.sleep(1000); } catch (InterruptedException e) {/**/}
        }
        return memory;
    }
}

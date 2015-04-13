/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.multijvm;

/**
 * Test cases for multi-jvm tests.
 */
public class CachePartitionedMultyJvmSelfTest extends MultiJvmTest{
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testBasicPut() throws Exception {
//        runIgniteProcess("ignite1", "cfg");
//
//        checkPuts(3, ignite1);
//    }
//
//    /**
//     * @throws Exception If test fails.
//     */
//    public void testMultiJvmPut() throws Exception {
//        checkPuts(1, ignite1, ignite2, ignite3);
//        checkPuts(1, ignite2, ignite1, ignite3);
//        checkPuts(1, ignite3, ignite1, ignite2);
//    }
//
//    /**
//     * Checks cache puts.
//     *
//     * @param cnt Count of puts.
//     * @param ignites Grids.
//     * @throws Exception If check fails.
//     */
//    private void checkPuts(int cnt, Ignite... ignites) throws Exception {
//        CountDownLatch latch = new CountDownLatch(ignites.length * cnt);
//
//        CacheEventListener lsnr = new CacheEventListener(latch, EVT_CACHE_OBJECT_PUT);
//
//        for (Ignite ignite : ignites)
//            addListener(ignite, lsnr);
//
//        IgniteCache<Integer, String> cache1 = ignites[0].cache(null);
//
//        for (int i = 1; i <= cnt; i++)
//            cache1.put(i, "val" + i);
//
//        for (int i = 1; i <= cnt; i++) {
//            String v = cache1.get(i);
//
//            assert v != null;
//            assert v.equals("val" + i);
//        }
//
//        latch.await(10, SECONDS);
//
//        for (Ignite ignite : ignites) {
//            IgniteCache<Integer, String> cache = ignite.cache(null);
//
//            if (cache == cache1)
//                continue;
//
//            for (int i = 1; i <= cnt; i++) {
//                String v = cache.get(i);
//
//                assert v != null;
//                assert v.equals("val" + i);
//            }
//        }
//
//        assert !cache1.isLocalLocked(1, false);
//        assert !cache1.isLocalLocked(2, false);
//        assert !cache1.isLocalLocked(3, false);
//
//        for (Ignite ignite : ignites)
//            ignite.events().stopLocalListen(lsnr);
//    }
//
}

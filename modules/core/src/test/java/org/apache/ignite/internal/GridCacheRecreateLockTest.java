package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Create lock after owner node left topology test
 */
@GridCommonTest(group = "Kernal Self")
public class GridCacheRecreateLockTest extends GridCommonAbstractTest {

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void test() throws Exception {
        final Ignite ignite = startNodeAndLock("node1");

        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    Thread.sleep(2000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                ignite.close();
            }
        }).start();

        startNodeAndLock("node2");
    }

    private Ignite startNodeAndLock(String name) {
        try {
            IgniteConfiguration cfg = new IgniteConfiguration();
            cfg.setGridName(name);

            Ignite ignite = Ignition.start(cfg);

            IgniteLock lock = ignite.reentrantLock("lock", true, true, true);

            System.out.println("acquiring lock");

            lock.lock();

            System.out.println("acquired lock");

            return ignite;
        }
        catch (Exception e) {
            assertTrue(false);
        }

        return null;
    }
}
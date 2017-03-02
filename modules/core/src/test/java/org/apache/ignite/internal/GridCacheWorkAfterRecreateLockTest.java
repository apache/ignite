package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import java.util.concurrent.CountDownLatch;

/**
 * Create lock after owner node left topology test and wwork with it from another nodes
 */
@GridCommonTest(group = "Kernal Self")
public class GridCacheWorkAfterRecreateLockTest extends GridCommonAbstractTest {

    CountDownLatch latch = new CountDownLatch(3);

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void test() throws Exception {

        final Ignite ignite = startNodeAndLock("node1");

        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    Thread.sleep(3000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                ignite.close();
            }
        }).start();

        Thread t1 = new Thread(new Runnable() {
            @Override public void run() {
                startNodeAndLock("node2");
            }
        });
        t1.start();
        Thread t2 = new Thread(new Runnable() {
            @Override public void run() {
                startNodeAndLock("node3");
            }
        });
        t2.start();

        Thread t3 = new Thread(new Runnable() {
            @Override public void run() {
                startNodeAndLock("node4");
            }
        });
        t3.start();

        latch.await();
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
            if (!"node1".equals(name)) {
                System.out.println("unlock lock");

                lock.unlock();

                latch.countDown();
            }
            return ignite;
        }
        catch (Exception e) {
            assertTrue(false);
        }

        return null;
    }

}
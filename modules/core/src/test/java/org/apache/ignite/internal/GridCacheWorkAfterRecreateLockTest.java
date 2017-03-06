/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import java.util.concurrent.CountDownLatch;

/**
 * Create lock after owner node left topology test and work with it from another nodes
 */
@GridCommonTest(group = "Kernal Self")
public class GridCacheWorkAfterRecreateLockTest extends GridCommonAbstractTest {

    CountDownLatch latch = new CountDownLatch(3);


    public void test() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("node1");
        final Ignite ignite = Ignition.start(cfg);

        cfg.setGridName("node2");
        final Ignite ignite2 = Ignition.start(cfg);

        cfg.setGridName("node3");
        final Ignite ignite3 = Ignition.start(cfg);

        cfg.setGridName("node4");
        final Ignite ignite4 = Ignition.start(cfg);

        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                ignite.close();
            }
        }).start();

        IgniteLock lock = ignite.reentrantLock("lock", true, true, true);

        new Thread(new Runnable() {
            @Override public void run() {
                lock(ignite2);
            }
        }).start();


        new Thread(new Runnable() {
            @Override public void run() {
                lock(ignite3);
            }
        }).start();

        new Thread(new Runnable() {
            @Override public void run() {
                lock(ignite4);
            }
        }).start();

        System.out.println(ignite.name() + " acquiring lock");

        lock.lock();

        System.out.println(ignite.name() + " acquired lock");
        latch.await();

    }

    private void lock(Ignite ignite) {
        try {

            IgniteLock lock = ignite.reentrantLock("lock", true, true, true);

            System.out.println(ignite.name() + " acquiring lock");

            lock.lock();

            System.out.println(ignite.name() + " acquired lock");

            Thread.sleep(500);

            lock.unlock();

            System.out.println(ignite.name() + " unlock lock");

            latch.countDown();
        }
        catch (Exception e) {
            latch.countDown();
            assertTrue(false);
        }
    }
}

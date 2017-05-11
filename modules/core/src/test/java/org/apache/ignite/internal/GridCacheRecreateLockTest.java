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
            cfg.setIgniteInstanceName(name);

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
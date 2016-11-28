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

package org.apache.ignite.internal.processors.cacheobject;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Ignite Data Streamer test to validate Data Streamer
 * throws exception if the cache is destroyed during
 * streaming of data.
 */
public class IgniteCacheObjectProcessorSelfTest extends GridCommonAbstractTest {

    /**
     * @throws Exception If failed.
     */
    public void testAddData() throws Exception{
        Ignite ignite = Ignition.start(new IgniteConfiguration().setGridName("server"));

        Ignite igniteClient = Ignition.start(new IgniteConfiguration().setGridName("client-1").setClientMode(true));

        final IgniteCache cache = ignite.createCache("test");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();

                    return;
                }

                X.println("Stopping cache: test");

                cache.destroy();
            }
        }).start();

        IgniteDataStreamer<Integer, Integer> s = igniteClient.dataStreamer("test");

        try {
            while (true) {
                s.addData(1, 1);
                s.flush();
                Thread.sleep(1000);
            }
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Failed to get partition: test"));
        }
    }
}
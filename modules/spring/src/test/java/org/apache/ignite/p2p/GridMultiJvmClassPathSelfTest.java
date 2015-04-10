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

package org.apache.ignite.p2p;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.config.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

/**
 * Test.
 */
public class GridMultiJvmClassPathSelfTest extends GridCommonAbstractTest {
    private static String START = "Start external node";

    public static void main(String[] args) throws Exception {
        U.setWorkDirectory(null, U.getIgniteHome());

        // Tell our process PID to the wrapper.
        X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

        X.println("Java path: " + System.getProperty("java.class.path"));

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            X.println(START);
            U.sleep(10000);
        }
    }
    /**
     * @throws Exception If failed.
     */
    public void testDifferentClassPath() throws Exception {
        final CountDownLatch killedLatch = new CountDownLatch(2);

        final CountDownLatch readyLatch = new CountDownLatch(2);

        GridJavaProcess proc1 = GridJavaProcess.exec(
            GridMultiJvmClassPathSelfTest.class, null,
            log,
            new CI1<String>() {
                @Override public void apply(String s) {
                    info("Process 1 prints: " + s);

                    if (s.startsWith(START))
                        readyLatch.countDown();
                }
            },
            new CA() {
                @Override public void apply() {
                    info("Process 1 is killed");

                    killedLatch.countDown();
                }
            },
            null,
            null
        );

        GridJavaProcess proc2 = GridJavaProcess.exec(
            GridMultiJvmClassPathSelfTest.class, null,
            log,
            new CI1<String>() {
                @Override public void apply(String s) {
                    info("Process 2 prints: " + s);

                    if (s.contains(START))
                        readyLatch.countDown();
                }
            },
            new CA() {
                @Override public void apply() {
                    info("Process 2 is killed");

                    killedLatch.countDown();
                }
            },
            null,
            GridTestProperties.getProperty("p2p.uri.cls")
        );

        readyLatch.await();

        try(Ignite ignite = Ignition.start("examples/config/example-ignite.xml")){
            proc1.kill();
            proc2.kill();

            killedLatch.await();
        }
    }
}

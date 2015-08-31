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

package org.apache.ignite.thread;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test for {@link org.apache.ignite.thread.IgniteThread}.
 */
@GridCommonTest(group = "Utils")
public class GridThreadTest extends GridCommonAbstractTest {
    /** Thread count. */
    private static final int THREAD_CNT = 3;

    /**
     * @throws Exception If failed.
     */
    public void testAssertion() throws Exception {
        Collection<IgniteThread> ts = new ArrayList<>();

        for (int i = 0; i < THREAD_CNT; i++) {
            ts.add(new IgniteThread("test-grid-" + i, "test-thread", new Runnable() {
                @Override public void run() {
                    assert false : "Expected assertion.";
                }
            }));
        }

        for (IgniteThread t : ts)
            t.start();

        for (IgniteThread t : ts)
            t.join();
    }
}
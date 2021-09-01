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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteThreadGroupNodeRestartTest extends GridCommonAbstractTest {
    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNodeRestartInsideThreadGroup() throws Exception {
        ThreadGroup tg = new ThreadGroup("test group");

        AtomicReference<Exception> err = new AtomicReference<>();

        Thread t = new Thread(tg, () -> {
            try {
                startGrid(0);

                stopGrid(0);
            }
            catch (Exception e) {
                err.set(e);
            }
        });

        t.start();
        t.join(getTestTimeout());

        if (err.get() != null)
            throw err.get();

        tg.destroy();

        startGrid(0);
        stopGrid(0);
    }
}

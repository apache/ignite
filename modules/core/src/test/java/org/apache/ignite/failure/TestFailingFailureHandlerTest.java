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

package org.apache.ignite.failure;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test failing failure handler test.
 */
public class TestFailingFailureHandlerTest extends OomFailureHandlerTest {
    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new TestFailingFailureHandler(this);
    }

    /**
     * @param igniteWork Working ignite instance.
     * @param igniteFail Failed ignite instance.
     */
    @Override protected void assertFailureState(IgniteEx igniteWork, IgniteEx igniteFail)
        throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(() -> ex.get() != null, 1_000));

        log.info("Expected critical failure [ex=" + ex.get().getClass().getSimpleName() +
            ", msg="+ex.get().getMessage() + ']');

        assertFalse(igniteWork.context().isStopping());
        assertTrue(igniteFail.context().isStopping());

        ex.set(null);
    }
}

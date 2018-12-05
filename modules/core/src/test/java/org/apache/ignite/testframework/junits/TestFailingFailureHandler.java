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

package org.apache.ignite.testframework.junits;

import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.util.typedef.internal.S;


/**
 * Stops node and fails test.
 */
public class TestFailingFailureHandler extends StopNodeFailureHandler {
    /** {@inheritDoc} */
    @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
        if (!GridAbstractTest.testIsRunning) {
            ignite.log().info("Critical issue detected after test finished. Test failure handler ignore it.");

            return true;
        }

        boolean nodeStopped = super.handle(ignite, failureCtx);

        TestCase.fail(failureCtx.toString());

        return nodeStopped;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TestFailingFailureHandler.class, this);
    }
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.failure;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;

/**
 * Test failure handler implementation
 */
public class TestFailureHandler extends AbstractFailureHandler {
    /** Invalidate. */
    private final boolean invalidate;

    /** Latch. */
    private final CountDownLatch latch;

    /** Failure context. */
    volatile FailureContext failureCtx;

    /**
     * @param invalidate Invalidate.
     */
    public TestFailureHandler(boolean invalidate) {
        this(invalidate, new CountDownLatch(1));
    }

    /**
     * @param invalidate Invalidate.
     * @param latch Latch.
     */
    public TestFailureHandler(boolean invalidate, CountDownLatch latch) {
        this.invalidate = invalidate;
        this.latch = latch;
    }

    /** {@inheritDoc} */
    @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
        if (this.failureCtx == null) {
            this.failureCtx = failureCtx;

            if (latch != null)
                latch.countDown();

            ignite.log().warning("Handled ignite failure: " + failureCtx);
        }

        return invalidate;
    }

    /**
     * @return Failure context.
     */
    public FailureContext failureContext() {
        return failureCtx;
    }

    /**
     * @param millis Millis.

     * @return Failure context.
     */
    public FailureContext awaitFailure(long millis) throws InterruptedException {
        latch.await(millis, TimeUnit.MILLISECONDS);

        return failureCtx;
    }
}

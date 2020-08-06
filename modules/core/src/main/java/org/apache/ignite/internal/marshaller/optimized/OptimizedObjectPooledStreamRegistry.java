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

package org.apache.ignite.internal.marshaller.optimized;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class OptimizedObjectPooledStreamRegistry extends OptimizedObjectStreamRegistry {
    /** Output streams pool. */
    private final BlockingQueue<OptimizedObjectOutputStream> outPool;

    /** Input streams pool. */
    private final BlockingQueue<OptimizedObjectInputStream> inPool;

    /**
     * @param size Pool size.
     */
    OptimizedObjectPooledStreamRegistry(int size) {
        assert size > 0 : "size must be positive for pooled stream registry: " + size;

        outPool = new LinkedBlockingQueue<>(size);
        inPool = new LinkedBlockingQueue<>(size);

        for (int i = 0; i < size; i++) {
            outPool.offer(createOut());
            inPool.offer(createIn());
        }
    }

    /** {@inheritDoc} */
    @Override OptimizedObjectOutputStream out() throws IgniteInterruptedCheckedException {
        try {
            return outPool.take();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(
                "Failed to take output object stream from pool (thread interrupted).", e);
        }
    }

    /** {@inheritDoc} */
    @Override OptimizedObjectInputStream in() throws IgniteInterruptedCheckedException {
        try {
            return inPool.take();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(
                "Failed to take input object stream from pool (thread interrupted).", e);
        }
    }

    /** {@inheritDoc} */
    @Override void closeOut(OptimizedObjectOutputStream out) {
        U.close(out, null);

        boolean b = outPool.offer(out);

        assert b;
    }

    /** {@inheritDoc} */
    @Override void closeIn(OptimizedObjectInputStream in) {
        U.close(in, null);

        boolean b = inPool.offer(in);

        assert b;
    }
}

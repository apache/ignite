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

package org.apache.ignite.internal.processors.localtask;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;

/**
 * Observer is a checkpoint listener.
 * Consumers will be reset after a single use.
 */
public class ObservingCheckpointListener implements CheckpointListener {
    /** On mark checkpoint begin consumer. */
    volatile IgniteThrowableConsumer<Context> onMarkCheckpointBeginConsumer;

    /** Repeat {@link #onMarkCheckpointBeginConsumer}. */
    volatile boolean repeatOnMarkCheckpointBeginConsumer;

    /** After checkpoint end consumer. */
    volatile IgniteThrowableConsumer<Context> afterCheckpointEndConsumer;

    /** Repeat {@link #afterCheckpointEndConsumer}. */
    volatile boolean repeatAfterCheckpointEndConsumer;

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
        IgniteThrowableConsumer<Context> consumer = onMarkCheckpointBeginConsumer;

        if (consumer != null) {
            consumer.accept(ctx);

            if (!repeatOnMarkCheckpointBeginConsumer)
                onMarkCheckpointBeginConsumer = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void afterCheckpointEnd(Context ctx) throws IgniteCheckedException {
        IgniteThrowableConsumer<Context> consumer = afterCheckpointEndConsumer;

        if (consumer != null) {
            consumer.accept(ctx);

            if (!repeatAfterCheckpointEndConsumer)
                afterCheckpointEndConsumer = null;
        }
    }

    /**
     * Asynchronous execution {@link #onMarkCheckpointBeginConsumer}.
     *
     * @param consumer On mark checkpoint begin consumer.
     * @return Future that complete when the consumer complete.
     */
    GridFutureAdapter<Void> onMarkCheckpointBeginAsync(IgniteThrowableConsumer<Context> consumer) {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        onMarkCheckpointBeginConsumer = asyncConsumer(consumer, fut);

        return fut;
    }

    /**
     * Asynchronous execution {@link #afterCheckpointEndConsumer}.
     *
     * @param consumer After checkpoint end consumer.
     * @return Future that complete when the consumer complete.
     */
    GridFutureAdapter<Void> afterCheckpointEndAsync(IgniteThrowableConsumer<Context> consumer) {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        afterCheckpointEndConsumer = asyncConsumer(consumer, fut);

        return fut;
    }

    /**
     * Make the consumer asynchronous.
     *
     * @param consumer Consumer.
     * @param fut Future that complete when the consumer complete.
     * @return New consumer.
     */
    private static IgniteThrowableConsumer<Context> asyncConsumer(
        IgniteThrowableConsumer<Context> consumer,
        GridFutureAdapter<Void> fut
    ) {
        return ctx -> {
            Throwable err = null;

            try {
                consumer.accept(ctx);
            }
            catch (Throwable t) {
                err = t;
            }

            fut.onDone(err);
        };
    }
}

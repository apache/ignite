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

package org.apache.ignite.agent.ws;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.agent.ManagementConsoleProcessor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.util.lang.GridFunc.isEmpty;

/**
 * Retryable sender with limited queue.
 */
public class RetryableSender extends GridProcessorAdapter implements Runnable {
    /** Queue capacity. */
    private static final int DEFAULT_QUEUE_CAP = 500;

    /** Max sleep time seconds between retries. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Batch size. */
    private static final int BATCH_SIZE = 10;

    /** Queue. */
    private final BlockingQueue<IgniteBiTuple<String, List<Object>>> queue;

    /** Executor service. */
    private final ExecutorService exSrvc;

    /** Retry count. */
    private int retryCnt;

    /**
     * @param ctx Context.
     */
    public RetryableSender(GridKernalContext ctx) {
        super(ctx);

        this.queue = new ArrayBlockingQueue<>(DEFAULT_QUEUE_CAP);
        this.exSrvc = Executors.newSingleThreadExecutor(new CustomizableThreadFactory("mgmt-console-sender-"));

        exSrvc.submit(this);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        while (true) {
            IgniteBiTuple<String, List<Object>> e = null;

            try {
                e = queue.take();

                if (!sendInternal(e.getKey(), e.getValue()))
                    addToQueue(e);

            }
            catch (Exception ex) {
                if (X.hasCause(ex, InterruptedException.class)) {
                    Thread.currentThread().interrupt();

                    break;
                }

                addToQueue(e);
            }
        }
    }

    /**
     * @param elements Elements.
     */
    boolean sendInternal(String dest, List<Object> elements) throws InterruptedException {
        Thread.sleep(Math.min(MAX_SLEEP_TIME_SECONDS, retryCnt) * 1000);

        WebSocketManager mgr = ((ManagementConsoleProcessor)ctx.managementConsole()).webSocketManager();

        if (mgr != null && !mgr.send(dest, elements)) {
            retryCnt++;

            if (retryCnt == 1)
                log.warning("Failed to send message to Management Console, will retry in " + retryCnt * 1000 + " ms");

            return false;
        }

        retryCnt = 0;

        return true;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        U.shutdownNow(getClass(), exSrvc, log);
    }

    /**
     * @param element Element to send.
     */
    public void send(String dest, Object element) {
        if (element != null)
            addToQueue(new IgniteBiTuple<>(dest, singletonList(element)));
    }

    /**
     * @param elements Elements to send.
     */
    public void sendList(String dest, List<?> elements) {
        if (!isEmpty(elements))
            splitOnBatches(elements).stream().map(e -> new IgniteBiTuple<>(dest, e)).forEach(this::addToQueue);
    }

    /**
     * @param list List.
     */
    private List<List<Object>> splitOnBatches(List<?> list) {
        List<Object> batch = new ArrayList<>();

        List<List<Object>> res = new ArrayList<>();

        for (Object e : list) {
            batch.add(e);

            if (batch.size() >= BATCH_SIZE) {
                res.add(batch);
                batch = new ArrayList<>();
            }
        }

        if (!batch.isEmpty())
            res.add(batch);

        return res;
    }

    /**
     * @param msg Message.
     */
    private void addToQueue(IgniteBiTuple<String, List<Object>> msg) {
        while (!queue.offer(msg))
            queue.poll();
    }
}

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.logger.NullLogger;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.with;

/**
 * Retryable sender test.
 */
public class RetryableSenderTest {
    /**
     * Should split list of elements into batches and send.
     */
    @Test
    public void shouldSendInBatches() throws Exception {
        List<List<Integer>> results = new ArrayList<>();

        RetryableSender<Integer> snd = new RetryableSender<Integer>(new StandaloneGridKernalContext(new NullLogger(), null, null)) {
            @Override boolean sendInternal(String dest, List<Integer> elements) {
                results.add(elements);

                return true;
            }
        };

        snd.send("dest", IntStream.range(0, 17).boxed().collect(Collectors.toList()));

        with().pollInterval(100, MILLISECONDS).await().atMost(1, SECONDS)
            .until(() -> !results.isEmpty() && results.get(0).size() == 10);

        with().pollInterval(100, MILLISECONDS).await().atMost(1, SECONDS)
            .until(() -> !results.isEmpty() && results.get(1).size() == 7);
    }

    /**
     * Should retry send elements if we can't send.
     */
    @Test
    public void shouldRetrySend() throws Exception {
        List<List<Integer>> results = new ArrayList<>();

        AtomicBoolean shouldSnd = new AtomicBoolean(false);

        AtomicInteger retryCnt = new AtomicInteger();

        RetryableSender<Integer> snd = new RetryableSender<Integer>(new StandaloneGridKernalContext(new NullLogger(), null, null)) {
            @Override boolean sendInternal(String dest, List<Integer> elements) {
                if (!shouldSnd.get()) {
                    retryCnt.incrementAndGet();

                    return false;
                }

                results.add(elements);

                return true;
            }
        };

        snd.send("dest", IntStream.range(0, 17).boxed().collect(Collectors.toList()));

        with().pollInterval(500, MILLISECONDS).await().atMost(10, SECONDS).until(() -> retryCnt.get() >= 2);

        shouldSnd.set(true);

        with().pollInterval(100, MILLISECONDS).await().atMost(10, SECONDS).until(() -> results.size() == 2);
    }
}

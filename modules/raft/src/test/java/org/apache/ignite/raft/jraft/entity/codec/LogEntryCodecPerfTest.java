/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.entity.codec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.codec.v1.V1Decoder;
import org.apache.ignite.raft.jraft.entity.codec.v1.V1Encoder;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class LogEntryCodecPerfTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(LogEntryCodecPerfTest.class);

    static byte[] DATA = new byte[512];

    static {
        ThreadLocalRandom.current().nextBytes(DATA);
    }

    static final int TIMES = 100000;

    static final int THREADS = 20;

    private final AtomicLong logSize = new AtomicLong(0);

    @BeforeEach
    public void setup() throws Exception {
        this.logSize.set(0);
        System.gc();
    }

    private void testEncodeDecode(final LogEntryEncoder encoder, final LogEntryDecoder decoder,
        final CyclicBarrier barrier) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(DATA);
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setData(buf);
        entry.setPeers(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));

        if (barrier != null) {
            barrier.await();
        }

        for (int i = 0; i < TIMES; i++) {
            entry.setId(new LogId(i, i));
            byte[] content = encoder.encode(entry);
            assert (content.length > 0);
            this.logSize.addAndGet(content.length);
            LogEntry nLog = decoder.decode(content);
            assertEquals(2, nLog.getPeers().size());
            assertArrayEquals(DATA, nLog.getData().array());
            assertEquals(i, nLog.getId().getIndex());
            assertEquals(i, nLog.getId().getTerm());
        }

        if (barrier != null) {
            barrier.await();
        }

    }

    @Test
    public void testV1Codec() throws Exception {
        LogEntryEncoder encoder = V1Encoder.INSTANCE;
        LogEntryDecoder decoder = V1Decoder.INSTANCE;
        testEncodeDecode(encoder, decoder, null);
        concurrentTest("V1", encoder, decoder);
    }

    private void concurrentTest(final String version, final LogEntryEncoder encoder, final LogEntryDecoder decoder)
        throws InterruptedException,
        BrokenBarrierException {
        final CyclicBarrier barrier = new CyclicBarrier(THREADS + 1);
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        for (int i = 0; i < THREADS; i++) {
            executor.execute(() -> {
                try {
                    testEncodeDecode(encoder, decoder, barrier);
                }
                catch (Exception e) {
                    LOG.error("Failed to run test", e); // NOPMD
                    fail();
                }
            });
        }
        long start = Utils.monotonicMs();
        barrier.await();
        barrier.await();
        LOG.info(version + " codec cost:" + (Utils.monotonicMs() - start) + " ms.");
        LOG.info("Total log size:" + this.logSize.get() + " bytes.");
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }
}

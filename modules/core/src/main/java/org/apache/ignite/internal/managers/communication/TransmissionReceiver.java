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

package org.apache.ignite.internal.managers.communication;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;

import static org.apache.ignite.internal.managers.communication.GridIoManager.DFLT_ACQUIRE_TIMEOUT_MS;

/**
 * Class represents a receiver of data which can be pulled from a channel by chunks of
 * predefined size. Closes when a transmission of represented object ends.
 */
abstract class TransmissionReceiver extends AbstractTransmission {
    /**
     * The total amount of permits for the 1 second period of time per the node instance for download.
     * To limit the download speed of reading the stream of data we will acuire a permit per byte.
     * <p>
     * For instance, for the 128 Kb/sec rate you should specify total <tt>131_072</tt> permits.
     */
    private final TimedSemaphore inBytePermits;

    /**
     * @param meta Initial file meta info.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param log Ignite logger.
     * @param chunkSize Size of chunks.
     */
    protected TransmissionReceiver(
        TransmissionMeta meta,
        BooleanSupplier stopChecker,
        IgniteLogger log,
        int chunkSize,
        TimedSemaphore inBytePermits
    ) {
        super(meta, stopChecker, log, chunkSize);

        this.inBytePermits = inBytePermits;
    }

    /**
     * @param ch Input channel to read data from.
     * @throws IOException If an io exception occurred.
     */
    public void receive(ReadableByteChannel ch) throws IOException, InterruptedException {
        // Read data from the input.
        while (hasNextChunk()) {
            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException("Recevier has been interrupted");

            if (stopped())
                throw new IgniteException("Receiver has been cancelled. Channel processing has been stopped.");


            // If the limit of permits at appropriate period of time reached,
            // the furhter invocations of the #acuqire(int) method will be blocked.
            boolean acquired = inBytePermits.tryAcquire(chunkSize,
                DFLT_ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (!acquired) {
                throw new IgniteException("Download speed is too slow " +
                    "[downloadSpeed=" + inBytePermits.permitsPerSec() + " byte/sec]");
            }

            readChunk(ch);
        }

        assert transferred == meta.count() : "The number of transferred bytes are not as expected " +
            "[expect=" + meta.count() + ", actual=" + transferred + ']';
    }

    /**
     * @param ch Channel to read data from.
     * @throws IOException If fails.
     */
    protected abstract void readChunk(ReadableByteChannel ch) throws IOException;
}

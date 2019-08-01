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
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;

import static org.apache.ignite.internal.util.IgniteUtils.assertParameter;

/**
 * Class represents a receiver of data which can be pulled from a channel by chunks of
 * predefined size. Closes when a transmission of represented object ends.
 */
abstract class AbstractReceiver extends AbstractTransmission {
    /**
     * @param initMeta Initial file meta info.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param log Ignite logger.
     * @param chunkSize Size of chunks.
     */
    protected AbstractReceiver(
        TransmissionMeta initMeta,
        BooleanSupplier stopChecker,
        IgniteLogger log,
        int chunkSize
    ) {
        super(initMeta, stopChecker, log, chunkSize);
    }

    /**
     * @param ch Input channel to read data from.
     * @param meta Meta information about receiving file.
     * @throws IOException If an io exception occurred.
     * @throws IgniteCheckedException If some check failed.
     */
    public void receive(
        ReadableByteChannel ch,
        TransmissionMeta meta
    ) throws IOException, IgniteCheckedException {
        assert meta != null;

        assertParameter(initMeta.name().equals(meta.name()), "Attempt to load different file " +
            "[initMeta=" + initMeta + ", meta=" + meta + ']');

        assertParameter(initMeta.offset() + transferred == meta.offset(),
            "The next chunk offest is incorrect [initMeta=" + initMeta +
                ", transferred=" + transferred + ", meta=" + meta + ']');

        assertParameter(initMeta.count() == meta.count() + transferred, " The count of bytes to transfer for " +
            "the next chunk is incorrect [total=" + initMeta.count() + ", transferred=" + transferred +
            ", initMeta=" + initMeta + ", meta=" + meta + ']');

        init(meta);

        // Read data from the input.
        while (hasNextChunk()) {
            if (Thread.currentThread().isInterrupted() || stopped()) {
                throw new IgniteCheckedException("Thread has been interrupted or operation has been cancelled " +
                    "due to node is stopping. Channel processing has been stopped.");
            }

            readChunk(ch);
        }

        assert transferred == initMeta.count() : "The number of transferred bytes are not as expected " +
            "[expect=" + initMeta.count() + ", actual=" + transferred + ']';
    }

    /**
     * @return Current receiver state written to a {@link TransmissionMeta} instance.
     */
    public TransmissionMeta state() {
        return new TransmissionMeta(initMeta.name(),
            initMeta.offset() + transferred,
            initMeta.count(),
            initMeta.params(),
            policy(),
            null);
    }

    /**
     * @return Read policy of data handling.
     */
    protected TransmissionPolicy policy() {
        return initMeta.policy();
    }

    /**
     * @param meta Meta information about receiving file.
     * @throws IgniteCheckedException If fails.
     */
    protected abstract void init(TransmissionMeta meta) throws IgniteCheckedException;

    /**
     * @param ch Channel to read data from.
     * @throws IOException If fails.
     * @throws IgniteCheckedException If fails.
     */
    protected abstract void readChunk(ReadableByteChannel ch) throws IOException, IgniteCheckedException;
}

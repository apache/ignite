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

package org.apache.ignite.internal.binary.compress;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.Compressor;

/** */
public abstract class CompressorAdapter implements Compressor {
    private final long PRINT_PER = 65536;

    /** */
    private final boolean CHECK = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_COMPRESSION_CHECK);

    protected static final int MIN_DELTA_BYTES = 8;

    protected final AtomicLong totalSamples = new AtomicLong();
    protected final AtomicLong acceptedSamples = new AtomicLong();

    protected final AtomicLong totalUncompressedSz = new AtomicLong();
    protected final AtomicLong totalCompressedSz = new AtomicLong();

    protected byte[] appraise(byte[] input, byte[] compressed) {
        if (compressed != null && CHECK) {
            byte[] decompressed = decompress(compressed);

            if (!Arrays.equals(input, decompressed)) {
                throw new IgniteException("Recompressed did not match: [input=" +
                    Arrays.toString(input) + ", afterDecompress=" + Arrays.toString(decompressed) + "]");
            }
        }

        byte[] message = compressed == null ? input : compressed;

        totalUncompressedSz.addAndGet(input.length);
        totalCompressedSz.addAndGet(message.length);

        if (totalSamples.incrementAndGet() % PRINT_PER == 0L)
            System.out.println("Ratio: " + (float)totalCompressedSz.get() / (float)totalUncompressedSz.get() +
                ", acceptance: " + (acceptedSamples.get() * 100L) / (totalSamples.get()) + "%");

        if ((input.length - message.length) > MIN_DELTA_BYTES) {
            acceptedSamples.incrementAndGet();
            return compressed;
        }

        return null;
    }
}

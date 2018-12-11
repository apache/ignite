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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.configuration.CompressionConfiguration;

/** */
public class ZstdDictionaryCompressor extends CompressorAdapter {

    private final AtomicInteger samplesCollected = new AtomicInteger();
    private final AtomicLong bufferCollected = new AtomicLong();
    private final Queue<byte[]> samples = new LinkedBlockingQueue<>();

    private volatile ZstdDictCompress compressor;
    private volatile ZstdDictDecompress decompressor;

    private final Lock dictLock = new ReentrantLock();

    private volatile int level;
    private volatile int dictSize;

    @Override public void configure(CompressionConfiguration compressionCfg) {
        dictLock.lock();

        level = compressionCfg.getCompressionLevel();
        dictSize = compressionCfg.getDictionarySize();

        samplesCollected.set(compressionCfg.getDictionaryTrainSamples());
        bufferCollected.set(compressionCfg.getDictionaryTrainBufferLength());

        samples.clear();

        dictLock.unlock();
    }

    public byte[] tryCompress(byte[] input) {
        ZstdDictCompress compressor0 = this.compressor;

        if (compressor0 != null) {
            byte[] compressed = Zstd.compress(input, compressor0);

            return appraise(input, compressed);
        }

        int iv = samplesCollected.decrementAndGet();
        if (iv > 0) {
            dictionarize(input);
        }
        else if (iv == 0) {
            dictLock.lock();
            int totalLen = 0;
            for (byte[] sample : samples)
                totalLen += sample.length;

            ZstdDictTrainer trainer = new ZstdDictTrainer(totalLen, dictSize);

            for (byte[] sample : samples)
                trainer.addSample(sample);

            byte[] dictionary = trainer.trainSamples();

            this.decompressor = new ZstdDictDecompress(dictionary);
            this.compressor = new ZstdDictCompress(dictionary, level);

            dictLock.unlock();
        }

        return null;
    }

    public byte[] decompress(byte[] bytes) {
        return Zstd.decompress(bytes, decompressor, /* XXX */bytes.length * 128);
    }

    /** Compress a string to a list of output symbols. */
    private void dictionarize(byte[] uncompressed) {
        if (uncompressed.length == 0 || !dictLock.tryLock()
            || bufferCollected.addAndGet(-uncompressed.length) <= 0 )
            return;

        samples.add(Arrays.copyOf(uncompressed, uncompressed.length));

        dictLock.unlock();
    }
}

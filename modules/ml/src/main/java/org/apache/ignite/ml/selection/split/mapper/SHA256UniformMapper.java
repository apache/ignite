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

package org.apache.ignite.ml.selection.split.mapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Implementation of uniform mappers based on SHA-256 hashing algorithm.
 *
 * @param <K> Type of a key.
 * @param <V> Type of a value.
 */
public class SHA256UniformMapper<K, V> implements UniformMapper<K,V> {
    /** */
    private static final long serialVersionUID = -8179630783617088803L;

    /** Hashing algorithm. */
    private static final String HASHING_ALGORITHM = "SHA-256";

    /** Message digest. */
    private static final ThreadLocal<MessageDigest> digest = new ThreadLocal<>();

    /** Strategy that defines how bytes will be swapped after SHA-256. */
    private final List<Integer> shuffleStgy = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7);

    /**
     * Constructs a new instance of SHA-256 uniform mapper.
     */
    public SHA256UniformMapper() {}

    /**
     * Constructs a new instance of SHA-256 uniform mapper.
     *
     * @param random Random used to define shuffle strategy.
     */
    public SHA256UniformMapper(Random random) {
        Collections.shuffle(shuffleStgy, random);
    }

    /** {@inheritDoc} */
    @Override public double map(K key, V val) {
        int h = Math.abs(key.hashCode());
        String str = String.valueOf(key.hashCode());

        byte[] hash = getDigest().digest(str.getBytes(StandardCharsets.UTF_8));

        byte hashByte = hash[h % hash.length];

        byte resByte = 0;

        for (int i = 0; i < 8; i++)
            resByte = (byte)(resByte << 1 | ((hashByte >> shuffleStgy.get(i)) & 0x1));

        return  1.0 * (resByte & 0xFF) / 256;
    }

    /**
     * Creates instance of digest in case it doesn't exist, otherwise returns existing instance.
     *
     * @return Instance of message digest.
     */
    private MessageDigest getDigest() {
        if (digest.get() == null) {
            try {
                digest.set(MessageDigest.getInstance(HASHING_ALGORITHM));
            }
            catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        return digest.get();
    }
}

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

package org.apache.ignite.internal.binary;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

/**
 * Charsets enumerated to be used in binary marshalling for string data.
 */
public enum BinaryStringEncoding {
    /** */
    ENC_US_ASCII(StandardCharsets.US_ASCII),

    /** */
    ENC_ISO_8859_1(StandardCharsets.ISO_8859_1),

    /** */
    ENC_UTF_8(StandardCharsets.UTF_8),

    /** */
    ENC_UTF_16BE(StandardCharsets.UTF_16BE),

    /** */
    ENC_UTF_16LE(StandardCharsets.UTF_16LE),

    /** */
    ENC_UTF_16(StandardCharsets.UTF_16),

    /** */
    ENC_KOI8_R(Charset.forName("KOI8-R")),

    /** */
    ENC_IBM866(Charset.forName("IBM866")),

    /** */
    ENC_WINDOWS_1251(Charset.forName("windows-1251"));

    // TODO: IGNITE-5655, add more; http://docs.oracle.com/javase/7/docs/technotes/guides/intl/encoding.doc.html

    /** Name-to-instance mapping. */
    private static final Map<String, BinaryStringEncoding> nameToInstance = new HashMap<>();

    /** Byte-to-instance mapping. */
    private static final Map<Byte, BinaryStringEncoding> byteToInstance = new HashMap<>();

    /** */
    static {
        for (BinaryStringEncoding enc : values()) {
            byteToInstance.put((byte)enc.ordinal(), enc);
            nameToInstance.put(enc.charset.name(), enc);
        }
    }

    /** */
    private final Charset charset;

    /**
     * Looks for {@link BinaryStringEncoding} instance by binary code.
     *
     * @param code binary encoding code.
     * @return {@link BinaryStringEncoding} instance or null if not found.
     */
    public static BinaryStringEncoding lookup(byte code) {
        return byteToInstance.get(code);
    }

    /**
     * Looks for {@link BinaryStringEncoding} instance by encoding name.
     *
     * @param name Encoding name.
     * @return {@link BinaryStringEncoding} instance or null if not found.
     */
    public static BinaryStringEncoding lookup(String name) {
        return nameToInstance.get(name);
    }

    /**
     * Constructs encoding for charset given.
     *
     * @param charset charset.
     */
    BinaryStringEncoding(@NotNull Charset charset) {
        this.charset = charset;
    }

    /**
     * Encoding id.
     *
     * @return Encoding id.
     */
    public byte id() {
        return (byte)ordinal();
    }

    /**
     * Corresponding charset.
     *
     * @return charset.
     */
    @NotNull public Charset charset() {
        return charset;
    }
}

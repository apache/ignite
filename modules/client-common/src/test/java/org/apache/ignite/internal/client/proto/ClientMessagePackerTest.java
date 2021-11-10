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

package org.apache.ignite.internal.client.proto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

/**
 * Tests Ignite ByteBuf-based packer against third-party library implementation to ensure identical results.
 */
public class ClientMessagePackerTest {
    private static void testPacker(Consumer<ClientMessagePacker> pack1, MessagePackerConsumer pack2) {
        var bytesIgnite = packIgnite(pack1);
        var bytesLibrary = packLibrary(pack2);
        
        assertArrayEquals(bytesLibrary, bytesIgnite);
    }
    
    private static byte[] packIgnite(Consumer<ClientMessagePacker> pack) {
        try (var packer = new ClientMessagePacker(Unpooled.buffer())) {
            pack.accept(packer);
            
            ByteBuf buf = packer.getBuffer();
            
            byte[] arr = buf.array();
            var offset = buf.arrayOffset() + ClientMessageCommon.HEADER_SIZE;
            var size = buf.writerIndex() - ClientMessageCommon.HEADER_SIZE;
            
            return Arrays.copyOfRange(arr, offset, offset + size);
        }
    }
    
    private static byte[] packLibrary(MessagePackerConsumer pack) {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            pack.accept(packer);
            
            return packer.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    
    @Test
    public void testPackNil() {
        testPacker(ClientMessagePacker::packNil, MessagePacker::packNil);
    }
    
    @ParameterizedTest
    @ValueSource(bytes = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE})
    public void testPackByte(byte b) {
        testPacker(p -> p.packByte(b), p -> p.packByte(b));
    }
    
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPackBoolean(boolean b) {
        testPacker(p -> p.packBoolean(b), p -> p.packBoolean(b));
    }
    
    @ParameterizedTest
    @ValueSource(shorts = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE})
    public void testPackShort(short s) {
        testPacker(p -> p.packShort(s), p -> p.packShort(s));
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE})
    public void testPackInt(int i) {
        testPacker(p -> p.packInt(i), p -> p.packInt(i));
    }
    
    @ParameterizedTest
    @ValueSource(longs = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE})
    public void testPackLong(long l) {
        testPacker(p -> p.packLong(l), p -> p.packLong(l));
    }
    
    @ParameterizedTest
    @ValueSource(longs = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE})
    public void testPackBigInteger(long l) {
        var bi = BigInteger.valueOf(l);
        testPacker(p -> p.packBigInteger(bi), p -> p.packBigInteger(bi));
    }
    
    @Test
    public void testPackBigIntegerThrowsOnTooLargeValues() {
        var bi = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN);
        
        assertThrows(IllegalArgumentException.class, () -> packIgnite(p -> p.packBigInteger(bi)));
    }
    
    @ParameterizedTest
    @ValueSource(floats = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Float.MIN_VALUE, Float.MAX_VALUE})
    public void testPackFloat(float f) {
        testPacker(p -> p.packFloat(f), p -> p.packFloat(f));
    }
    
    @ParameterizedTest
    @ValueSource(doubles = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Float.MIN_VALUE, Float.MAX_VALUE,
            Double.MIN_VALUE, Double.MAX_VALUE})
    public void testPackDouble(double d) {
        testPacker(p -> p.packDouble(d), p -> p.packDouble(d));
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"", "Abc", "Абв", "🔥", "𒀖𝄞"})
    public void testPackString(String s) {
        testPacker(p -> p.packString(s), p -> p.packString(s));
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testPackArrayHeader(int i) {
        testPacker(p -> p.packArrayHeader(i), p -> p.packArrayHeader(i));
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testPackMapHeader(int i) {
        testPacker(p -> p.packMapHeader(i), p -> p.packMapHeader(i));
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testPackExtensionTypeHeader(int i) {
        testPacker(p -> p.packExtensionTypeHeader((byte) 33, i), p -> p.packExtensionTypeHeader((byte) 33, i));
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testPackBinaryHeader(int i) {
        testPacker(p -> p.packBinaryHeader(i), p -> p.packBinaryHeader(i));
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testPackRawStringHeader(int i) {
        testPacker(p -> p.packRawStringHeader(i), p -> p.packRawStringHeader(i));
    }
    
    @Test
    public void testWritePayload() {
        var b = new byte[]{1, 5, 120};
        
        testPacker(p -> p.writePayload(b), p -> p.writePayload(b));
        testPacker(p -> p.writePayload(b, 1, 1), p -> p.writePayload(b, 1, 1));
    }
    
    private interface MessagePackerConsumer {
        void accept(MessagePacker p) throws IOException;
    }
}

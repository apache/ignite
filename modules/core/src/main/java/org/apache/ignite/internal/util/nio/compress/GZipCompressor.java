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

package org.apache.ignite.internal.util.nio.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongUnaryOperator;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class GZipCompressor implements NioCompressor {

    /*
    * 0 - 0..10 bytes
    * 1 - 10..100 bytes
    * 2 - 100..1000 bytes
    * 3 - > 1000 bytes
    */
    static AtomicLongArray arrCnt = new AtomicLongArray(4);
    static AtomicLongArray arrBytes = new AtomicLongArray(4);
    static AtomicLongArray arrCompressBytes = new AtomicLongArray(4);

    static void selfcheck(int cnt) {
        //test
        byte[] arr = new byte[cnt];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (byte)i;
        }

        try {
            ByteBuffer buf = ByteBuffer.wrap(arr);
            ByteBuffer compress = new GridNioCompressor().compress(buf);
            byte[] arr1 = new byte[compress.limit()];
            compress.get(arr1);
            int length = arr1.length;
//            System.out.println("Compress: "+ Arrays.toString(arr1));

            ByteBuffer buf2 = ByteBuffer.wrap(arr1);
            ByteBuffer decompress = new GridNioCompressor().decompress(buf2);
            byte[] arr3 = new byte[decompress.limit()];
            decompress.get(arr3);
//            System.out.println("Decompress: "+ Arrays.toString(arr3));

            System.out.println("MY SELFTEST:"+cnt+" "+(cnt==arr3.length) + " c:"+length);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    static {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override public void run() {
//                selfcheck(0);
//                selfcheck(1);
//                selfcheck(10);
//                selfcheck(100);
//                selfcheck(1000);
//                selfcheck(10000);
//                System.out.println("0..10 cnt:"+arrCnt.get(0)+" b:"+arrBytes.get(0)+" c:"+arrCompressBytes.get(0)+" r:"+arrBytes.get(0)*1.0/arrCompressBytes.get(0));
//                System.out.println("10..100 cnt:"+arrCnt.get(1)+" b:"+arrBytes.get(1)+" c:"+arrCompressBytes.get(1)+" r:"+arrBytes.get(1)*1.0/arrCompressBytes.get(1));
//                System.out.println("100..1000 cnt:"+arrCnt.get(2)+" b:"+arrBytes.get(2)+" c:"+arrCompressBytes.get(2)+" r:"+arrBytes.get(2)*1.0/arrCompressBytes.get(2));
//                System.out.println("1000.. cnt:"+arrCnt.get(3)+" b:"+arrBytes.get(3)+" c:"+arrCompressBytes.get(3)+" r:"+arrBytes.get(3)*1.0/arrCompressBytes.get(3));
            }
        }, 0, 3, TimeUnit.SECONDS);
    }

    static byte beforeCompress(int length) {
        byte type;
        if (length < 10)
            type = 0;
        else if (length < 100)
            type = 1;
        else if (length < 1000)
            type = 2;
        else
            type = 3;

        arrCnt.incrementAndGet(type);
        arrBytes.updateAndGet(type, new LongUnaryOperator() {
            @Override public long applyAsLong(long l) {
                return l + length;
            }
        });
        return type;
    }

    static void afterCompress(byte type, int length) {
        arrCnt.incrementAndGet(type);
        arrCompressBytes.updateAndGet(type, new LongUnaryOperator() {
            @Override public long applyAsLong(long l) {
                return l + length;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer compress(@NotNull ByteBuffer buf) throws IOException {
        byte[] bytes = new byte[buf.remaining()];

        buf.get(bytes);
//        System.out.println("1: "+bytes.length);

//        System.out.println("before:"+ Arrays.toString(bytes));

//        byte b = beforeCompress(bytes.length);

        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream out = new GZIPOutputStream(baos);
        ) {
            out.write(bytes);
            out.close(); // need it, otherwise EOFException at decompressing

            bytes = baos.toByteArray();

//            afterCompress(b, bytes.length);

//            byte[] tail = new byte[]{7,7,7};
//            byte[] temp = new byte[bytes.length+tail.length];
//            System.arraycopy(bytes, 0, temp, 0, bytes.length);
//            System.arraycopy(tail, 0, temp, bytes.length, tail.length);
//            bytes = temp;

            if (bytes.length > buf.capacity()) {
                ByteOrder order = buf.order();

                if (buf.isDirect())
                    buf = ByteBuffer.allocateDirect(bytes.length);
                else
                    buf = ByteBuffer.allocate(bytes.length);

                buf.order(order);
            }
            else
                buf.clear();

//            System.out.println("\t2: "+bytes.length);
            buf.put(bytes);

            buf.flip();

            return buf;
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer decompress(@NotNull ByteBuffer buf) throws IOException {
        byte[] bytes = new byte[buf.remaining()];

        buf.get(bytes);
//        System.out.println("\t\t3: "+(bytes.length));

//        byte[] bytes = new byte[buf.remaining() - 3];
//        byte[] temp = new byte[3];
//        System.out.println("\t\t3: "+(bytes.length + 3));
//        buf.get(bytes);
//        buf.get(temp);
//        if (temp[0] != 7) {
//            System.out.println("FAIL!!:" + Arrays.toString(temp));
//            buf.rewind();
//            return buf;
//        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))
        ) {
            byte[] buffer = new byte[32];
            int length;

            while ((length = in.read(buffer)) != -1)
                baos.write(buffer, 0, length);

            baos.flush();

            bytes = baos.toByteArray();
        }

        if (bytes.length > buf.capacity()) {
            ByteOrder order = buf.order();

            if (buf.isDirect())
                buf = ByteBuffer.allocateDirect(bytes.length);
            else
                buf = ByteBuffer.allocate(bytes.length);

            buf.order(order);
        }
        else
            buf.clear();

//        System.out.println("\t\t\t4: "+bytes.length);
        buf.put(bytes);

        buf.flip();

        return buf;
    }
}

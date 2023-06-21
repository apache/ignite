/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class ByteBufferWalIteratorTest extends GridCommonAbstractTest {

    /** */
    private static ByteBuffer readFile(File file) throws IOException {
        int byteCount = (int)file.length();

        FileInputStream fileInputStream = new FileInputStream(file);

        final byte[] bytes = new byte[byteCount];

        int l = fileInputStream.read(bytes);

        return ByteBuffer.wrap(bytes, 0, l);
    }

    /** */
    @Test
    public void test() throws IOException, IgniteCheckedException {
        ByteBuffer bb = readFile(new File("/Users/21107153/sources/ignite/tmp/0000000000000000.wal"));

        bb.order(ByteOrder.nativeOrder());

        ByteBufferWalIterator byteBufferWalIterator = new ByteBufferWalIterator(log, bb);

        while (byteBufferWalIterator.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> next = byteBufferWalIterator.next();

            System.out.println(next);
        }

        byteBufferWalIterator.close();
    }
}

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
package org.apache.ignite.raft.jraft.util;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class UtilsTest {
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    public void teardown() throws Exception {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }

    @Test
    public void testRunThread() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Utils.runInThread(executor, new Runnable() {
            @Override
            public void run() {
                latch.countDown();
            }
        });
        latch.await();
    }

    @Test
    public void tetsVerifyGroupId1() {
        assertThrows(IllegalArgumentException.class, () -> Utils.verifyGroupId(""));
    }

    @Test
    public void tetsVerifyGroupId2() {
        assertThrows(IllegalArgumentException.class, () -> Utils.verifyGroupId(null));
    }

    @Test
    public void tetsVerifyGroupId4() {
        assertThrows(IllegalArgumentException.class, () -> Utils.verifyGroupId("*test"));
    }

    @Test
    public void tetsVerifyGroupId5() {
        Utils.verifyGroupId("t");
        Utils.verifyGroupId("T");
        Utils.verifyGroupId("Test");
        Utils.verifyGroupId("test");
        Utils.verifyGroupId("test-hello");
        Utils.verifyGroupId("test123");
        Utils.verifyGroupId("t_hello");
        Utils.verifyGroupId("1abc");
    }

    @Test
    public void testRunClosure() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Utils.runClosureInExecutor(executor, new Closure() {

            @Override
            public void run(Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        });
        latch.await();
    }

    @Test
    public void testRunClosureWithStatus() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Utils.runClosureInExecutor(executor, new Closure() {

            @Override
            public void run(Status status) {
                assertFalse(status.isOk());
                assertEquals(RaftError.EACCES.getNumber(), status.getCode());
                assertEquals("test 99", status.getErrorMsg());
                latch.countDown();
            }
        }, new Status(RaftError.EACCES, "test %d", 99));
        latch.await();
    }

    @Test
    public void test_getProcessId() {
        long pid = Utils.getProcessId(-1);
        assertNotEquals(-1, pid);
        System.out.println("test pid:" + pid);
    }

    @Test
    public void testAllocateExpandByteBuffer() {
        ByteBuffer buf = Utils.allocate(128);
        assertEquals(0, buf.position());
        assertEquals(128, buf.capacity());
        assertEquals(128, buf.remaining());

        buf.put("hello".getBytes());
        assertEquals(5, buf.position());

        buf = Utils.expandByteBufferAtLeast(buf, 128);
        assertEquals(5, buf.position());
        assertEquals(1152, buf.capacity());
        assertEquals(1147, buf.remaining());

        buf = Utils.expandByteBufferAtLeast(buf, 2048);
        assertEquals(5, buf.position());
        assertEquals(1152 + 2048, buf.capacity());
        assertEquals(1147 + 2048, buf.remaining());
    }

    @Test
    public void testParsePeerId() {
        String pid = "192.168.1.88:5566";
        String[] result = Utils.parsePeerId(pid);
        String[] expecteds = {"192.168.1.88", "5566"};
        assertTrue(result.length == 2);
        assertArrayEquals(expecteds, result);

        pid = "[fe80:0:0:0:6450:aa3c:cd98:ed0f]:8847";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] {"[fe80:0:0:0:6450:aa3c:cd98:ed0f]", "8847"};
        assertTrue(result.length == 2);
        assertArrayEquals(expecteds, result);

        pid = "192.168.1.88:5566:9";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] {"192.168.1.88", "5566", "9"};
        assertTrue(result.length == 3);
        assertArrayEquals(expecteds, result);

        pid = "[fe80:0:0:0:6450:aa3c:cd98:ed0f]:8847:9";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] {"[fe80:0:0:0:6450:aa3c:cd98:ed0f]", "8847", "9"};
        assertTrue(result.length == 3);
        assertArrayEquals(expecteds, result);

        pid = "192.168.1.88:5566:0:6";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] {"192.168.1.88", "5566", "0", "6"};
        assertTrue(result.length == 4);
        assertArrayEquals(expecteds, result);

        pid = "[fe80:0:0:0:6450:aa3c:cd98:ed0f]:8847:0:6";
        result = Utils.parsePeerId(pid);
        expecteds = new String[] {"[fe80:0:0:0:6450:aa3c:cd98:ed0f]", "8847", "0", "6"};
        assertTrue(result.length == 4);
        assertArrayEquals(expecteds, result);

        boolean ex1 = false;
        try {
            pid = "[192.168.1].88:eee:x:b:j";
            Utils.parsePeerId(pid);
        }
        catch (Exception e) {
            ex1 = true;
        }
        assertTrue(ex1);

        boolean ex2 = false;
        try {
            pid = "[dsfsadf]:eee:x:b:j";
            Utils.parsePeerId(pid);
        }
        catch (Exception e) {
            ex2 = true;
        }
        assertTrue(ex2);

    }

}

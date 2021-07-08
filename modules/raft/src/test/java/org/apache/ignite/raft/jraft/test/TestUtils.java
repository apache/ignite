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
package org.apache.ignite.raft.jraft.test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.mockito.ArgumentCaptor;

import static java.lang.Thread.sleep;

/**
 * Test helper
 */
public class TestUtils {
    public static ConfigurationEntry getConfEntry(String confStr, String oldConfStr) {
        ConfigurationEntry entry = new ConfigurationEntry();
        entry.setConf(JRaftUtils.getConfiguration(confStr));
        entry.setOldConf(JRaftUtils.getConfiguration(oldConfStr));
        return entry;
    }

    public static void dumpThreads() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = bean.dumpAllThreads(true, true);
        for (ThreadInfo info : infos)
            System.out.println(info);
    }

    public static LogEntry mockEntry(int index, int term) {
        return mockEntry(index, term, 0);
    }

    public static LogEntry mockEntry(int index, int term, int dataSize) {
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(index, term));
        if (dataSize > 0) {
            byte[] bs = new byte[dataSize];
            ThreadLocalRandom.current().nextBytes(bs);
            entry.setData(ByteBuffer.wrap(bs));
        }
        return entry;
    }

    public static List<LogEntry> mockEntries() {
        return mockEntries(10);
    }

    /**
     * Returns the localhost IP address.
     *
     * @return localhost IP address
     */
    public static String getLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new IgniteInternalException(e);
        }
    }

    public static List<LogEntry> mockEntries(int n) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            LogEntry entry = mockEntry(i, i);
            if (i > 0)
                entry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
            entries.add(entry);
        }
        return entries;
    }

    public static RpcRequests.PingRequest createPingRequest() {
        return RpcRequests.PingRequest.newBuilder()
            .setSendTimestamp(System.currentTimeMillis())
            .build();
    }

    public static final int INIT_PORT = 5003;

    public static List<PeerId> generatePeers(int n) {
        List<PeerId> ret = new ArrayList<>();
        for (int i = 0; i < n; i++)
            ret.add(new PeerId(getLocalAddress(), INIT_PORT + i));
        return ret;
    }

    public static List<PeerId> generatePriorityPeers(int n, List<Integer> priorities) {
        List<PeerId> ret = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Endpoint endpoint = new Endpoint(getLocalAddress(), INIT_PORT + i);
            PeerId peerId = new PeerId(endpoint, 0, priorities.get(i));
            ret.add(peerId);
        }
        return ret;
    }

    public static byte[] getRandomBytes() {
        byte[] requestContext = new byte[ThreadLocalRandom.current().nextInt(10) + 1];
        ThreadLocalRandom.current().nextBytes(requestContext);
        return requestContext;
    }

    /**
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    public static boolean waitForTopology(ClusterService cluster, int expected, int timeout) {
        return waitForCondition(() -> cluster.topologyService().allMembers().size() >= expected, timeout);
    }

    /**
     * @param cond The condition.
     * @param timeout The timeout.
     * @return {@code True} if condition has happened within the timeout.
     */
    @SuppressWarnings("BusyWait") public static boolean waitForCondition(BooleanSupplier cond, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean())
                return true;

            try {
                sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * @param captor The captor.
     * @param timeout The timeout.
     * @return {@code True} if condition has happened within the timeout.
     */
    public static boolean waitForArgumentCapture(ArgumentCaptor<?> captor, long timeout) {
        return waitForCondition(() -> {
            try {
                return captor.getValue() != null;
            }
            catch (Exception e) {
                return false;
            }
        }, timeout);
    }
}

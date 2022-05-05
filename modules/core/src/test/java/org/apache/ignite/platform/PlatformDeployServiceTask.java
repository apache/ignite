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

package org.apache.ignite.platform;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.platform.model.ACL;
import org.apache.ignite.platform.model.Account;
import org.apache.ignite.platform.model.Address;
import org.apache.ignite.platform.model.Department;
import org.apache.ignite.platform.model.Employee;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.platform.model.Parameter;
import org.apache.ignite.platform.model.Role;
import org.apache.ignite.platform.model.User;
import org.apache.ignite.platform.model.V10;
import org.apache.ignite.platform.model.V11;
import org.apache.ignite.platform.model.V12;
import org.apache.ignite.platform.model.V13;
import org.apache.ignite.platform.model.V14;
import org.apache.ignite.platform.model.V15;
import org.apache.ignite.platform.model.V16;
import org.apache.ignite.platform.model.V5;
import org.apache.ignite.platform.model.V6;
import org.apache.ignite.platform.model.V7;
import org.apache.ignite.platform.model.V8;
import org.apache.ignite.platform.model.V9;
import org.apache.ignite.platform.model.Value;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Calendar.JANUARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Task that deploys a Java service.
 */
public class PlatformDeployServiceTask extends ComputeTaskAdapter<String, Object> {
    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String serviceName) throws IgniteException {
        return Collections.singletonMap(new PlatformDeployServiceJob(serviceName), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class PlatformDeployServiceJob extends ComputeJobAdapter {
        /** Service name. */
        private final String serviceName;

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Ctor.
         *
         * @param serviceName Service name.
         */
        private PlatformDeployServiceJob(String serviceName) {
            assert serviceName != null;
            this.serviceName = serviceName;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            ignite.services().deployNodeSingleton(serviceName, new PlatformTestService());

            return null;
        }
    }

    /**
     * Test service.
     */
    public static class PlatformTestService implements Service {
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private boolean isCancelled;

        /** */
        private boolean isInitialized;

        /** */
        private boolean isExecuted;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            isCancelled = true;
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            isInitialized = true;
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            isExecuted = true;
        }

        /**
         * Returns a value indicating whether this service is cancelled.
         */
        public boolean isCancelled() {
            return isCancelled;
        }

        /**
         * Returns a value indicating whether this service is initialized.
         */
        public boolean isInitialized() {
            return isInitialized;
        }

        /**
         * Returns a value indicating whether this service is executed.
         */
        public boolean isExecuted() {
            return isExecuted;
        }

        /** */
        public byte test(byte arg) {
            return (byte) (arg + 1);
        }

        /** */
        public short test(short arg) {
            return (short) (arg + 1);
        }

        /** */
        public int test(int arg) {
            return arg + 1;
        }

        /** */
        public long test(long arg) {
            return arg + 1;
        }

        /** */
        public float test(float arg) {
            return arg + 1.5f;
        }

        /** */
        public double test(double arg) {
            return arg + 2.5;
        }

        /** */
        public boolean test(boolean arg) {
            return !arg;
        }

        /** */
        public char test(char arg) {
            return (char) (arg + 1);
        }

        /** */
        public String test(String arg) {
            return arg == null ? null : arg + "!";
        }

        /** */
        public Timestamp test(Timestamp input) {
            Timestamp exp = new Timestamp(92, JANUARY, 1, 0, 0, 0, 0);

            if (!exp.equals(input))
                throw new RuntimeException("Expected \"" + exp + "\" but got \"" + input + "\"");

            return input;
        }

        /** */
        public UUID test(UUID input) {
            return input;
        }

        /** */
        public Byte testWrapper(Byte arg) {
            return arg == null ? null : (byte) (arg + 1);
        }

        /** */
        public Short testWrapper(Short arg) {
            return arg == null ? null : (short) (arg + 1);
        }

        /** */
        public Integer testWrapper(Integer arg) {
            return arg == null ? null : arg + 1;
        }

        /** */
        public Long testWrapper(Long arg) {
            return arg == null ? null : arg + 1;
        }

        /** */
        public Float testWrapper(Float arg) {
            return arg == null ? null : arg + 1.5f;
        }

        /** */
        public Double testWrapper(Double arg) {
            return arg == null ? null : arg + 2.5;
        }

        /** */
        public Boolean testWrapper(Boolean arg) {
            return arg == null ? null : !arg;
        }

        /** */
        public Character testWrapper(Character arg) {
            return arg == null ? null : (char) (arg + 1);
        }

        /** */
        public byte[] testArray(byte[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public short[] testArray(short[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public int[] testArray(int[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public long[] testArray(long[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public double[] testArray(double[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public float[] testArray(float[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public String[] testArray(String[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public char[] testArray(char[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] += 1;

            return arg;
        }

        /** */
        public boolean[] testArray(boolean[] arg) {
            if (arg != null)
                for (int i = 0; i < arg.length; i++)
                    arg[i] = !arg[i];

            return arg;
        }

        /** */
        public Timestamp[] testArray(Timestamp[] arg) {
            if (arg == null || arg.length != 1)
                throw new RuntimeException("Expected array of length 1");

            return new Timestamp[] {test(arg[0])};
        }

        /** */
        public UUID[] testArray(UUID[] arg) {
            return arg;
        }

        /** */
        public Integer testNull(Integer arg) {
            return arg == null ? null : arg + 1;
        }

        /** */
        public UUID testNullUUID(UUID arg) {
            return arg;
        }

        /** */
        public Timestamp testNullTimestamp(Timestamp arg) {
            return arg;
        }

        /** */
        public int testParams(Object... args) {
            return args.length;
        }

        /** */
        public int test(int x, String y) {
            return x + 1;
        }

        /** */
        public int test(String x, int y) {
            return y + 1;
        }

        /** */
        public PlatformComputeBinarizable testBinarizable(PlatformComputeBinarizable arg) {
            return arg == null ? null : new PlatformComputeBinarizable(arg.field + 1);
        }

        /** */
        public Object[] testBinarizableArrayOfObjects(Object[] arg) {
            if (arg == null)
                return null;

            for (int i = 0; i < arg.length; i++)
                arg[i] = arg[i] == null
                    ? null
                    : new PlatformComputeBinarizable(((PlatformComputeBinarizable)arg[i]).field + 1);

            return arg;
        }

        /** */
        public PlatformComputeBinarizable[] testBinarizableArray(PlatformComputeBinarizable[] arg) {
            return (PlatformComputeBinarizable[])testBinarizableArrayOfObjects(arg);
        }

        /** */
        public BinaryObject[] testBinaryObjectArray(BinaryObject[] arg) {
            for (int i = 0; i < arg.length; i++) {
                int field = arg[i].field("Field");

                arg[i] = arg[i].toBuilder().setField("Field", field + 1).build();
            }

            return arg;
        }

        /** */
        public Collection testBinarizableCollection(Collection arg) {
            if (arg == null)
                return null;

            Collection<PlatformComputeBinarizable> res = new ArrayList<>(arg.size());

            for (Object x : arg)
                res.add(new PlatformComputeBinarizable(((PlatformComputeBinarizable)x).field + 1));

            return res;
        }

        /** */
        public BinaryObject testBinaryObject(BinaryObject o) {
            if (o == null)
                return null;

            return o.toBuilder().setField("field", 15).build();
        }

        /** */
        public Address testAddress(Address addr) {
            if (addr == null)
                return null;

            assertEquals("000", addr.getZip());
            assertEquals("Moscow", addr.getAddr());

            addr.setZip("127000");
            addr.setAddr("Moscow Akademika Koroleva 12");

            return addr;
        }

        /** */
        public int testOverload(Integer count, Employee[] emps) {
            assertNotNull(emps);
            assertEquals((int)count, emps.length);

            assertEquals("Sarah Connor", emps[0].getFio());
            assertEquals(1, emps[0].getSalary());

            assertEquals("John Connor", emps[1].getFio());
            assertEquals(2, emps[1].getSalary());

            return 42;
        }

        /** */
        public int testOverload(int count, Parameter[] params) {
            assert params != null;
            assert count == params.length;

            assert 1 == params[0].getId();
            assert 2 == params[0].getValues().length;

            assert 1 == params[0].getValues()[0].getId();
            assert 42 == params[0].getValues()[0].getVal();

            assert 2 == params[0].getValues()[1].getId();
            assert 43 == params[0].getValues()[1].getVal();

            assert 2 == params[1].getId();
            assert 2 == params[1].getValues().length;

            assert 3 == params[1].getValues()[0].getId();
            assert 44 == params[1].getValues()[0].getVal();

            assert 4 == params[1].getValues()[1].getId();
            assert 45 == params[1].getValues()[1].getVal();

            return 43;
        }

        /** */
        public int testOverload(int first, int second) {
            return first + second;
        }

        /** */
        public Employee[] testEmployees(Employee[] emps) {
            if (emps == null)
                return null;

            assertEquals(2, emps.length);

            assertEquals("Sarah Connor", emps[0].getFio());
            assertEquals(1, emps[0].getSalary());

            assertEquals("John Connor", emps[1].getFio());
            assertEquals(2, emps[1].getSalary());

            Employee kyle = new Employee();

            kyle.setFio("Kyle Reese");
            kyle.setSalary(3);

            return new Employee[] { kyle };
        }

        /** */
        public Collection testDepartments(Collection deps) {
            if (deps == null)
                return null;

            assertEquals(2, deps.size());

            Iterator<Department> iter = deps.iterator();

            assertEquals("HR", iter.next().getName());
            assertEquals("IT", iter.next().getName());

            Collection<Department> res = new ArrayList<>();

            Department d = new Department();

            d.setName("Executive");

            res.add(d);

            return res;
        }

        /** */
        public Map testMap(Map map) {
            if (map == null)
                return null;

            assertTrue(map.containsKey(new Key(1)));
            assertTrue(map.containsKey(new Key(2)));

            assertEquals("value1", ((Value)map.get(new Key(1))).getVal());
            assertEquals("value2", ((Value)map.get(new Key(2))).getVal());

            Map m = new HashMap();

            m.put(new Key(3), new Value("value3"));

            return m;
        }

        /** */
        public Account[] testAccounts() {
            return new Account[] {
                new Account("123", 42),
                new Account("321", 0)
            };
        }

        /** */
        public User[] testUsers() {
            return new User[] {
                new User(1, ACL.ALLOW, new Role("admin")),
                new User(2, ACL.DENY, new Role("user"))
            };
        }

        /** */
        public void testDateArray(Timestamp[] dates) {
            assertNotNull(dates);
            assertEquals(2, dates.length);
            assertEquals(new Timestamp(new Date(82, Calendar.APRIL, 1, 0, 0, 0).getTime()), dates[0]);
            assertEquals(new Timestamp(new Date(91, Calendar.OCTOBER, 1, 0, 0, 0).getTime()), dates[1]);
        }

        /** */
        public Timestamp testDate(Timestamp date) {
            if (date == null)
                return null;

            assertEquals(new Timestamp(new Date(82, Calendar.APRIL, 1, 0, 0, 0).getTime()), date);

            return new Timestamp(new Date(91, Calendar.OCTOBER, 1, 0, 0, 0).getTime());
        }

        /** */
        public void testUTCDateFromCache() {
            IgniteCache<Integer, Timestamp> cache = ignite.cache("net-dates");

            cache.put(3, new Timestamp(new Date(82, Calendar.APRIL, 1, 0, 0, 0).getTime()));
            cache.put(4, new Timestamp(new Date(91, Calendar.OCTOBER, 1, 0, 0, 0).getTime()));

            assertEquals(new Timestamp(new Date(82, Calendar.APRIL, 1, 0, 0, 0).getTime()), cache.get(1));
            assertEquals(new Timestamp(new Date(91, Calendar.OCTOBER, 1, 0, 0, 0).getTime()), cache.get(2));
        }

        /** */
        public void testLocalDateFromCache() {
            IgniteCache<Integer, Timestamp> cache = ignite.cache("net-dates");

            ZoneId msk = ZoneId.of("Europe/Moscow");

            //This Date in Europe/Moscow have offset +4.
            Timestamp ts1 = new Timestamp(ZonedDateTime.of(1982, 4, 1, 1, 0, 0, 0, msk).toInstant().toEpochMilli());
            //This Date in Europe/Moscow have offset +3.
            Timestamp ts2 = new Timestamp(ZonedDateTime.of(1982, 3, 31, 22, 0, 0, 0, msk).toInstant().toEpochMilli());

            assertEquals(ts1, cache.get(5));
            assertEquals(ts2, cache.get(6));

            cache.put(7, ts1);
            cache.put(8, ts2);
        }

        /** */
        public void putValsForCache() {
            ignite.<Integer, V9>getOrCreateCache("V9").put(1, new V9("1"));

            IgniteCache<Integer, V10> v10 = ignite.getOrCreateCache("V10");

            v10.put(1, new V10("1"));
            v10.put(2, new V10("2"));

            ignite.<Integer, V11>getOrCreateCache("V11").put(1, new V11("1"));

            IgniteCache<Integer, V12> v12 = ignite.getOrCreateCache("V12");

            v12.put(1, new V12("1"));
            v12.put(2, new V12("2"));

            IgniteCache<Integer, V13> v13 = ignite.getOrCreateCache("V13");

            v13.put(1, new V13("1"));
            v13.put(2, new V13("2"));

            IgniteCache<Integer, V14> v14 = ignite.getOrCreateCache("V14");

            v14.put(1, new V14("1"));
            v14.put(2, new V14("2"));

            IgniteCache<Integer, V15> v15 = ignite.getOrCreateCache("V15");

            v15.put(1, new V15("1"));
            v15.put(2, new V15("2"));

            CacheConfiguration<Integer, V16> ccfg = new CacheConfiguration<>("V16");

            ccfg.setIndexedTypes(Integer.class, V16.class);

            IgniteCache<Integer, V16> v16 = ignite.getOrCreateCache(ccfg);

            v16.put(1, new V16("1"));
            v16.put(2, new V16("2"));
        }

        /** */
        private final AtomicInteger cntMsgs = new AtomicInteger(0);

        /** */
        public void startReceiveMessage() {
            ignite.message().localListen("test-topic-2", (node, obj) -> {
                assert obj instanceof BinaryObject;

                V6 v6 = ((BinaryObject)obj).deserialize();

                assert "Sarah Connor".equals(v6.getName()) ||
                    "John Connor".equals(v6.getName()) ||
                    "Kyle Reese".equals(v6.getName());

                cntMsgs.incrementAndGet();

                return true;
            });

            ignite.message().localListen("test-topic-3", (node, obj) -> {
                assert obj instanceof BinaryObject;

                V7 v7 = ((BinaryObject)obj).deserialize();

                assert "V7-1".equals(v7.getName()) ||
                    "V7-2".equals(v7.getName()) ||
                    "V7-3".equals(v7.getName());

                cntMsgs.incrementAndGet();

                return true;
            });

            ignite.message().localListen("test-topic-4", (node, obj) -> {
                assert obj instanceof BinaryObject;

                V8 v8 = ((BinaryObject)obj).deserialize();

                assert "V8".equals(v8.getName()) ||
                    "V9".equals(v8.getName()) ||
                    "V10".equals(v8.getName());

                cntMsgs.incrementAndGet();

                return true;
            });
        }

        /** */
        public boolean testMessagesReceived() {
            try {
                return GridTestUtils.waitForCondition(() -> cntMsgs.get() == 9, 1_000 * 5);
            }
            catch (IgniteInterruptedCheckedException e) {
                return false;
            }
        }

        /** */
        public void testSendMessage() {
            ignite.message().sendOrdered("test-topic", new V5("1"), 1_000 * 5);
            ignite.message().sendOrdered("test-topic", new V5("2"), 1_000 * 5);
            ignite.message().sendOrdered("test-topic", new V5("3"), 1_000 * 5);
        }

        /** */
        public void testException(String exCls) throws Exception {
            switch (exCls) {
                case "InterruptedException": throw new InterruptedException("Test");
                case "IllegalArgumentException": throw new IllegalArgumentException("Test");
                case "TestMapped1Exception": throw new TestMapped1Exception("Test");
                case "TestMapped2Exception": throw new TestMapped2Exception("Test");
                case "TestUnmappedException": throw new TestUnmappedException("Test");
                default: throw new IgniteException("Unexpected exception class: " + exCls);
            }
        }

        /** */
        public void sleep(long delayMs) {
            try {
                U.sleep(delayMs);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }

    /** */
    public static class TestMapped1Exception extends RuntimeException {
        /** */
        public TestMapped1Exception(String msg) {
            super(msg);
        }
    }

    /** */
    public static class TestMapped2Exception extends RuntimeException {
        /** */
        public TestMapped2Exception(String msg) {
            super(msg);
        }
    }

    /** */
    public static class TestUnmappedException extends RuntimeException {
        /** */
        public TestUnmappedException(String msg) {
            super(msg);
        }
    }
}

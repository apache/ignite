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

package org.apache.ignite.lang.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.util.GridConcurrentWeakHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test for {@link GridConcurrentWeakHashSet}.
 */
@GridCommonTest(group = "Lang")
public class GridConcurrentWeakHashSetSelfTest extends GridCommonAbstractTest {
    /** Time to wait after {@link System#gc} method call. */
    private static final long WAIT_TIME = 3000;

    /** How many time to call GC. */
    private static final int GC_CALL_CNT = 5;

    /**
     * @throws Exception Thrown if test failed.
     */
    public void testA() throws Exception {
        Collection<Integer> set = new GridConcurrentWeakHashSet<>();

        Integer i = 1;

        assert set.add(i);
        assert !set.add(i);

        assert set.contains(i);

        assert set.size() == 1;

        Collection<Integer> c = F.asList(2, 3, 4, 5);

        assert set.addAll(c);
        assert !set.addAll(c);

        assert set.containsAll(c);

        assert set.size() == 1 + c.size();

        assert set.remove(i);
        assert !set.remove(i);

        assert !set.contains(i);

        assert set.size() == c.size();

        assert set.removeAll(c);
        assert !set.removeAll(c);

        assert !set.containsAll(c);

        assert set.isEmpty();

        Collection<Integer> c1 = Arrays.asList(1, 3, 5, 7, 9);

        int cnt = 0;

        for (Iterator<Integer> iter = set.iterator(); iter.hasNext(); cnt++)
            c1.contains(iter.next());

        assert set.size() == cnt;

        assert set.size() == set.toArray().length;

        assert set.addAll(c1);

        assert set.retainAll(c);
        assert !set.retainAll(c);

        Collection<Integer> c2 = F.retain(c1, true, c);

        assert set.containsAll(c2);
        assert !set.containsAll(c1);
        assert !set.containsAll(c);

        assert set.size() == c2.size();

        set.clear();

        assert set.isEmpty();

        try {
            set.iterator().next();

            assert false;
        }
        catch (NoSuchElementException ignored) {
            assert true;
        }

        try {
            set.add(null);

            assert false;
        }
        catch (NullPointerException ignored) {
            assert true;
        }
    }

    /**
     * @throws Exception Thrown if test failed.
     */
    @SuppressWarnings({"UnusedAssignment"})
    public void testB() throws Exception {
        Collection<SampleBean> set = new GridConcurrentWeakHashSet<>();

        SampleBean bean1 = new SampleBean(1);

        assert set.add(bean1);
        assert !set.add(bean1);

        assert set.size() == 1;

        assert set.contains(bean1);

        bean1 = null;

        gc();

        assert set.isEmpty();

        Collection<SampleBean> c = F.asList(new SampleBean(1), new SampleBean(2), new SampleBean(3), new SampleBean(4));

        assert set.addAll(c);
        assert !set.addAll(c);

        assert set.size() == c.size();

        assert set.containsAll(c);

        c = null;

        gc();

        assert set.isEmpty();

        SampleBean b1 = new SampleBean(1);
        SampleBean b2 = new SampleBean(2);
        SampleBean b3 = new SampleBean(3);
        SampleBean b4 = new SampleBean(4);
        SampleBean b5 = new SampleBean(5);

        set.add(b1);
        set.add(b2);
        set.add(b3);
        set.add(b4);
        set.add(b5);

        Iterator iter = set.iterator();

        assert iter.hasNext();

        b2 = null;
        b3 = null;
        b4 = null;

        gc();

        int cnt = 0;

        while (iter.hasNext()) {
            info(iter.next().toString());

            cnt++;
        }

        assert set.size() == cnt;
    }

    /**
     * @throws Exception Thrown if test failed.
     */
    public void testC() throws Exception {
        final Collection<SampleBean> set = new GridConcurrentWeakHashSet<>();

        int threadCnt = 2;

        final int cnt = 5;

        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch stop = new CountDownLatch(threadCnt);

        Runnable r = new Runnable() {
            @Override public void run() {
                try {
                    start.await();

                    for (int i = 0; i < cnt; i++) {
                        for (int j = 0; j < cnt; j++)
                            set.add(new SampleBean(i));
                    }
                }
                catch (Exception e) {
                    error(e.getMessage());
                }

                stop.countDown();
            }
        };

        for (int i = 0; i < threadCnt; i++)
            new Thread(r).start();

        start.countDown();

        stop.await();

        assert set.size() == cnt;

        gc();

        assert set.isEmpty();
    }

    /**
     * @throws Exception Thrown if test failed.
     */
    public void testD() throws Exception {
        final Collection<SampleBean> set = new GridConcurrentWeakHashSet<>();

        final int cnt = 100;

        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch stop = new CountDownLatch(3);

        new Thread() {
            @Override public void run() {
                try {
                    start.await();

                    for (int i = 0; i < cnt; i++) {
                        for (int j = 0; j < cnt; j++)
                            set.add(new SampleBean(i));
                    }
                }
                catch (Exception e) {
                    error(e.getMessage());
                }

                stop.countDown();
            }
        }.start();

        new Thread() {
            @Override public void run() {
                try {
                    start.await();

                    for (int i = 0; i < cnt; i++) {
                        for (int j = 0; j < cnt; j++)
                            set.remove(new SampleBean(i));
                    }
                }
                catch (Exception e) {
                    error(e.getMessage());
                }

                stop.countDown();
            }
        }.start();

        new Thread() {
            @SuppressWarnings({"UnusedDeclaration"})
            @Override public void run() {
                try {
                    start.await();

                    while (stop.getCount() > 1) {
                        for (SampleBean b : set) {
                            // No-op.
                        }
                    }
                }
                catch (Exception e) {
                    error(e.getMessage());
                }

                stop.countDown();
            }
        }.start();

        start.countDown();

        stop.await();

        gc();

        assert set.isEmpty();
    }

    /**
     * Calls garbage collector and wait.
     *
     * @throws Exception if any thread has interrupted the current thread while waiting.
     */
    private void gc() throws Exception {
        Runtime rt = Runtime.getRuntime();

        long freeMem0 = rt.freeMemory();
        long freeMem = Long.MAX_VALUE;

        int cnt = 0;

        while (freeMem0 < freeMem && cnt < GC_CALL_CNT) {
            System.gc();

            U.sleep(WAIT_TIME);

            cnt++;

            freeMem = freeMem0;
            freeMem0 = rt.freeMemory();
        }
    }

    /**
     * Sample bean for this test.
     */
    private static class SampleBean {
        /** Number. */
        private int num;

        /** String. */
        private String str;

        /**
         * Creates sample bean.
         *
         * @param num Number.
         */
        private SampleBean(int num) {
            this.num = num;

            str = String.valueOf(num);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"RedundantIfStatement"})
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof SampleBean))
                return false;

            SampleBean that = (SampleBean)o;

            if (num != that.num)
                return false;

            if (str != null ? !str.equals(that.str) : that.str != null)
                return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = num;

            result = 31 * result + (str != null ? str.hashCode() : 0);

            return result;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SampleBean.class, this);
        }
    }
}
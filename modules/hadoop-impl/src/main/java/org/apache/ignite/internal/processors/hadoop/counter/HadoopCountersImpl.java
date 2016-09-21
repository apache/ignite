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

package org.apache.ignite.internal.processors.hadoop.counter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentHashMap8;

/**
 * Default in-memory counters store.
 */
public class HadoopCountersImpl implements HadoopCounters, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final ConcurrentMap<CounterKey, HadoopCounter> cntrsMap = new ConcurrentHashMap8<>();

    /**
     * Default constructor. Creates new instance without counters.
     */
    public HadoopCountersImpl() {
        // No-op.
    }

    /**
     * Creates new instance that contain given counters.
     *
     * @param cntrs Counters to store.
     */
    public HadoopCountersImpl(Iterable<HadoopCounter> cntrs) {
        addCounters(cntrs, true);
    }

    /**
     * Copy constructor.
     *
     * @param cntrs Counters to copy.
     */
    public HadoopCountersImpl(HadoopCounters cntrs) {
        this(cntrs.all());
    }

    /**
     * Creates counter instance.
     *
     * @param cls Class of the counter.
     * @param grp Group name.
     * @param name Counter name.
     * @return Counter.
     */
    private <T extends HadoopCounter> T createCounter(Class<? extends HadoopCounter> cls, String grp,
        String name) {
        try {
            Constructor constructor = cls.getConstructor(String.class, String.class);

            return (T)constructor.newInstance(grp, name);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Adds counters collection in addition to existing counters.
     *
     * @param cntrs Counters to add.
     * @param cp Whether to copy counters or not.
     */
    private void addCounters(Iterable<HadoopCounter> cntrs, boolean cp) {
        assert cntrs != null;

        for (HadoopCounter cntr : cntrs) {
            if (cp) {
                HadoopCounter cntrCp = createCounter(cntr.getClass(), cntr.group(), cntr.name());

                cntrCp.merge(cntr);

                cntr = cntrCp;
            }

            cntrsMap.put(new CounterKey(cntr.getClass(), cntr.group(), cntr.name()), cntr);
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends HadoopCounter> T counter(String grp, String name, Class<T> cls) {
        assert cls != null;

        CounterKey mapKey = new CounterKey(cls, grp, name);

        T cntr = (T)cntrsMap.get(mapKey);

        if (cntr == null) {
            cntr = createCounter(cls, grp, name);

            T old = (T)cntrsMap.putIfAbsent(mapKey, cntr);

            if (old != null)
                return old;
        }

        return cntr;
    }

    /** {@inheritDoc} */
    @Override public Collection<HadoopCounter> all() {
        return cntrsMap.values();
    }

    /** {@inheritDoc} */
    @Override public void merge(HadoopCounters other) {
        for (HadoopCounter counter : other.all())
            counter(counter.group(), counter.name(), counter.getClass()).merge(counter);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeCollection(out, cntrsMap.values());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        addCounters(U.<HadoopCounter>readCollection(in), false);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        HadoopCountersImpl counters = (HadoopCountersImpl)o;

        return cntrsMap.equals(counters.cntrsMap);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return cntrsMap.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopCountersImpl.class, this, "counters", cntrsMap.values());
    }

    /**
     * The tuple of counter identifier components for more readable code.
     */
    private static class CounterKey extends GridTuple3<Class<? extends HadoopCounter>, String, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Constructor.
         *
         * @param cls Class of the counter.
         * @param grp Group name.
         * @param name Counter name.
         */
        private CounterKey(Class<? extends HadoopCounter> cls, String grp, String name) {
            super(cls, grp, name);
        }

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public CounterKey() {
            // No-op.
        }
    }
}
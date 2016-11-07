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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class CacheContinuousQueryFactoryAsyncFilterRandomOperationTest
    extends CacheContinuousQueryFactoryFilterRandomOperationTest {
    /** {@inheritDoc} */
    @NotNull @Override protected Factory<? extends CacheEntryEventFilter<QueryTestKey, QueryTestValue>>
        createFilterFactory() {
        return new AsyncFilterFactory();
    }

    /**
     *
     */
    @IgniteAsyncCallback
    protected static class NonSerializableAsyncFilter implements
        CacheEntryEventSerializableFilter<QueryTestKey, QueryTestValue>, Externalizable {
        /** */
        public NonSerializableAsyncFilter() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> evt) {
            assertTrue("Failed. Current thread name: " + Thread.currentThread().getName(),
                Thread.currentThread().getName().contains("callback-"));

            assertFalse("Failed. Current thread name: " + Thread.currentThread().getName(),
                Thread.currentThread().getName().contains("sys-"));

            return isAccepted(evt.getValue());
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            fail("Entry filter should not be marshaled.");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            fail("Entry filter should not be marshaled.");
        }

        /**
         * @param val Value.
         * @return {@code True} if value is even.
         */
        public static boolean isAccepted(QueryTestValue val) {
            return val == null || val.val1 % 2 == 0;
        }
    }

    /**
     *
     */
    protected static class AsyncFilterFactory implements Factory<NonSerializableAsyncFilter> {
        /** {@inheritDoc} */
        @Override public NonSerializableAsyncFilter create() {
            return new NonSerializableAsyncFilter();
        }
    }

    /** {@inheritDoc} */
    @Override protected Factory<? extends CacheEntryEventFilter<QueryTestKey, QueryTestValue>> noOpFilterFactory() {
        return FactoryBuilder.factoryOf(NoopAsyncFilter.class);
    }

    /**
     *
     */
    @IgniteAsyncCallback
    protected static class NoopAsyncFilter implements
        CacheEntryEventSerializableFilter<QueryTestKey, QueryTestValue>, Externalizable {
        /** */
        public NoopAsyncFilter() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> evt) {
            assertTrue("Failed. Current thread name: " + Thread.currentThread().getName(),
                Thread.currentThread().getName().contains("callback-"));

            assertFalse("Failed. Current thread name: " + Thread.currentThread().getName(),
                Thread.currentThread().getName().contains("sys-"));

            return true;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }
}

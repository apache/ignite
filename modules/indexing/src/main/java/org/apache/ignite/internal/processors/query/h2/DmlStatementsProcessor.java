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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 * Contains entry processors for DML. Should be modified very carefully to maintain binary compatibility due to
 * seializable anonymous classes.
 */
@SuppressWarnings({"Anonymous2MethodRef", "PublicInnerClass", "unused"})
public class DmlStatementsProcessor {
    /** */
    public static final class InsertEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** Value to set. */
        private final Object val;

        /** */
        public InsertEntryProcessor(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments)
            throws EntryProcessorException {
            if (entry.exists())
                return false;

            entry.setValue(val);
            return null; // To leave out only erroneous keys - nulls are skipped on results' processing.
        }
    }

    /**
     * Entry processor invoked by UPDATE and DELETE operations.
     */
    public static final class ModifyingEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** Value to expect. */
        private final Object val;

        /** Action to perform on entry. */
        private final IgniteInClosure<MutableEntry<Object, Object>> entryModifier;

        /** */
        public ModifyingEntryProcessor(Object val, IgniteInClosure<MutableEntry<Object, Object>> entryModifier) {
            assert val != null;

            this.val = val;
            this.entryModifier = entryModifier;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments)
            throws EntryProcessorException {
            if (!entry.exists())
                return null; // Someone got ahead of us and removed this entry, let's skip it.

            Object entryVal = entry.getValue();

            if (entryVal == null)
                return null;

            // Something happened to the cache while we were performing map-reduce.
            if (!F.eq(entryVal, val))
                return false;

            entryModifier.apply(entry);

            return null; // To leave out only erroneous keys - nulls are skipped on results' processing.
        }
    }

    /** Dummy anonymous class to advance RMV anonymous value to 5. */
    private static final Runnable DUMMY_1 = new Runnable() {
        @Override public void run() {
            // No-op.
        }
    };

    /** Dummy anonymous class to advance RMV anonymous value to 5. */
    private static final Runnable DUMMY_2 = new Runnable() {
        @Override public void run() {
            // No-op.
        }
    };

    /** Dummy anonymous class to advance RMV anonymous value to 5. */
    private static final Runnable DUMMY_3 = new Runnable() {
        @Override public void run() {
            // No-op.
        }
    };

    /** Dummy anonymous class to advance RMV anonymous value to 5. */
    private static final Runnable DUMMY_4 = new Runnable() {
        @Override public void run() {
            // No-op.
        }
    };

    /** Remove updater. Must not be moved around to keep at anonymous position 5. */
    public static final IgniteInClosure<MutableEntry<Object, Object>> RMV =
        new IgniteInClosure<MutableEntry<Object, Object>>() {
        @Override public void apply(MutableEntry<Object, Object> e) {
            e.remove();
        }
    };

    /**
     * Entry value updater.
     */
    public static final class EntryValueUpdater implements IgniteInClosure<MutableEntry<Object, Object>> {
        /** Value to set. */
        private final Object val;

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public EntryValueUpdater(Object val) {
            assert val != null;

            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void apply(MutableEntry<Object, Object> e) {
            e.setValue(val);
        }
    }
}

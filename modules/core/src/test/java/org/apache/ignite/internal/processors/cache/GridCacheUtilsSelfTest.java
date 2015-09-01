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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Grid cache utils test.
 */
public class GridCacheUtilsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String EXTERNALIZABLE_WARNING = "For best performance you should implement " +
        "java.io.Externalizable";

    /**
     * Does not override equals and hashCode.
     */
    private static class NoEqualsAndHashCode {
    }

    /**
     * Does not override equals.
     */
    private static class NoEquals {
        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 1;
        }
    }

    /**
     * Does not override hashCode.
     */
    private static class NoHashCode {
        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    /**
     * Defines equals with different signature.
     */
    private static class WrongEquals {
        /**
         * @param obj Object.
         * @return {@code False}.
         */
        @SuppressWarnings("CovariantEquals")
        public boolean equals(String obj) {
            return false;
        }
    }

    /**
     * Overrides equals and hashCode.
     */
    private static class EqualsAndHashCode {
        /** {@inheritDoc} */
        @Override public int hashCode() {
            return super.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    /**
     * Overrides equals and hashCode and implements {@link Externalizable}.
     */
    private static class ExternalizableEqualsAndHashCode implements Externalizable {
        /**
         * Constructor required by {@link Externalizable}.
         */
        public ExternalizableEqualsAndHashCode() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return super.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return super.equals(obj);
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

    /**
     * Extends class which overrides equals and hashCode.
     */
    private static class ExtendsClassWithEqualsAndHashCode extends EqualsAndHashCode {
    }

    /**
     * Extends class which overrides equals and hashCode, overrides equals and hashCode.
     */
    private static class ExtendsClassWithEqualsAndHashCode2 extends EqualsAndHashCode {
        /** {@inheritDoc} */
        @Override public int hashCode() {
            return super.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    /**
     * Does not implement {@link Externalizable}.
     */
    private static class NoImplExternalizable {
    }

    /**
     * Implements {@link Externalizable}.
     */
    private static class ImplExternalizable implements Externalizable  {
        /**
         * Constructor required by {@link Externalizable}.
         */
        public ImplExternalizable() {
            // No-op.
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

    /**
     * Extends class which implements {@link Externalizable}.
     */
    private static class ExtendsImplExternalizable extends ImplExternalizable {
        /**
         * Constructor required by {@link Externalizable}.
         */
        public ExtendsImplExternalizable() {
            // No-op.
        }
    }

    /**
     */
    public void testCacheKeyValidation() {
        CU.validateCacheKey(log, "key");

        CU.validateCacheKey(log, 1);

        CU.validateCacheKey(log, 1L);

        CU.validateCacheKey(log, 1.0);

        CU.validateCacheKey(log, new ExtendsClassWithEqualsAndHashCode());

        CU.validateCacheKey(log, new ExtendsClassWithEqualsAndHashCode2());

        assertThrowsForInvalidKey(new NoEqualsAndHashCode());

        assertThrowsForInvalidKey(new NoEquals());

        assertThrowsForInvalidKey(new NoHashCode());

        assertThrowsForInvalidKey(new WrongEquals());

        IgniteLogger log = new GridStringLogger(false);

        CU.validateCacheKey(log, new ExternalizableEqualsAndHashCode());

        assertFalse(log.toString().contains(EXTERNALIZABLE_WARNING));

        CU.validateCacheKey(log, "key");

        assertFalse(log.toString().contains(EXTERNALIZABLE_WARNING));

        CU.validateCacheKey(log, new EqualsAndHashCode());

        assertTrue(log.toString().contains(EXTERNALIZABLE_WARNING));
    }

    /**
     */
    public void testCacheValueValidation() {
        IgniteLogger log = new GridStringLogger(false);

        CU.validateCacheValue(log, new ImplExternalizable());

        assertFalse(log.toString().contains(EXTERNALIZABLE_WARNING));

        CU.validateCacheValue(log, new ExtendsImplExternalizable());

        assertFalse(log.toString().contains(EXTERNALIZABLE_WARNING));

        CU.validateCacheValue(log, "val");

        assertFalse(log.toString().contains(EXTERNALIZABLE_WARNING));

        CU.validateCacheValue(log, new byte[10]);

        assertFalse(log.toString().contains(EXTERNALIZABLE_WARNING));

        CU.validateCacheValue(log, new NoImplExternalizable());

        assertTrue(log.toString().contains(EXTERNALIZABLE_WARNING));
    }

    /**
     * @param key Cache key.
     */
    private void assertThrowsForInvalidKey(final Object key) {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                CU.validateCacheKey(log, key);

                return null;
            }
        }, IllegalArgumentException.class, null);
    }
}

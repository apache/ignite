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

import java.util.concurrent.Callable;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Grid cache utils test.
 */
public class GridCacheUtilsSelfTest extends GridCommonAbstractTest {
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
     */
    public void testCacheKeyValidation() {
        CU.validateCacheKey("key");

        CU.validateCacheKey(1);

        CU.validateCacheKey(1L);

        CU.validateCacheKey(1.0);

        CU.validateCacheKey(new ExtendsClassWithEqualsAndHashCode());

        CU.validateCacheKey(new ExtendsClassWithEqualsAndHashCode2());

        assertThrowsForInvalidKey(new NoEqualsAndHashCode());

        assertThrowsForInvalidKey(new NoEquals());

        assertThrowsForInvalidKey(new NoHashCode());

        assertThrowsForInvalidKey(new WrongEquals());
    }

    /**
     * @param key Cache key.
     */
    private void assertThrowsForInvalidKey(final Object key) {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                CU.validateCacheKey(key);

                return null;
            }
        }, IllegalArgumentException.class, null);
    }
}

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

import javax.cache.processor.MutableEntry;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.GridTestNode;
import org.junit.Assert;
import org.junit.Test;

/**
 * Ensures that anonymous classes of entry modifiers are compatible with old versions.
 */
public class DmlStatementsProcessorTest {
    /**
     * Checks that remove-closure is available by anonymous class position (4).
     * This is required for compatibility with versions < 2.7.0.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveEntryModifierCompatibilityOld() throws Exception {
        checkRemoveClosureByAnonymousPosition(4);
    }

    /**
     * Checks that remove-closure is available by anonymous class position (5).
     * This is required for compatibility with versions >= 2.7.0.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveEntryModifierCompatibilityNew() throws Exception {
        checkRemoveClosureByAnonymousPosition(5);
    }

    /**
     * Checks that the old remove-closure is used if the remote node version is less than 2.7.0.
     */
    @Test
    public void testRemoveEntryModifierClassName() {
        String oldClsName = DmlStatementsProcessor.class.getName() + "$" + 4;
        String newClsName = DmlStatementsProcessor.class.getName() + "$" + 5;

        checkRemoveEntryClassName("2.4.0", oldClsName);
        checkRemoveEntryClassName("2.5.0", oldClsName);
        checkRemoveEntryClassName("2.6.0", oldClsName);

        checkRemoveEntryClassName("2.7.0", newClsName);
        checkRemoveEntryClassName("2.8.0", newClsName);
    }

    /**
     * Checks remove-closure class name.
     *
     * @param ver The version of the remote node.
     * @param expClsName Expected class name.
     */
    private void checkRemoveEntryClassName(final String ver, String expClsName) {
        ClusterNode node = new GridTestNode() {
            @Override public IgniteProductVersion version() {
                return IgniteProductVersion.fromString(ver);
            }
        };

        IgniteInClosure<MutableEntry<Object, Object>> rmvC =
            DmlStatementsProcessor.getRemoveClosure(node, 0);

        Assert.assertNotNull("Check remove-closure", rmvC);

        Assert.assertEquals("Check remove-closure class name for version " + ver,
            expClsName, rmvC.getClass().getName());
    }

    /**
     * Checks that remove-closure is available by anonymous class position.
     */
    @SuppressWarnings("unchecked")
    private void checkRemoveClosureByAnonymousPosition(int position) throws Exception {
        Class<?> cls = Class.forName(DmlStatementsProcessor.class.getName() + "$" + position);

        IgniteInClosure<MutableEntry<Object, Object>> rmvC =
            (IgniteInClosure<MutableEntry<Object, Object>>)cls.newInstance();

        CustomMutableEntry<Object, Object> entry = new CustomMutableEntry<>();

        rmvC.apply(entry);

        Assert.assertTrue("Entry should be removed", entry.isRemoved());
    }

    /**
     *
     */
    private static class CustomMutableEntry<K, V> implements MutableEntry<K, V> {
        /** */
        private boolean rmvd;

        /**
         * @return {@code true} if remove method was called.
         */
        private boolean isRemoved() {
            return rmvd;
        }

        /** {@inheritDoc} */
        @Override public boolean exists() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            rmvd = true;
        }

        /** {@inheritDoc} */
        @Override public void setValue(V v) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> aCls) {
            return null;
        }
    }
}

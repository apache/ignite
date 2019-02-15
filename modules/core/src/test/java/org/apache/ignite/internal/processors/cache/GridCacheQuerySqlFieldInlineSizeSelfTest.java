/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QueryGroupIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests cache configuration with inlineSize property of the QuerySqlField annotation.
 */
@SuppressWarnings({"unchecked", "unused"})
public class GridCacheQuerySqlFieldInlineSizeSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleFieldIndexes() throws Exception {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setIndexedTypes(Integer.class, TestValueSingleFieldIndexes.class);

        assertEquals(1, ccfg.getQueryEntities().size());

        QueryEntity ent = (QueryEntity)ccfg.getQueryEntities().iterator().next();

        assertEquals(2, ent.getIndexes().size());

        for (QueryIndex idx : ent.getIndexes()) {
            if(idx.getFields().containsKey("val0"))
                assertEquals(10, idx.getInlineSize());
            else if(idx.getFields().containsKey("val1"))
                assertEquals(20, idx.getInlineSize());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGroupIndex() throws Exception {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setIndexedTypes(Integer.class, TestValueGroupIndex.class);

        assertEquals(1, ccfg.getQueryEntities().size());

        QueryEntity ent = (QueryEntity)ccfg.getQueryEntities().iterator().next();

        assertEquals(1, ent.getIndexes().size());

        QueryIndex idx = ent.getIndexes().iterator().next();

        assertEquals(10, idx.getInlineSize());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGroupIndexInvalidAnnotaion() throws Exception {
        final CacheConfiguration ccfg = defaultCacheConfiguration();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ccfg.setIndexedTypes(Integer.class, TestValueGroupIndexInvalidAnnotation.class);

                return null;
            }
        }, CacheException.class, "Inline size cannot be set on a field with group index");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeInlineSize() throws Exception {
        final CacheConfiguration ccfg = defaultCacheConfiguration();

        GridTestUtils.assertThrows(
            log, new Callable<Object>() {

                @Override public Object call() throws Exception {
                    ccfg.setIndexedTypes(Integer.class, TestValueNegativeInlineSize.class);

                    return null;
                }
            },
            CacheException.class,
            "Illegal inline size [idxName=TestValueNegativeInlineSize_val_idx, inlineSize=-10]"
        );
    }

    /**
     *
     */
    static class TestValueSingleFieldIndexes {
        /** */
        @QuerySqlField(index = true, inlineSize = 10)
        String val0;

        /** */
        @QuerySqlField(index = true, inlineSize = 20)
        String val1;
    }

    /**
     *
     */
    @QueryGroupIndex(name="idx", inlineSize = 10)
    static class TestValueGroupIndex {
        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = "idx", order = 0))
        String val0;

        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = "idx", order = 1))
        String val1;
    }

    /**
     *
     */
    @QueryGroupIndex(name="idx")
    static class TestValueGroupIndexInvalidAnnotation {
        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = "idx", order = 0))
        String val0;

        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = "idx", order = 1), inlineSize = 10)
        String val1;
    }

    /**
     *
     */
    static class TestValueNegativeInlineSize {
         /** */
         @QuerySqlField(index = true, inlineSize = -10)
         String val;
     }
}

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
            if (idx.getFields().containsKey("val0"))
                assertEquals(10, idx.getInlineSize());
            else if (idx.getFields().containsKey("val1"))
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
    @QueryGroupIndex(name = "idx", inlineSize = 10)
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
    @QueryGroupIndex(name = "idx")
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

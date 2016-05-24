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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.Callable;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests for "keepBinary" method.
 */
@SuppressWarnings("unchecked")
public class CacheWithKeepBinarySelfTest extends GridCommonAbstractTest {
    /** Whether to use binary marshaller. */
    private boolean binary;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (binary)
            cfg.setMarshaller(new BinaryMarshaller());
        else
            cfg.setMarshaller(new OptimizedMarshaller());

        cfg.setCacheConfiguration(new CacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test for binary marshaller.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("UnnecessaryBoxing")
    public void testBinaryMarshaller() throws Exception {
        binary = true;

        Ignite ignite = startGrid();

        IgniteCache<Integer, BinaryObject> cache = ignite.cache(null).withKeepBinary();

        BinaryObject obj = ignite.binary().builder(CacheWithKeepBinarySelfTest.class.getSimpleName())
            .setField("intField", 1).build();

        cache.put(1, obj);

        BinaryObject otherObj = cache.get(1);

        assert otherObj != null;
        assert F.eq(obj.type().typeName(), otherObj.type().typeName());
        assert F.eq(otherObj.field("intField"), Integer.valueOf(1));
    }

    /**
     * Test for non-binary marshaller.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testNonBinaryMarshaller() throws Exception {
        binary = false;

        final Ignite ignite = startGrid();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public IgniteCache<String,String> call() throws Exception {
                ignite.cache(null).withKeepBinary();

                return null;
            }
        }, IgniteException.class, null);
    }
}

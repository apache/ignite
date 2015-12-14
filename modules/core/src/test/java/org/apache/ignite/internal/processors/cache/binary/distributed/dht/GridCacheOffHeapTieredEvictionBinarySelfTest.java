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

package org.apache.ignite.internal.processors.cache.binary.distributed.dht;

import java.util.Arrays;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapTieredEvictionSelfTest;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.binary.BinaryObject;

/**
 *
 */
public class GridCacheOffHeapTieredEvictionBinarySelfTest extends GridCacheOffHeapTieredEvictionSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        // Enable binary.
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setClassNames(Arrays.asList(TestValue.class.getName()));

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected TestPredicate testPredicate(String expVal, boolean acceptNull) {
        return new BinaryValuePredicate(expVal, acceptNull);
    }

    /** {@inheritDoc} */
    @Override protected TestProcessor testClosure(String expVal, boolean acceptNull) {
        return new BinaryValueClosure(expVal, acceptNull);
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class BinaryValuePredicate extends TestPredicate {
        /**
         * @param expVal Expected value.
         * @param acceptNull If {@code true} value can be null;
         */
        BinaryValuePredicate(String expVal, boolean acceptNull) {
            super(expVal, acceptNull);
        }

        /** {@inheritDoc} */
        @Override public void checkValue(Object val) {
            BinaryObject obj = (BinaryObject)val;

            assertEquals(expVal, obj.field("val"));
        }
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class BinaryValueClosure extends TestProcessor {
        /**
         * @param expVal Expected value.
         * @param acceptNull If {@code true} value can be null;
         */
        BinaryValueClosure(String expVal, boolean acceptNull) {
            super(expVal, acceptNull);
        }

        /** {@inheritDoc} */
        @Override public void checkValue(Object val) {
            BinaryObject obj = (BinaryObject)val;

            assertEquals(expVal, obj.field("val"));
        }
    }
}

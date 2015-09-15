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

package org.apache.ignite.internal.processors.cache.portable.distributed.dht;

import java.util.Arrays;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapTieredEvictionSelfTest;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableObject;

/**
 *
 */
public class GridCacheOffHeapTieredEvictionPortableSelfTest extends GridCacheOffHeapTieredEvictionSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        // Enable portables.
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(TestValue.class.getName()));

        cfg.setMarshaller(marsh);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected TestPredicate testPredicate(String expVal, boolean acceptNull) {
        return new PortableValuePredicate(expVal, acceptNull);
    }

    /** {@inheritDoc} */
    @Override protected TestProcessor testClosure(String expVal, boolean acceptNull) {
        return new PortableValueClosure(expVal, acceptNull);
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class PortableValuePredicate extends TestPredicate {
        /**
         * @param expVal Expected value.
         * @param acceptNull If {@code true} value can be null;
         */
        PortableValuePredicate(String expVal, boolean acceptNull) {
            super(expVal, acceptNull);
        }

        /** {@inheritDoc} */
        @Override public void checkValue(Object val) {
            PortableObject obj = (PortableObject)val;

            assertEquals(expVal, obj.field("val"));
        }
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class PortableValueClosure extends TestProcessor {
        /**
         * @param expVal Expected value.
         * @param acceptNull If {@code true} value can be null;
         */
        PortableValueClosure(String expVal, boolean acceptNull) {
            super(expVal, acceptNull);
        }

        /** {@inheritDoc} */
        @Override public void checkValue(Object val) {
            PortableObject obj = (PortableObject)val;

            assertEquals(expVal, obj.field("val"));
        }
    }
}
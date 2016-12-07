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

package org.apache.ignite.internal.binary;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryArrayIdentityResolver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.ArrayList;
import java.util.List;

/**
 * Test for identity resolver configuration.
 */
public class BinaryIdentityResolverConfigurationSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

        BinaryConfiguration binCfg = new BinaryConfiguration();

        BinaryTypeConfiguration binTypCfg = new BinaryTypeConfiguration();

        binTypCfg.setTypeName(MyClass.class.getName());
        binTypCfg.setIdentityResolver(new CustomResolver());

        List<BinaryTypeConfiguration> binTypCfgs = new ArrayList<>();

        binTypCfgs.add(binTypCfg);

        binCfg.setTypeConfigurations(binTypCfgs);

        cfg.setBinaryConfiguration(binCfg);

        return cfg;
    }

    /**
     * Test type resolver.
     */
    public void testTypeResolver() {
        MyClass obj = new MyClass(1, 2);

        int expHash = hash(obj.a, obj.b);

        BinaryObject binObj1 = binary().toBinary(obj);
        BinaryObject binObj2 =
            binary().builder(MyClass.class.getName()).setField("a", obj.a).setField("b", obj.b).build();

        assertEquals(expHash, binObj1.hashCode());
        assertEquals(expHash, binObj2.hashCode());
    }

    /**
     * @return Binary interface for current Ignite instance.
     */
    public IgniteBinary binary() {
        return grid().binary();
    }

    /**
     * Second hash function.
     *
     * @param a First value.
     * @param b Second value.
     * @return Result.
     */
    public static int hash(Object a, Object b) {
        return 31 * a.hashCode() + b.hashCode();
    }

    /**
     * First class.
     */
    private static class MyClass {
        /** Value 1. */
        public int a;

        /** Value 2. */
        public int b;

        /**
         * Constructor.
         *
         * @param a Value 1.
         * @param b Value 2.
         */
        public MyClass(int a, int b) {
            this.a = a;
            this.b = b;
        }
    }

    /**
     * First custom identity resolver.
     */
    private static class CustomResolver extends BinaryArrayIdentityResolver {
        /** {@inheritDoc} */
        @Override protected int hashCode0(BinaryObject obj) {
            return hash(obj.field("a"), obj.field("b"));
        }
    }
}

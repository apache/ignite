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

package org.apache.ignite.internal.processors.hadoop;

import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests Hadoop Snappy codec invocation.
 */
public class HadoopSnappyTest extends GridCommonAbstractTest {
    /**
     * Checks Snappy Codec invokeing same simple test from .
     *
     * @throws Exception On error.
     */
    public void testSnappyCodec() throws Exception {
        // Run Snappy test in default class loader:
        HadoopSnappyUtil.printDiagnosticAndTestSnappy(getClass(), null);

        // Run the same in several more class loaders simulating jobs and tasks:
        for (int i=0; i<5; i++) {
            ClassLoader cl = new HadoopClassLoader(null, "cl-" + i);

            Class<?> clazz = (Class)Class.forName(HadoopSnappyUtil.class.getName(), true, cl);

            Method m = clazz.getDeclaredMethod("printDiagnosticAndTestSnappy", Class.class,
                cl.loadClass(Configuration.class.getName()));

            m.setAccessible(true);

            m.invoke(null, clazz, null);
        }
    }
}

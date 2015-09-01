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

import junit.framework.TestCase;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 */
public class HadoopClassLoaderTest extends TestCase {
    /** */
    HadoopClassLoader ldr = new HadoopClassLoader(null, "test");

    /**
     * @throws Exception If failed.
     */
    public void testClassLoading() throws Exception {
        assertNotSame(Test1.class, ldr.loadClass(Test1.class.getName()));
        assertNotSame(Test2.class, ldr.loadClass(Test2.class.getName()));
        assertSame(Test3.class, ldr.loadClass(Test3.class.getName()));
    }

//    public void testDependencySearch() {
//        assertTrue(ldr.hasExternalDependencies(Test1.class.getName(), new HashSet<String>()));
//        assertTrue(ldr.hasExternalDependencies(Test2.class.getName(), new HashSet<String>()));
//    }

    /**
     *
     */
    private static class Test1 {
        /** */
        Test2 t2;

        /** */
        Job[][] jobs = new Job[4][4];
    }

    /**
     *
     */
    private static abstract class Test2 {
        /** */
        abstract Test1 t1();
    }

    /**
     *
     */
    private static class Test3 {
        // No-op.
    }
}
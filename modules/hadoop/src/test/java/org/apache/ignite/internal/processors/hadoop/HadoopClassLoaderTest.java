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

import javax.security.auth.AuthPermission;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.ignite.internal.processors.hadoop.deps.CircularWIthHadoop;
import org.apache.ignite.internal.processors.hadoop.deps.CircularWithoutHadoop;
import org.apache.ignite.internal.processors.hadoop.deps.WithIndirectField;
import org.apache.ignite.internal.processors.hadoop.deps.WithCast;
import org.apache.ignite.internal.processors.hadoop.deps.WithClassAnnotation;
import org.apache.ignite.internal.processors.hadoop.deps.WithConstructorInvocation;
import org.apache.ignite.internal.processors.hadoop.deps.WithMethodCheckedException;
import org.apache.ignite.internal.processors.hadoop.deps.WithMethodRuntimeException;
import org.apache.ignite.internal.processors.hadoop.deps.WithExtends;
import org.apache.ignite.internal.processors.hadoop.deps.WithField;
import org.apache.ignite.internal.processors.hadoop.deps.WithImplements;
import org.apache.ignite.internal.processors.hadoop.deps.WithInitializer;
import org.apache.ignite.internal.processors.hadoop.deps.WithInnerClass;
import org.apache.ignite.internal.processors.hadoop.deps.WithLocalVariable;
import org.apache.ignite.internal.processors.hadoop.deps.WithMethodAnnotation;
import org.apache.ignite.internal.processors.hadoop.deps.WithMethodInvocation;
import org.apache.ignite.internal.processors.hadoop.deps.WithMethodArgument;
import org.apache.ignite.internal.processors.hadoop.deps.WithMethodReturnType;
import org.apache.ignite.internal.processors.hadoop.deps.WithOuterClass;
import org.apache.ignite.internal.processors.hadoop.deps.WithParameterAnnotation;
import org.apache.ignite.internal.processors.hadoop.deps.WithStaticField;
import org.apache.ignite.internal.processors.hadoop.deps.WithStaticInitializer;
import org.apache.ignite.internal.processors.hadoop.deps.Without;

/**
 * Tests for Hadoop classloader.
 */
public class HadoopClassLoaderTest extends TestCase {
    /** */
    final HadoopClassLoader ldr = new HadoopClassLoader(null, "test");

    /**
     * @throws Exception If failed.
     */
    public void testClassLoading() throws Exception {
        assertNotSame(CircularWIthHadoop.class, ldr.loadClass(CircularWIthHadoop.class.getName()));
        assertNotSame(CircularWithoutHadoop.class, ldr.loadClass(CircularWithoutHadoop.class.getName()));

        assertSame(Without.class, ldr.loadClass(Without.class.getName()));
    }

    /**
     * Test dependency search.
     */
    public void testDependencySearch() {
        // Positive cases:
        final Class[] positiveClasses = {
            Configuration.class,
            HadoopUtils.class,
            WithStaticField.class,
            WithCast.class,
            WithClassAnnotation.class,
            WithConstructorInvocation.class,
            WithMethodCheckedException.class,
            WithMethodRuntimeException.class,
            WithExtends.class,
            WithField.class,
            WithImplements.class,
            WithInitializer.class,
            WithInnerClass.class,
            WithOuterClass.InnerNoHadoop.class,
            WithLocalVariable.class,
            WithMethodAnnotation.class,
            WithMethodInvocation.class,
            WithMethodArgument.class,
            WithMethodReturnType.class,
            WithParameterAnnotation.class,
            WithStaticField.class,
            WithStaticInitializer.class,
            WithIndirectField.class,
            CircularWIthHadoop.class,
            CircularWithoutHadoop.class,
        };

        for (Class c: positiveClasses)
            assertTrue(c.getName(), ldr.hasExternalDependencies(c.getName()));

        // Negative cases:
        final Class[] negativeClasses = {
            Object.class,
            AuthPermission.class,
            Without.class,
        };

        for (Class c: negativeClasses)
            assertFalse(c.getName(), ldr.hasExternalDependencies(c.getName()));
    }
}
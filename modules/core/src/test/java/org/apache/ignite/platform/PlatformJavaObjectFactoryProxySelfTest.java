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

package org.apache.ignite.platform;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.platform.PlatformJavaObjectFactoryProxy;
import org.apache.ignite.platform.javaobject.TestJavaObject;
import org.apache.ignite.platform.javaobject.TestJavaObjectNoDefaultCtor;
import org.apache.ignite.platform.javaobject.TestJavaObjectNoDefaultCtorFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Dedicated tests for {@link PlatformJavaObjectFactoryProxy}.
 */
public class PlatformJavaObjectFactoryProxySelfTest extends GridCommonAbstractTest {
    /** Name of the class. */
    private static final String CLS_NAME = TestJavaObject.class.getName();

    /** Name of the factory class to create object without default constructor. */
    private static final String NO_DFLT_CTOR_FACTORY_CLS_NAME = TestJavaObjectNoDefaultCtorFactory.class.getName();

    /** Kernal context. */
    private GridKernalContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = ((IgniteKernal)Ignition.start(getConfiguration(PlatformJavaObjectFactoryProxySelfTest.class.getName())))
            .context();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ctx = null;

        Ignition.stopAll(true);
    }

    /**
     * Test normal object creation using default factory.
     */
    @Test
    public void testDefaultFactoryNormal() {
        Map<String, Object> props = new HashMap<>();

        props.put("fBoolean", true);
        props.put("fByte", (byte)1);
        props.put("fShort", (short)2);
        props.put("fChar", '3');
        props.put("fInt", 4);
        props.put("fLong", 5L);
        props.put("fFloat", 6.6f);
        props.put("fDouble", 7.7d);

        UUID obj = UUID.randomUUID();

        props.put("fObj", obj);

        props.put("fIntBoxed", 10);

        PlatformJavaObjectFactoryProxy proxy =
            new PlatformJavaObjectFactoryProxy(PlatformJavaObjectFactoryProxy.TYP_DEFAULT, null, CLS_NAME, props);

        TestJavaObject val = (TestJavaObject)proxy.factory(ctx).create();

        TestJavaObject expVal = new TestJavaObject().setBoolean(true).setByte((byte)1).setShort((short)2).setChar('3')
            .setInt(4).setLong(5L).setFloat(6.6f).setDouble(7.7d).setObject(obj).setIntBoxed(10);

        assertEquals(expVal, val);
    }

    /**
     * Test normal object creation using custom factory.
     */
    @Test
    public void testCustomFactoryNormal() {
        Map<String, Object> props = new HashMap<>();

        props.put("fBoolean", true);
        props.put("fByte", (byte)1);
        props.put("fShort", (short)2);
        props.put("fChar", '3');
        props.put("fInt", 4);
        props.put("fLong", 5L);
        props.put("fFloat", 6.6f);
        props.put("fDouble", 7.7d);

        UUID obj = UUID.randomUUID();

        props.put("fObj", obj);

        props.put("fIntBoxed", 10);

        PlatformJavaObjectFactoryProxy proxy = proxyForCustom(NO_DFLT_CTOR_FACTORY_CLS_NAME, props);

        TestJavaObjectNoDefaultCtor val = (TestJavaObjectNoDefaultCtor)proxy.factory(ctx).create();

        TestJavaObject expVal = new TestJavaObject().setBoolean(true).setByte((byte)1).setShort((short)2).setChar('3')
            .setInt(4).setLong(5L).setFloat(6.6f).setDouble(7.7d).setObject(obj).setIntBoxed(10);

        assertEquals(expVal, val);

        assertNotNull(val.node);
        assertEquals(val.node.name(), ctx.igniteInstanceName());
    }

    /**
     * Test object creation with boxed property.
     */
    @Test
    public void testCustomFactoryBoxedProperty() {
        PlatformJavaObjectFactoryProxy proxy = proxyForCustom(NO_DFLT_CTOR_FACTORY_CLS_NAME,
            Collections.singletonMap("fIntBoxed", (Object)1));

        Object val = proxy.factory(ctx).create();

        assertEquals(val, new TestJavaObject().setIntBoxed(1));
    }

    /**
     * Test object creation without properties.
     */
    @Test
    public void testCustomFactoryNoProperties() {
        PlatformJavaObjectFactoryProxy proxy = proxyForCustom(NO_DFLT_CTOR_FACTORY_CLS_NAME,
            Collections.<String, Object>emptyMap());

        Object val = proxy.factory(ctx).create();

        assertEquals(val, new TestJavaObject());
    }

    /**
     * Test object creation with invalid property name.
     */
    @Test
    public void testCustomFactoryInvalidPropertyName() {
        final PlatformJavaObjectFactoryProxy proxy = proxyForCustom(NO_DFLT_CTOR_FACTORY_CLS_NAME,
            Collections.singletonMap("invalid", (Object)1));

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return proxy.factory(ctx).create();
            }
        }, IgniteException.class, null);
    }

    /**
     * Test object creation with invalid property value.
     */
    @Test
    public void testCustomFactoryInvalidPropertyValue() {
        final PlatformJavaObjectFactoryProxy proxy = proxyForCustom(NO_DFLT_CTOR_FACTORY_CLS_NAME,
            Collections.singletonMap("fInt", (Object)1L));

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return proxy.factory(ctx).create();
            }
        }, IgniteException.class, null);
    }

    /**
     * Test object creation with null class name.
     */
    @Test
    public void testCustomFactoryNullClassName() {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return proxyForCustom(null, null).factory(ctx).create();
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return proxyForCustom(null, new HashMap<String, Object>()).factory(ctx).create();
            }
        }, IgniteException.class, null);
    }

    /**
     * Test object creation with invalid class name.
     */
    @Test
    public void testCustomFactoryInvalidClassName() {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return proxyForCustom("invalid", null).factory(ctx).create();
            }
        }, IgniteException.class, null);
    }

    /**
     * Create proxy for user-defined factory.
     *
     * @param clsName Class name.
     * @param props Properties.
     * @return Proxy.
     */
    private static PlatformJavaObjectFactoryProxy proxyForCustom(String clsName, Map<String, Object> props) {
        return new PlatformJavaObjectFactoryProxy(PlatformJavaObjectFactoryProxy.TYP_USER, clsName, null, props);
    }
}

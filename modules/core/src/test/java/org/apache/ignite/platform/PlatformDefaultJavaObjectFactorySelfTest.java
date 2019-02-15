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

package org.apache.ignite.platform;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.platform.PlatformDefaultJavaObjectFactory;
import org.apache.ignite.platform.javaobject.TestJavaObject;
import org.apache.ignite.platform.javaobject.TestJavaObjectNoDefaultCtor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.junit.Test;

/**
 * Dedicated tests for {@link PlatformDefaultJavaObjectFactory}.
 */
public class PlatformDefaultJavaObjectFactorySelfTest extends GridCommonAbstractTest {
    /** Name of the class. */
    private static final String CLS_NAME = TestJavaObject.class.getName();

    /** Name of the class without default constructor. */
    private static final String NO_DFLT_CTOR_CLS_NAME = TestJavaObjectNoDefaultCtor.class.getName();

    /**
     * Test normal object creation.
     */
    @Test
    public void testNormal() {
        final PlatformDefaultJavaObjectFactory factory = new PlatformDefaultJavaObjectFactory();

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

        factory.initialize(CLS_NAME, props);

        Object val = factory.create();

        TestJavaObject expVal = new TestJavaObject().setBoolean(true).setByte((byte)1).setShort((short)2).setChar('3')
            .setInt(4).setLong(5L).setFloat(6.6f).setDouble(7.7d).setObject(obj).setIntBoxed(10);

        assertEquals(expVal, val);
    }

    /**
     * Test object creation with boxed property.
     */
    @Test
    public void testBoxedProperty() {
        final PlatformDefaultJavaObjectFactory factory = new PlatformDefaultJavaObjectFactory();

        factory.initialize(CLS_NAME, Collections.singletonMap("fIntBoxed", 1));

        Object val = factory.create();

        assertEquals(val, new TestJavaObject().setIntBoxed(1));
    }

    /**
     * Test object creation without properties.
     */
    @Test
    public void testNoProperties() {
        final PlatformDefaultJavaObjectFactory factory = new PlatformDefaultJavaObjectFactory();

        factory.initialize(CLS_NAME, Collections.emptyMap());

        Object val = factory.create();

        assertEquals(val, new TestJavaObject());
    }

    /**
     * Test object creation with invalid property name.
     */
    @Test
    public void testInvalidPropertyName() {
        final PlatformDefaultJavaObjectFactory factory = new PlatformDefaultJavaObjectFactory();

        factory.initialize(CLS_NAME, Collections.singletonMap("invalid", 1));

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return factory.create();
            }
        }, IgniteException.class, null);
    }

    /**
     * Test object creation with invalid property value.
     */
    @Test
    public void testInvalidPropertyValue() {
        final PlatformDefaultJavaObjectFactory factory = new PlatformDefaultJavaObjectFactory();

        factory.initialize(CLS_NAME, Collections.singletonMap("fInt", 1L));

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return factory.create();
            }
        }, IgniteException.class, null);
    }

    /**
     * Test object creation without default constructor.
     */
    @Test
    public void testNoDefaultConstructor() {
        final PlatformDefaultJavaObjectFactory factory = new PlatformDefaultJavaObjectFactory();

        factory.initialize(NO_DFLT_CTOR_CLS_NAME, null);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return factory.create();
            }
        }, IgniteException.class, null);
    }

    /**
     * Test object creation with null class name.
     */
    @Test
    public void testNullClassName() {
        final PlatformDefaultJavaObjectFactory factory = new PlatformDefaultJavaObjectFactory();

        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                factory.initialize(null, null);

                return null;
            }
        }, IgniteException.class, null);

        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                factory.initialize(null, new HashMap<String, Object>());

                return null;
            }
        }, IgniteException.class, null);
    }

    /**
     * Test object creation with invalid class name.
     */
    @Test
    public void testInvalidClassName() {
        final PlatformDefaultJavaObjectFactory factory = new PlatformDefaultJavaObjectFactory();

        factory.initialize("invalid", null);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return factory.create();
            }
        }, IgniteException.class, null);
    }
}

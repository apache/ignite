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

package org.apache.ignite.igfs;

import java.io.Externalizable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * {@link IgfsPath} self test.
 */
public class IgfsPathSelfTest extends GridCommonAbstractTest {
    /** Marshaller to test {@link Externalizable} interface. */
    private final Marshaller marshaller = new OptimizedMarshaller();

    /**
     * Test public methods of igfs path.
     *
     * @throws Exception In case of any exception.
     */
    public void testMethods() throws Exception {
        IgfsPath path = new IgfsPath("/a/s/d/f");

        validateParent("/a/s/d/f", "/a/s/d");
        validateParent("/a/s/d", "/a/s");
        validateParent("/a/s", "/a");
        validateParent("/a", "/");
        validateParent("/", null);

        assertEquals(new IgfsPath("/a/s/d/f-2"), path.suffix("-2"));
        assertEquals(Arrays.asList("a", "s", "d", "f"), path.components());
        assertEquals(4, path.depth());
        assertEquals(3, path.parent().depth());
        assertEquals("f", path.name());

        assertEquals(path, mu(path));

        IgfsPath parent = path.parent();

        assertTrue(path.compareTo(new IgfsPath(parent, "e")) > 0);
        assertTrue(path.compareTo(new IgfsPath(parent, "g")) < 0);
    }

    /**
     * Validate parent resolution is correct.
     *
     * @param child Child path.
     * @param parent Expected parent path.
     */
    private void validateParent(String child, @Nullable String parent) {
        assertEquals(parent == null ? null : new IgfsPath(parent), new IgfsPath(child).parent());
    }

    /**
     * Marshal/unmarshal object.
     *
     * @param obj Object to marshal/unmarshal.
     * @return Marshalled and then unmarshalled object.
     * @throws IgniteCheckedException In case of any marshalling exception.
     */
    private <T> T mu(T obj) throws IgniteCheckedException {
        return marshaller.unmarshal(marshaller.marshal(obj), null);
    }

    /**
     * Test igfs path constructors.
     *
     * @throws Exception In case of any exception.
     */
    @SuppressWarnings("TooBroadScope")
    public void testConstructors() throws Exception {
        String pathStr = "///";
        URI uri = URI.create(pathStr);
        IgfsPath path = new IgfsPath(uri);

        assertNotNull(new IgfsPath(uri));
        assertNotNull(new IgfsPath(pathStr));
        assertNotNull(new IgfsPath("/"));
        assertNotNull(new IgfsPath(path, pathStr));
        assertNotNull(new IgfsPath());

        Class nullUri = URI.class;
        Class nullStr = String.class;
        Class nullPath = IgfsPath.class;

        expectConstructorThrows(NullPointerException.class, nullUri);
        expectConstructorThrows(IllegalArgumentException.class, nullStr);
        expectConstructorThrows(IllegalArgumentException.class, nullStr, nullStr, nullStr);
        expectConstructorThrows(NullPointerException.class, nullPath, nullStr);
        expectConstructorThrows(NullPointerException.class, nullPath, nullUri);

        String name = "with space in path.txt";

        assertEquals(name, new IgfsPath(path, name).name());
    }

    /**
     * Validate igfs path constructor fails with specified exception.
     *
     * @param cls Expected exception.
     * @param args Constructor arguments. Null-values are passed as Class instancies: null-string => String.class.
     */
    private void expectConstructorThrows(Class<? extends Exception> cls, final Object... args) {
        final Class<?>[] paramTypes = new Class[args.length];

        for (int i = 0, size = args.length; i < size; i++) {
            Object arg = args[i];

            if (arg instanceof Class) {
                paramTypes[i] = (Class)arg;
                args[i] = null; // Reset argument of specified type to null.
            }
            else
                paramTypes[i] = arg.getClass();
        }

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    Constructor<IgfsPath> constructor = IgfsPath.class.getConstructor(paramTypes);

                    constructor.newInstance(args);
                }
                catch (InvocationTargetException e) {
                    Throwable cause = e.getCause();

                    // Unwrap invocation target exception.
                    if (cause instanceof Exception)
                        throw (Exception)cause;

                    throw e;
                }

                return null;
            }
        }, cls, null);
    }
}
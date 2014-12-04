/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * {@link GridGgfsPath} self test.
 */
public class GridGgfsPathSelfTest extends GridCommonAbstractTest {
    /** Marshaller to test {@link Externalizable} interface. */
    private final IgniteMarshaller marshaller = new IgniteOptimizedMarshaller();

    /**
     * Test public methods of ggfs path.
     *
     * @throws Exception In case of any exception.
     */
    public void testMethods() throws Exception {
        GridGgfsPath path = new GridGgfsPath("/a/s/d/f");

        validateParent("/a/s/d/f", "/a/s/d");
        validateParent("/a/s/d", "/a/s");
        validateParent("/a/s", "/a");
        validateParent("/a", "/");
        validateParent("/", null);

        assertEquals(new GridGgfsPath("/a/s/d/f-2"), path.suffix("-2"));
        assertEquals(Arrays.asList("a", "s", "d", "f"), path.components());
        assertEquals(4, path.depth());
        assertEquals(3, path.parent().depth());
        assertEquals("f", path.name());

        assertEquals(path, mu(path));

        GridGgfsPath parent = path.parent();

        assertTrue(path.compareTo(new GridGgfsPath(parent, "e")) > 0);
        assertTrue(path.compareTo(new GridGgfsPath(parent, "g")) < 0);
    }

    /**
     * Validate parent resolution is correct.
     *
     * @param child Child path.
     * @param parent Expected parent path.
     */
    private void validateParent(String child, @Nullable String parent) {
        assertEquals(parent == null ? null : new GridGgfsPath(parent), new GridGgfsPath(child).parent());
    }

    /**
     * Marshal/unmarshal object.
     *
     * @param obj Object to marshal/unmarshal.
     * @return Marshalled and then unmarshalled object.
     * @throws GridException In case of any marshalling exception.
     */
    private <T> T mu(T obj) throws GridException {
        return marshaller.unmarshal(marshaller.marshal(obj), null);
    }

    /**
     * Test ggfs path constructors.
     *
     * @throws Exception In case of any exception.
     */
    @SuppressWarnings("TooBroadScope")
    public void testConstructors() throws Exception {
        String pathStr = "///";
        URI uri = URI.create(pathStr);
        GridGgfsPath path = new GridGgfsPath(uri);

        assertNotNull(new GridGgfsPath(uri));
        assertNotNull(new GridGgfsPath(pathStr));
        assertNotNull(new GridGgfsPath("/"));
        assertNotNull(new GridGgfsPath(path, pathStr));
        assertNotNull(new GridGgfsPath());

        Class nullUri = URI.class;
        Class nullStr = String.class;
        Class nullPath = GridGgfsPath.class;

        expectConstructorThrows(NullPointerException.class, nullUri);
        expectConstructorThrows(IllegalArgumentException.class, nullStr);
        expectConstructorThrows(IllegalArgumentException.class, nullStr, nullStr, nullStr);
        expectConstructorThrows(NullPointerException.class, nullPath, nullStr);
        expectConstructorThrows(NullPointerException.class, nullPath, nullUri);

        String name = "with space in path.txt";

        assertEquals(name, new GridGgfsPath(path, name).name());
    }

    /**
     * Validate ggfs path constructor fails with specified exception.
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
                    Constructor<GridGgfsPath> constructor = GridGgfsPath.class.getConstructor(paramTypes);

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

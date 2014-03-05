// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework;

import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Utility classloader that has ability to load classes from external resources.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings({"CustomClassloader"})
public class GridTestExternalClassLoader extends URLClassLoader {
    /** */
    private Set<String> excludeClassNames;

    /** */
    private Map<String, byte[]> resourceMap;

    /** */
    private long timeout;

    /**
     * Constructor.
     * @param urls the URLs from which to load classes and resources.
     * @param excludeClassNames list of excluded classes.
     */
    public GridTestExternalClassLoader(URL[] urls, String... excludeClassNames) {
        this(urls, Collections.<String, byte[]>emptyMap(), excludeClassNames);
    }

    /**
     * Constructor.
     * @param urls the URLs from which to load classes and resources.
     * @param resourceMap mapped resources.
     */
    public GridTestExternalClassLoader(URL[] urls, Map<String, byte[]> resourceMap) {
        this(urls, resourceMap, Collections.<String>emptySet());
    }

    /**
     * Constructor.
     * @param urls the URLs from which to load classes and resources.
     * @param resourceMap Resource map.
     * @param excludeClassNames list of excluded classes.
     */
    public GridTestExternalClassLoader(URL[] urls, Map<String, byte[]> resourceMap, String... excludeClassNames) {
        this(urls, resourceMap, new HashSet<>(Arrays.asList(excludeClassNames)));
    }

    /**
     * Constructor.
     * @param urls the URLs from which to load classes and resources.
     * @param resourceMap Resource map.
     * @param excludeClassNames list of excluded classes.
     */
    public GridTestExternalClassLoader(URL[] urls, Map<String, byte[]> resourceMap, Set<String> excludeClassNames) {
        super(urls, GridTestExternalClassLoader.class.getClassLoader());

        this.excludeClassNames = excludeClassNames;

        assert resourceMap != null;

        this.resourceMap = resourceMap;
    }

    /**
     * Sets set of excluded resource paths.
     * @param excludeClassNames excluded resource paths.
     */
    public void setExcludeClassNames(Set<String> excludeClassNames) {
        this.excludeClassNames = excludeClassNames;
    }

    /**
     * Sets set of excluded resource paths.
     * @param excludeClassNames excluded resource paths.
     */
    public void setExcludeClassNames(String... excludeClassNames) {
        setExcludeClassNames(new HashSet<>(Arrays.asList(excludeClassNames)));
    }

    /**
     * @param timeout Timeout.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * Sleep {@code timeout} period of time.
     */
    private void doTimeout() {
        try {
            Thread.sleep(timeout);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Thread was interrupted", e);
        }
    }

    /**
     * @param resName Resource name.
     * @return Class name.
     */
    private String resNameToClassName(String resName) {
        if (resName.endsWith(".class"))
            resName = resName.substring(0, resName.length() - ".class".length());

        return resName.replace('/', '.');
    }

    /** {@inheritDoc} */
    @Override protected Class<?> findClass(String name) throws ClassNotFoundException {
        for (String s : excludeClassNames)
            if (s.equals(name))
                throw new ClassNotFoundException(name);

        return super.findClass(name);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"NonSynchronizedMethodOverridesSynchronizedMethod"})
    @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (excludeClassNames.contains(name))
            throw new ClassNotFoundException(name);

        return super.loadClass(name, resolve);
    }

    /** {@inheritDoc} */
    @Nullable @Override public URL findResource(String name) {
        if (excludeClassNames.contains(resNameToClassName(name)))
            return null;

        return super.findResource(name);
    }

    /** {@inheritDoc} */
    @Override public InputStream getResourceAsStream(String name) {
        doTimeout();

        byte[] res = resourceMap.get(name);

        return res == null ? super.getResourceAsStream(name) : new ByteArrayInputStream(res);
    }

    /**
     * @param resourceMap mapped resources.
     */
    public void setResourceMap(Map<String, byte[]> resourceMap) {
        this.resourceMap = resourceMap;
    }

    /**
     * Returns an Enumeration of URLs representing all of the resources on the URL search path having the specified name.
     *
     * @param name the resource name.
     * @return an {@code Enumeration} of {@code URL}s.
     * @throws IOException if an I/O exception occurs.
     */
    @Override public Enumeration<URL> findResources(String name) throws IOException {
        if (excludeClassNames.contains(resNameToClassName(name))) {
            return new Enumeration<URL>() {
                @Override public boolean hasMoreElements() {
                    return false;
                }

                @Override public URL nextElement() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        return super.findResources(name);
    }
}

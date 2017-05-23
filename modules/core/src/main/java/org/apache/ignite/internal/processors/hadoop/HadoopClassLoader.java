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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.ClassCache;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Class loader allowing explicitly load classes without delegation to parent class loader.
 * Also supports class parsing for finding dependencies which contain transitive dependencies
 * unavailable for parent.
 */
public class HadoopClassLoader extends URLClassLoader implements ClassCache {
    /** Hadoop class name: Daemon. */
    public static final String CLS_DAEMON = "org.apache.hadoop.util.Daemon";

    /** Hadoop class name: ShutdownHookManager. */
    public static final String CLS_SHUTDOWN_HOOK_MANAGER = "org.apache.hadoop.util.ShutdownHookManager";

    /** Hadoop class name: Daemon replacement. */
    public static final String CLS_DAEMON_REPLACE = "org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopDaemon";

    /** Hadoop class name: ShutdownHookManager replacement. */
    public static final String CLS_SHUTDOWN_HOOK_MANAGER_REPLACE =
        "org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopShutdownHookManager";

    /** */
    private static final URLClassLoader APP_CLS_LDR = (URLClassLoader)HadoopClassLoader.class.getClassLoader();

    /** */
    private static final Collection<URL> appJars = F.asList(APP_CLS_LDR.getURLs());

    /** Mutex for native libraries initialization. */
    private static final Object LIBS_MUX = new Object();

    /** Predefined native libraries to load. */
    private static final Collection<String> PREDEFINED_NATIVE_LIBS;

    /** Native libraries. */
    private static Collection<Object> NATIVE_LIBS;

    /** */
    private static volatile Collection<URL> hadoopJars;

    /** */
    private static final Map<String, byte[]> bytesCache = new ConcurrentHashMap8<>();

    /** Class cache. */
    private final ConcurrentMap<String, Class> cacheMap = new ConcurrentHashMap<>();

    /** Diagnostic name of this class loader. */
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
    private final String name;

    /** Igfs Helper. */
    private final HadoopHelper helper;

    static {
        // We are very parallel capable.
        registerAsParallelCapable();

        PREDEFINED_NATIVE_LIBS = new HashSet<>();

        PREDEFINED_NATIVE_LIBS.add("hadoop");
        PREDEFINED_NATIVE_LIBS.add("MapRClient");
    }

    /**
     * Classloader name for job.
     *
     * @param jobId Job ID.
     * @return Name.
     */
    public static String nameForJob(HadoopJobId jobId) {
        return "hadoop-job-" + jobId;
    }

    /**
     * Gets name for the task class loader. Task class loader
     * @param info The task info.
     * @param prefix Get only prefix (without task type and number)
     * @return The class loader name.
     */
    public static String nameForTask(HadoopTaskInfo info, boolean prefix) {
        if (prefix)
            return "hadoop-task-" + info.jobId() + "-";
        else
            return "hadoop-task-" + info.jobId() + "-" + info.type() + "-" + info.taskNumber();
    }

    /**
     * Constructor.
     *
     * @param urls Urls.
     * @param name Classloader name.
     * @param libNames Optional additional native library names to be linked from parent classloader.
     */
    public HadoopClassLoader(URL[] urls, String name, @Nullable String[] libNames, HadoopHelper helper) {
        super(addHadoopUrls(urls), APP_CLS_LDR);

        assert !(getParent() instanceof HadoopClassLoader);

        this.name = name;
        this.helper = helper;

        initializeNativeLibraries(libNames);
    }

    /**
     * Workaround to load native Hadoop libraries. Java doesn't allow native libraries to be loaded from different
     * classloaders. But we load Hadoop classes many times and one of these classes - {@code NativeCodeLoader} - tries
     * to load the same native library over and over again.
     * <p>
     * To fix the problem, we force native library load in parent class loader and then "link" handle to this native
     * library to our class loader. As a result, our class loader will think that the library is already loaded and will
     * be able to link native methods.
     *
     * @see <a href="http://docs.oracle.com/javase/1.5.0/docs/guide/jni/spec/invocation.html#library_version">
     *     JNI specification</a>
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void initializeNativeLibraries(@Nullable String[] usrLibs) {
        Collection<Object> res;

        synchronized (LIBS_MUX) {
            if (NATIVE_LIBS == null) {
                LinkedList<NativeLibrary> libs = new LinkedList<>();

                for (String lib : PREDEFINED_NATIVE_LIBS)
                    libs.add(new NativeLibrary(lib, true));

                if (!F.isEmpty(usrLibs)) {
                    for (String usrLib : usrLibs)
                        libs.add(new NativeLibrary(usrLib, false));
                }

                NATIVE_LIBS = initializeNativeLibraries0(libs);
            }

            res = NATIVE_LIBS;
        }

        // Link libraries to class loader.
        Vector<Object> ldrLibs = nativeLibraries(this);

        synchronized (ldrLibs) {
            ldrLibs.addAll(res);
        }
    }

    /**
     * Initialize native libraries.
     *
     * @param libs Libraries to initialize.
     * @return Initialized libraries.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private static Collection<Object> initializeNativeLibraries0(Collection<NativeLibrary> libs) {
        assert Thread.holdsLock(LIBS_MUX);

        Collection<Object> res = new HashSet<>();

        for (NativeLibrary lib : libs) {
            String libName = lib.name;

            File libFile = new File(libName);

            try {
                // Load library.
                if (libFile.isAbsolute())
                    System.load(libName);
                else
                    System.loadLibrary(libName);

                // Find library in class loader internals.
                Object libObj = null;

                ClassLoader ldr = APP_CLS_LDR;

                while (ldr != null) {
                    Vector<Object> ldrLibObjs = nativeLibraries(ldr);

                    synchronized (ldrLibObjs) {
                        for (Object ldrLibObj : ldrLibObjs) {
                            String name = nativeLibraryName(ldrLibObj);

                            if (libFile.isAbsolute()) {
                                if (F.eq(name, libFile.getCanonicalPath())) {
                                    libObj = ldrLibObj;

                                    break;
                                }
                            } else {
                                if (name.contains(libName)) {
                                    libObj = ldrLibObj;

                                    break;
                                }
                            }
                        }
                    }

                    if (libObj != null)
                        break;

                    ldr = ldr.getParent();
                }

                if (libObj == null)
                    throw new IgniteException("Failed to find loaded library: " + libName);

                res.add(libObj);
            }
            catch (UnsatisfiedLinkError e) {
                if (!lib.optional)
                    throw e;
            }
            catch (IOException e) {
                throw new IgniteException("Failed to initialize native libraries due to unexpected exception.", e);
            }
        }

        return res;
    }

    /**
     * Get native libraries collection for the given class loader.
     *
     * @param ldr Class loaded.
     * @return Native libraries.
     */
    private static Vector<Object> nativeLibraries(ClassLoader ldr) {
        assert ldr != null;

        return U.field(ldr, "nativeLibraries");
    }

    /**
     * Get native library name.
     *
     * @param lib Library.
     * @return Name.
     */
    private static String nativeLibraryName(Object lib) {
        assert lib != null;

        return U.field(lib, "name");
    }

    /** {@inheritDoc} */
    @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            // Always load Hadoop classes explicitly, since Hadoop can be available in App classpath.
            if (name.equals(CLS_SHUTDOWN_HOOK_MANAGER))  // Dirty hack to get rid of Hadoop shutdown hooks.
                return loadReplace(name, CLS_SHUTDOWN_HOOK_MANAGER_REPLACE);
            else if (name.equals(CLS_DAEMON))
                // We replace this in order to be able to forcibly stop some daemon threads
                // that otherwise never stop (e.g. PeerCache runnables):
                return loadReplace(name, CLS_DAEMON_REPLACE);

            // For Ignite Hadoop and IGFS classes we have to check if they depend on Hadoop.
            if (loadByCurrentClassloader(name))
                return loadClassExplicitly(name, resolve);

            return super.loadClass(name, resolve);
        }
        catch (NoClassDefFoundError | ClassNotFoundException e) {
            throw new ClassNotFoundException("Failed to load class: " + name, e);
        }
    }

    /**
     * Load a class replacing it with our own implementation.
     *
     * @param originalName Name.
     * @param replaceName Replacement.
     * @return Class.
     */
    private Class<?> loadReplace(final String originalName, final String replaceName) {
        synchronized (getClassLoadingLock(originalName)) {
            // First, check if the class has already been loaded
            Class c = findLoadedClass(originalName);

            if (c != null)
                return c;

            byte[] bytes = bytesCache.get(originalName);

            if (bytes == null) {
                InputStream in = helper.loadClassBytes(this, replaceName);

                if (in == null)
                    throw new IgniteException("Failed to replace class [originalName=" + originalName +
                        ", replaceName=" +  replaceName + ']');

                bytes = helper.loadReplace(in, originalName, replaceName);

                bytesCache.put(originalName, bytes);
            }

            return defineClass(originalName, bytes, 0, bytes.length);
        }
    }

    /** {@inheritDoc} */
    @Override public Class<?> getFromCache(String clsName) throws ClassNotFoundException {
        Class<?> cls = cacheMap.get(clsName);

        if (cls == null) {
            Class old = cacheMap.putIfAbsent(clsName, cls = Class.forName(clsName, true, this));

            if (old != null)
                cls = old;
        }

        return cls;
    }

    /**
     * Check whether file must be loaded with current class loader, or normal delegation model should be used.
     * <p>
     * Override is only necessary for Ignite classes which have direct or transitive dependencies on Hadoop classes.
     * These are all classes from "org.apache.ignite.internal.processors.hadoop.impl" package,
     * and these are several well-know classes from "org.apache.ignite.hadoop" package.
     *
     * @param clsName Class name.
     * @return Whether class must be loaded by current classloader without delegation.
     */
    @SuppressWarnings("RedundantIfStatement")
    public static boolean loadByCurrentClassloader(String clsName) {
        // All impl classes.
        if (clsName.startsWith("org.apache.ignite.internal.processors.hadoop.impl"))
            return true;

        // Several classes from public API.
        if (clsName.startsWith("org.apache.ignite.hadoop")) {
            // We use "contains" instead of "equals" to handle subclasses properly.
            if (clsName.contains("org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem") ||
                clsName.contains("org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem") ||
                clsName.contains("org.apache.ignite.hadoop.io.TextPartiallyRawComparator") ||
                clsName.contains("org.apache.ignite.hadoop.mapreduce.IgniteHadoopClientProtocolProvider"))
                return true;
        }

        return false;
    }

    /**
     * @param name Class name.
     * @param resolve Resolve class.
     * @return Class.
     * @throws ClassNotFoundException If failed.
     */
    private Class<?> loadClassExplicitly(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            // First, check if the class has already been loaded
            Class c = findLoadedClass(name);

            if (c == null) {
                long t1 = System.nanoTime();

                c = findClass(name);

                // this is the defining class loader; record the stats
                sun.misc.PerfCounter.getFindClassTime().addElapsedTimeFrom(t1);
                sun.misc.PerfCounter.getFindClasses().increment();
            }

            if (resolve)
                resolveClass(c);

            return c;
        }
    }

    /**
     * @param urls URLs.
     * @return URLs.
     */
    private static URL[] addHadoopUrls(URL[] urls) {
        Collection<URL> hadoopJars;

        try {
            hadoopJars = hadoopUrls();
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }

        ArrayList<URL> list = new ArrayList<>(hadoopJars.size() + appJars.size() + (urls == null ? 0 : urls.length));

        list.addAll(appJars);
        list.addAll(hadoopJars);

        if (!F.isEmpty(urls))
            list.addAll(F.asList(urls));

        return list.toArray(new URL[list.size()]);
    }

    /**
     * @return Collection of jar URLs.
     * @throws IgniteCheckedException If failed.
     */
    public static Collection<URL> hadoopUrls() throws IgniteCheckedException {
        Collection<URL> hadoopUrls = hadoopJars;

        if (hadoopUrls != null)
            return hadoopUrls;

        synchronized (HadoopClassLoader.class) {
            hadoopUrls = hadoopJars;

            if (hadoopUrls != null)
                return hadoopUrls;

            try {
                hadoopUrls = HadoopClasspathUtils.classpathForClassLoader();
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to resolve Hadoop JAR locations: " + e.getMessage(), e);
            }

            hadoopJars = hadoopUrls;

            return hadoopUrls;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopClassLoader.class, this);
    }

    /**
     * Getter for name field.
     */
    public String name() {
        return name;
    }

    /**
     * Native library abstraction.
     */
    private static class NativeLibrary {
        /** Library name. */
        private final String name;

        /** Whether library is optional. */
        private final boolean optional;

        /**
         * Constructor.
         *
         * @param name Library name.
         * @param optional Optional flag.
         */
        public NativeLibrary(String name, boolean optional) {
            this.name = name;
            this.optional = optional;
        }
    }
}
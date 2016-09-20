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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.ClassCache;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Class loader allowing explicitly load classes without delegation to parent class loader.
 * Also supports class parsing for finding dependencies which contain transitive dependencies
 * unavailable for parent.
 */
public class HadoopClassLoader extends URLClassLoader implements ClassCache {
    static {
        // We are very parallel capable.
        registerAsParallelCapable();
    }

    /** Hadoop class name: Daemon. */
    public static final String CLS_DAEMON = "org.apache.hadoop.util.Daemon";

    /** Hadoop class name: ShutdownHookManager. */
    public static final String CLS_SHUTDOWN_HOOK_MANAGER = "org.apache.hadoop.util.ShutdownHookManager";

    /** Hadoop class name: NativeCodeLoader. */
    public static final String CLS_NATIVE_CODE_LOADER = "org.apache.hadoop.util.NativeCodeLoader";

    /** Hadoop class name: Daemon replacement. */
    public static final String CLS_DAEMON_REPLACE = "org.apache.ignite.internal.processors.hadoop.v2.HadoopDaemon";

    /** Hadoop class name: ShutdownHookManager replacement. */
    public static final String CLS_SHUTDOWN_HOOK_MANAGER_REPLACE = "org.apache.ignite.internal.processors.hadoop.v2.HadoopShutdownHookManager";

    /** Name of libhadoop library. */
    private static final String LIBHADOOP = "hadoop.";

    /** */
    private static final URLClassLoader APP_CLS_LDR = (URLClassLoader)HadoopClassLoader.class.getClassLoader();

    /** */
    private static final Collection<URL> appJars = F.asList(APP_CLS_LDR.getURLs());

    /** */
    private static volatile Collection<URL> hadoopJars;

    /** */
    private static final Map<String, byte[]> bytesCache = new ConcurrentHashMap8<>();

    /** Class cache. */
    private final ConcurrentMap<String, Class> cacheMap = new ConcurrentHashMap<>();

    /** Diagnostic name of this class loader. */
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"}) private final String name;

    /** Native library names. */
    private String[] loadedLibNames;

    /** Igfs Helper. */
    private final HadoopHelper helper;

    /**
     * Gets name for Job class loader. The name is specific for local node id.
     *
     * @param locNodeId The local node id.
     * @return The class loader name.
     */
    public static String nameForJob(UUID locNodeId) {
        return "hadoop-job-node-" + locNodeId.toString();
    }

    /**
     * Gets name for the task class loader. Task class loader
     *
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
        assert getClass().getClassLoader() == APP_CLS_LDR; // by definition, app cls loader created in such way.

        this.name = name;
        this.helper = helper;

        // TODO: for POC:
        if (libNames == null)
            libNames = new String[] { "hadoop" };

        setNativeLibrariesToBeInjectedIfNeeded(this, libNames);
    }

    //    /**
    //     * Workaround to load native Hadoop libraries. Java doesn't allow native libraries to be loaded from different
    //     * classloaders. But we load Hadoop classes many times and one of these classes - {@code NativeCodeLoader} - tries

    //     * to load the same native library over and over again.
    //     * <p>
    //     * To fix the problem, we force native library load in parent class loader and then "link" handle to this native
    //     * library to our class loader. As a result, our class loader will think that the library is already loaded and will
    //     * be able to link native methods.
    //     *
    //     * @see <a href="http://docs.oracle.com/javase/1.5.0/docs/guide/jni/spec/invocation.html#library_version">
    //     *     JNI specification</a>
    //     */
    //    private void initializeNativeLibraries() {
    //        try {
    //            // This must trigger native library load.
    //            // TODO: Do not delegate to APP LDR
    //            Class.forName(CLS_NATIVE_CODE_LOADER, true, this);
    //
    //            final Vector<Object> curVector = U.field(this, "nativeLibraries");
    //
    //            // TODO: Do not delegate to APP LDR
    //            ClassLoader ldr = APP_CLS_LDR;
    //
    //            while (ldr != null) {
    //                Vector vector = U.field(ldr, "nativeLibraries");
    //
    //                for (Object lib : vector) {
    //                    String name = U.field(lib, "name");
    //
    //                    boolean add = name.contains(LIBHADOOP);
    //
    //                    if (!add && libNames != null) {
    //                        for (String libName : libNames) {
    //                            if (libName != null && name.contains(libName)) {
    //                                add = true;
    //
    //                                break;
    //                            }
    //                        }
    //                    }
    //
    //                    if (add) {
    //                        curVector.add(lib);
    //
    //                        return;
    //                    }
    //                }
    //
    //                ldr = ldr.getParent();
    //            }
    //        }
    //        catch (Exception e) {
    //            U.quietAndWarn(null, "Failed to initialize Hadoop native library " +
    //                "(native Hadoop methods might not work properly): " + e);
    //        }
    //    }

    /** */
    private static volatile Collection<Object> nativeLibrariesToBeInjected;

    /**
     * This method will be invoked for each created instance of HadoopClassLoader, but the list of native libraries will
     * be loaded only once.
     */
    private static void setNativeLibrariesToBeInjectedIfNeeded(HadoopClassLoader instance, String[] libs) {
        if (libs == null)
            return;

        boolean created = false;

        // 1. If needed, init the native lib data collection:
        if (nativeLibrariesToBeInjected == null) {
            synchronized (HadoopClassLoader.class) {
                if (nativeLibrariesToBeInjected == null) {
                    instance.runLoadingCode(libs);

                    nativeLibrariesToBeInjected = instance.collectNativeLibraries();

                    created = true;
                }
            }
        }

        assert nativeLibrariesToBeInjected != null;

        // 2. Inject libraries:
        if (!created)
            // This is an instance that did not load the libs:
            instance.injectNatives();
    }

    /**
     * Injects previously
     */
    private void injectNatives() {
        try {
            // 2. Init this instance with the natives:
            final Vector<Object> curVector = U.field(this, "nativeLibraries");

            curVector.addAll(nativeLibrariesToBeInjected);
        }
        catch (Exception e) {
            U.quietAndWarn(null, "Failed to initialize Hadoop native library " +
                 "(native Hadoop methods might not work properly): " + e);
        }
    }

    /**
     *
     * @return
     */
    private Collection<Object> collectNativeLibraries() {
        List<Object> target = new ArrayList<>();

        ClassLoader ldr = this;

        while (ldr != null) {
            collectNativeLibrariesFromLoader(ldr, target);

            ldr = ldr.getParent();
        }

        return Collections.unmodifiableList(target);
    }

    /**
     * Run default or user code to force native libs loading:
     */
    private void runLoadingCode(String[] libs) {
        try {
            // TODO: "XXX" is a special class loaded by Hadoop class loader (simulating Hadoop class).
            // NB: this sample class must *not* cause loading of any natives.
            Class<?> sampleCls = this.loadClass(XXX.class.getName(), true);

            assert sampleCls != null;
            assert sampleCls.getClassLoader() == this;

            Collection<String> loadedLibs = new ArrayList<>();

            for (String lib: libs) {
                boolean ok = LoadHelper.tryLoad(sampleCls, lib);

                if (ok)
                    loadedLibs.add(lib);
            }

            loadedLibNames = loadedLibs.toArray(new String[loadedLibs.size()]);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }

    }

    /**
     *
     * @param ldr
     * @param target
     */
    private void collectNativeLibrariesFromLoader(ClassLoader ldr, Collection<Object> target) {
        final Vector vector = U.field(ldr, "nativeLibraries");

        for (Object lib : vector) {
            String name = U.field(lib, "name");

            // TODO: LIBHADOOP should be added implicitly into "libNames"
            boolean addLib = false; //name.contains(LIBHADOOP);

            if (loadedLibNames != null) {
                for (String libName : loadedLibNames) {
                    if (libName != null && name.contains(libName)) {
                        addLib = true;

                        break;
                    }
                }
            }

            if (addLib)
                target.add(lib);
        }
    }


    /** {@inheritDoc} */
    @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            // Always load Hadoop classes explicitly, since Hadoop can be available in App classpath.
            if (helper.isHadoop(name)) {
                if (name.equals(CLS_SHUTDOWN_HOOK_MANAGER))  // Dirty hack to get rid of Hadoop shutdown hooks.
                    return loadReplace(name, CLS_SHUTDOWN_HOOK_MANAGER_REPLACE);
                else if (name.equals(CLS_DAEMON))
                    // We replace this in order to be able to forcibly stop some daemon threads
                    // that otherwise never stop (e.g. PeerCache runnables):
                    return loadReplace(name, CLS_DAEMON_REPLACE);

                return loadClassExplicitly(name, resolve);
            }

            // For Ignite Hadoop and IGFS classes we have to check if they depend on Hadoop.
            if (helper.isHadoopIgfs(name)) {
                if (hasExternalDependencies(name))
                    return loadClassExplicitly(name, resolve);
            }

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
                InputStream in = helper.loadClassBytes(getParent(), replaceName);

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
     * Check whether class has external dependencies on Hadoop.
     *
     * @param clsName Class name.
     * @return {@code True} if class has external dependencies.
     */
    boolean hasExternalDependencies(String clsName) {
        return helper.hasExternalDependencies(clsName, getParent());
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
}
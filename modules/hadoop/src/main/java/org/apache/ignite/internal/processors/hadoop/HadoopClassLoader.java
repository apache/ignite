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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.v2.HadoopDaemon;
import org.apache.ignite.internal.processors.hadoop.v2.HadoopNativeCodeLoader;
import org.apache.ignite.internal.processors.hadoop.v2.HadoopShutdownHookManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingClassAdapter;

/**
 * Class loader allowing explicitly load classes without delegation to parent class loader.
 * Also supports class parsing for finding dependencies which contain transitive dependencies
 * unavailable for parent.
 */
public class HadoopClassLoader extends URLClassLoader {
    /**
     * We are very parallel capable.
     */
    static {
        registerAsParallelCapable();
    }

    /** Name of the Hadoop daemon class. */
    public static final String HADOOP_DAEMON_CLASS_NAME = "org.apache.hadoop.util.Daemon";

    /** */
    private static final URLClassLoader APP_CLS_LDR = (URLClassLoader)HadoopClassLoader.class.getClassLoader();

    /** */
    private static final Collection<URL> appJars = F.asList(APP_CLS_LDR.getURLs());

    /** */
    private static volatile Collection<URL> hadoopJars;

    /** */
    private static final Map<String, Boolean> cache = new ConcurrentHashMap8<>();

    /** */
    private static final Map<String, byte[]> bytesCache = new ConcurrentHashMap8<>();

    /** Diagnostic name of this class loader. */
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
    private final String name;

    /**
     * Gets name for Job class loader. The name is specific for local node id.
     * @param locNodeId The local node id.
     * @return The class loader name.
     */
    public static String nameForJob(UUID locNodeId) {
        return "hadoop-job-node-" + locNodeId.toString();
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
     * @param urls Urls.
     */
    public HadoopClassLoader(URL[] urls, String name) {
        super(addHadoopUrls(urls), APP_CLS_LDR);

        assert !(getParent() instanceof HadoopClassLoader);

        this.name = name;
    }

    /**
     * Need to parse only Ignite Hadoop and IGFS classes.
     *
     * @param cls Class name.
     * @return {@code true} if we need to check this class.
     */
    private static boolean isHadoopIgfs(String cls) {
        String ignitePackagePrefix = "org.apache.ignite";
        int len = ignitePackagePrefix.length();

        return cls.startsWith(ignitePackagePrefix) && (cls.indexOf("igfs.", len) != -1 || cls.indexOf(".fs.", len) != -1 || cls.indexOf("hadoop.", len) != -1);
    }

    /**
     * @param cls Class name.
     * @return {@code true} If this is Hadoop class.
     */
    private static boolean isHadoop(String cls) {
        return cls.startsWith("org.apache.hadoop.");
    }

    /** {@inheritDoc} */
    @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            if (isHadoop(name)) { // Always load Hadoop classes explicitly, since Hadoop can be available in App classpath.
                if (name.endsWith(".util.ShutdownHookManager"))  // Dirty hack to get rid of Hadoop shutdown hooks.
                    return loadFromBytes(name, HadoopShutdownHookManager.class.getName());
                else if (name.endsWith(".util.NativeCodeLoader"))
                    return loadFromBytes(name, HadoopNativeCodeLoader.class.getName());
                else if (name.equals(HADOOP_DAEMON_CLASS_NAME))
                    // We replace this in order to be able to forcibly stop some daemon threads
                    // that otherwise never stop (e.g. PeerCache runnables):
                    return loadFromBytes(name, HadoopDaemon.class.getName());

                return loadClassExplicitly(name, resolve);
            }

            if (isHadoopIgfs(name)) { // For Ignite Hadoop and IGFS classes we have to check if they depend on Hadoop.
                Boolean hasDeps = cache.get(name);

                if (hasDeps == null) {
                    hasDeps = hasExternalDependencies(name, new HashSet<String>());

                    cache.put(name, hasDeps);
                }

                if (hasDeps)
                    return loadClassExplicitly(name, resolve);
            }

            return super.loadClass(name, resolve);
        }
        catch (NoClassDefFoundError | ClassNotFoundException e) {
            throw new ClassNotFoundException("Failed to load class: " + name, e);
        }
    }

    /**
     * @param name Name.
     * @param replace Replacement.
     * @return Class.
     */
    private Class<?> loadFromBytes(final String name, final String replace) {
        synchronized (getClassLoadingLock(name)) {
            // First, check if the class has already been loaded
            Class c = findLoadedClass(name);

            if (c != null)
                return c;

            byte[] bytes = bytesCache.get(name);

            if (bytes == null) {
                InputStream in = loadClassBytes(getParent(), replace);

                ClassReader rdr;

                try {
                    rdr = new ClassReader(in);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }

                ClassWriter w = new ClassWriter(Opcodes.ASM4);

                rdr.accept(new RemappingClassAdapter(w, new Remapper() {
                    /** */
                    String replaceType = replace.replace('.', '/');

                    /** */
                    String nameType = name.replace('.', '/');

                    @Override public String map(String type) {
                        if (type.equals(replaceType))
                            return nameType;

                        return type;
                    }
                }), ClassReader.EXPAND_FRAMES);

                bytes = w.toByteArray();

                bytesCache.put(name, bytes);
            }

            return defineClass(name, bytes, 0, bytes.length);
        }
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
     * @param ldr Loader.
     * @param clsName Class.
     * @return Input stream.
     */
    @Nullable private InputStream loadClassBytes(ClassLoader ldr, String clsName) {
        return ldr.getResourceAsStream(clsName.replace('.', '/') + ".class");
    }

    /**
     * @param clsName Class name.
     * @return {@code true} If the class has external dependencies.
     */
    boolean hasExternalDependencies(final String clsName, final Set<String> visited) {
        if (isHadoop(clsName)) // Hadoop must not be in classpath but Idea sucks, so filtering explicitly as external.
            return true;

        // Try to get from parent to check if the type accessible.
        InputStream in = loadClassBytes(getParent(), clsName);

        if (in == null) // The class is external itself, it must be loaded from this class loader.
            return true;

        if (!isHadoopIgfs(clsName)) // Other classes should not have external dependencies.
            return false;

        final ClassReader rdr;

        try {
            rdr = new ClassReader(in);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read class: " + clsName, e);
        }

        visited.add(clsName);

        final AtomicBoolean hasDeps = new AtomicBoolean();

        rdr.accept(new ClassVisitor(Opcodes.ASM4) {
            AnnotationVisitor av = new AnnotationVisitor(Opcodes.ASM4) {
                // TODO
            };

            FieldVisitor fv = new FieldVisitor(Opcodes.ASM4) {
                @Override public AnnotationVisitor visitAnnotation(String desc, boolean b) {
                    onType(desc);

                    return av;
                }
            };

            MethodVisitor mv = new MethodVisitor(Opcodes.ASM4) {
                @Override public AnnotationVisitor visitAnnotation(String desc, boolean b) {
                    onType(desc);

                    return av;
                }

                @Override public AnnotationVisitor visitParameterAnnotation(int i, String desc, boolean b) {
                    onType(desc);

                    return av;
                }

                @Override public AnnotationVisitor visitAnnotationDefault() {
                    return av;
                }

                @Override public void visitFieldInsn(int i, String owner, String name, String desc) {
                    onType(owner);
                    onType(desc);
                }

                @Override public void visitFrame(int i, int i2, Object[] locTypes, int i3, Object[] stackTypes) {
                    for (Object o : locTypes) {
                        if (o instanceof String)
                            onType((String)o);
                    }

                    for (Object o : stackTypes) {
                        if (o instanceof String)
                            onType((String)o);
                    }
                }

                @Override public void visitLocalVariable(String name, String desc, String signature, Label lb,
                    Label lb2, int i) {
                    onType(desc);
                }

                @Override public void visitMethodInsn(int i, String owner, String name, String desc) {
                    onType(owner);
                }

                @Override public void visitMultiANewArrayInsn(String desc, int dim) {
                    onType(desc);
                }

                @Override public void visitTryCatchBlock(Label lb, Label lb2, Label lb3, String e) {
                    onType(e);
                }
            };

            void onClass(String depCls) {
                assert validateClassName(depCls) : depCls;

                if (depCls.startsWith("java.")) // Filter out platform classes.
                    return;

                if (visited.contains(depCls))
                    return;

                Boolean res = cache.get(depCls);

                if (res == Boolean.TRUE || (res == null && hasExternalDependencies(depCls, visited)))
                    hasDeps.set(true);
            }

            void onType(String type) {
                if (type == null)
                    return;

                int off = 0;

                while (type.charAt(off) == '[')
                    off++; // Handle arrays.

                if (off != 0)
                    type = type.substring(off);

                if (type.length() == 1)
                    return; // Get rid of primitives.

                if (type.charAt(type.length() - 1) == ';') {
                    assert type.charAt(0) == 'L' : type;

                    type = type.substring(1, type.length() - 1);
                }

                type = type.replace('/', '.');

                onClass(type);
            }

            @Override public void visit(int i, int i2, String name, String signature, String superName,
                String[] ifaces) {
                onType(superName);

                if (ifaces != null) {
                    for (String iface : ifaces)
                        onType(iface);
                }
            }

            @Override public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
                onType(desc);

                return av;
            }

            @Override public void visitInnerClass(String name, String outerName, String innerName, int i) {
                onType(name);
            }

            @Override public FieldVisitor visitField(int i, String name, String desc, String signature, Object val) {
                onType(desc);

                return fv;
            }

            @Override public MethodVisitor visitMethod(int i, String name, String desc, String signature,
                String[] exceptions) {
                if (exceptions != null) {
                    for (String e : exceptions)
                        onType(e);
                }

                return mv;
            }
        }, 0);

        if (hasDeps.get()) // We already know that we have dependencies, no need to check parent.
            return true;

        // Here we are known to not have any dependencies but possibly we have a parent which have them.
        int idx = clsName.lastIndexOf('$');

        if (idx == -1) // No parent class.
            return false;

        String parentCls = clsName.substring(0, idx);

        if (visited.contains(parentCls))
            return false;

        Boolean res = cache.get(parentCls);

        if (res == null)
            res = hasExternalDependencies(parentCls, visited);

        return res;
    }

    /**
     * @param name Class name.
     * @return {@code true} If this is a valid class name.
     */
    private static boolean validateClassName(String name) {
        int len = name.length();

        if (len <= 1)
            return false;

        if (!Character.isJavaIdentifierStart(name.charAt(0)))
            return false;

        boolean hasDot = false;

        for (int i = 1; i < len; i++) {
            char c = name.charAt(i);

            if (c == '.')
                hasDot = true;
            else if (!Character.isJavaIdentifierPart(c))
                return false;
        }

        return hasDot;
    }

    /**
     * @param name Variable name.
     * @param dflt Default.
     * @return Value.
     */
    private static String getEnv(String name, String dflt) {
        String res = System.getProperty(name);

        if (F.isEmpty(res))
            res = System.getenv(name);

        return F.isEmpty(res) ? dflt : res;
    }

    /**
     * @param res Result.
     * @param dir Directory.
     * @param startsWith Starts with prefix.
     * @throws MalformedURLException If failed.
     */
    private static void addUrls(Collection<URL> res, File dir, final String startsWith) throws Exception {
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return startsWith == null || name.startsWith(startsWith);
            }
        });

        if (files == null)
            throw new IOException("Path is not a directory: " + dir);

        for (File file : files)
            res.add(file.toURI().toURL());
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
     * @return HADOOP_HOME Variable.
     */
    @Nullable public static String hadoopHome() {
        return getEnv("HADOOP_PREFIX", getEnv("HADOOP_HOME", null));
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

            hadoopUrls = new ArrayList<>();

            String hadoopPrefix = hadoopHome();

            if (F.isEmpty(hadoopPrefix))
                throw new IgniteCheckedException("Failed resolve Hadoop installation location. Either HADOOP_PREFIX or " +
                    "HADOOP_HOME environment variables must be set.");

            String commonHome = getEnv("HADOOP_COMMON_HOME", hadoopPrefix + "/share/hadoop/common");
            String hdfsHome = getEnv("HADOOP_HDFS_HOME", hadoopPrefix + "/share/hadoop/hdfs");
            String mapredHome = getEnv("HADOOP_MAPRED_HOME", hadoopPrefix + "/share/hadoop/mapreduce");

            try {
                addUrls(hadoopUrls, new File(commonHome + "/lib"), null);
                addUrls(hadoopUrls, new File(hdfsHome + "/lib"), null);
                addUrls(hadoopUrls, new File(mapredHome + "/lib"), null);

                addUrls(hadoopUrls, new File(hdfsHome), "hadoop-hdfs-");

                addUrls(hadoopUrls, new File(commonHome), "hadoop-common-");
                addUrls(hadoopUrls, new File(commonHome), "hadoop-auth-");
                addUrls(hadoopUrls, new File(commonHome + "/lib"), "hadoop-auth-");

                addUrls(hadoopUrls, new File(mapredHome), "hadoop-mapreduce-client-common");
                addUrls(hadoopUrls, new File(mapredHome), "hadoop-mapreduce-client-core");
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
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
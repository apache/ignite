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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.v2.HadoopDaemon;
import org.apache.ignite.internal.processors.hadoop.v2.HadoopShutdownHookManager;
import org.apache.ignite.internal.util.ClassCache;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingClassAdapter;

/**
 * Class loader allowing explicitly load classes without delegation to parent class loader.
 * Also supports class parsing for finding dependencies which contain transitive dependencies
 * unavailable for parent.
 */
public class HadoopClassLoader extends URLClassLoader implements ClassCache {
    /**
     * We are very parallel capable.
     */
    static {
        registerAsParallelCapable();
    }

    /** Name of the Hadoop daemon class. */
    public static final String HADOOP_DAEMON_CLASS_NAME = "org.apache.hadoop.util.Daemon";

    /** Name of libhadoop library. */
    private static final String LIBHADOOP = "hadoop.";

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

    /** Class cache. */
    private final ConcurrentMap<String, Class> cacheMap = new ConcurrentHashMap<>();

    /** Diagnostic name of this class loader. */
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
    private final String name;

    /** Native library names. */
    private final String[] libNames;

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
     * Constructor.
     *
     * @param urls Urls.
     * @param name Classloader name.
     * @param libNames Optional additional native library names to be linked from parent classloader.
     */
    public HadoopClassLoader(URL[] urls, String name, @Nullable String[] libNames) {
        super(addHadoopUrls(urls), APP_CLS_LDR);

        assert !(getParent() instanceof HadoopClassLoader);

        this.name = name;
        this.libNames = libNames;

        initializeNativeLibraries();
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
    private void initializeNativeLibraries() {
        try {
            // This must trigger native library load.
            Class.forName(NativeCodeLoader.class.getName(), true, APP_CLS_LDR);

            final Vector<Object> curVector = U.field(this, "nativeLibraries");

            ClassLoader ldr = APP_CLS_LDR;

            while (ldr != null) {
                Vector vector = U.field(ldr, "nativeLibraries");

                for (Object lib : vector) {
                    String name = U.field(lib, "name");

                    boolean add = name.contains(LIBHADOOP);

                    if (!add && libNames != null) {
                        for (String libName : libNames) {
                            if (libName != null && name.contains(libName)) {
                                add = true;

                                break;
                            }
                        }
                    }

                    if (add) {
                        curVector.add(lib);

                        return;
                    }
                }

                ldr = ldr.getParent();
            }
        }
        catch (Exception e) {
            U.quietAndWarn(null, "Failed to initialize Hadoop native library " +
                "(native Hadoop methods might not work properly): " + e);
        }
    }

    /**
     * Need to parse only Ignite Hadoop and IGFS classes.
     *
     * @param cls Class name.
     * @return {@code true} if we need to check this class.
     */
    private static boolean isHadoopIgfs(String cls) {
        String ignitePkgPrefix = "org.apache.ignite";

        int len = ignitePkgPrefix.length();

        return cls.startsWith(ignitePkgPrefix) && (
            cls.indexOf("igfs.", len) != -1 ||
            cls.indexOf(".fs.", len) != -1 ||
            cls.indexOf("hadoop.", len) != -1);
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
                else if (name.equals(HADOOP_DAEMON_CLASS_NAME))
                    // We replace this in order to be able to forcibly stop some daemon threads
                    // that otherwise never stop (e.g. PeerCache runnables):
                    return loadFromBytes(name, HadoopDaemon.class.getName());

                return loadClassExplicitly(name, resolve);
            }

            if (isHadoopIgfs(name)) { // For Ignite Hadoop and IGFS classes we have to check if they depend on Hadoop.
                Boolean hasDeps = cache.get(name);

                if (hasDeps == null) {
                    hasDeps = hasExternalDependencies(name);

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
     * @param ldr Loader.
     * @param clsName Class.
     * @return Input stream.
     */
    @Nullable private InputStream loadClassBytes(ClassLoader ldr, String clsName) {
        return ldr.getResourceAsStream(clsName.replace('.', '/') + ".class");
    }

    /**
     * Check whether class has external dependencies on Hadoop.
     *
     * @param clsName Class name.
     * @return {@code True} if class has external dependencies.
     */
    boolean hasExternalDependencies(String clsName) {
        CollectingContext ctx = new CollectingContext();

        ctx.annVisitor = new CollectingAnnotationVisitor(ctx);
        ctx.mthdVisitor = new CollectingMethodVisitor(ctx, ctx.annVisitor);
        ctx.fldVisitor = new CollectingFieldVisitor(ctx, ctx.annVisitor);
        ctx.clsVisitor = new CollectingClassVisitor(ctx, ctx.annVisitor, ctx.mthdVisitor, ctx.fldVisitor);

        return hasExternalDependencies(clsName, ctx);
    }

    /**
     * Check whether class has external dependencies on Hadoop.
     *
     * @param clsName Class name.
     * @param ctx Context.
     * @return {@code true} If the class has external dependencies.
     */
    boolean hasExternalDependencies(String clsName, CollectingContext ctx) {
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

        ctx.visited.add(clsName);

        rdr.accept(ctx.clsVisitor, 0);

        if (ctx.found) // We already know that we have dependencies, no need to check parent.
            return true;

        // Here we are known to not have any dependencies but possibly we have a parent which has them.
        int idx = clsName.lastIndexOf('$');

        if (idx == -1) // No parent class.
            return false;

        String parentCls = clsName.substring(0, idx);

        if (ctx.visited.contains(parentCls))
            return false;

        Boolean res = cache.get(parentCls);

        if (res == null)
            res = hasExternalDependencies(parentCls, ctx);

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
                hadoopUrls = HadoopClasspathUtils.classpathUrls();
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
     * Context for dependencies collection.
     */
    private class CollectingContext {
        /** Visited classes. */
        private final Set<String> visited = new HashSet<>();

        /** Whether dependency found. */
        private boolean found;

        /** Annotation visitor. */
        private AnnotationVisitor annVisitor;

        /** Method visitor. */
        private MethodVisitor mthdVisitor;

        /** Field visitor. */
        private FieldVisitor fldVisitor;

        /** Class visitor. */
        private ClassVisitor clsVisitor;

        /**
         * Processes a method descriptor
         * @param methDesc The method desc String.
         */
        void onMethodsDesc(final String methDesc) {
            // Process method return type:
            onType(Type.getReturnType(methDesc));

            if (found)
                return;

            // Process method argument types:
            for (Type t: Type.getArgumentTypes(methDesc)) {
                onType(t);

                if (found)
                    return;
            }
        }

        /**
         * Processes dependencies of a class.
         *
         * @param depCls The class name as dot-notated FQN.
         */
        void onClass(final String depCls) {
            assert depCls.indexOf('/') == -1 : depCls; // class name should be fully converted to dot notation.
            assert depCls.charAt(0) != 'L' : depCls;
            assert validateClassName(depCls) : depCls;

            if (depCls.startsWith("java.") || depCls.startsWith("javax.")) // Filter out platform classes.
                return;

            if (visited.contains(depCls))
                return;

            Boolean res = cache.get(depCls);

            if (res == Boolean.TRUE || (res == null && hasExternalDependencies(depCls, this)))
                found = true;
        }

        /**
         * Analyses dependencies of given type.
         *
         * @param t The type to process.
         */
        void onType(Type t) {
            if (t == null)
                return;

            int sort = t.getSort();

            switch (sort) {
                case Type.ARRAY:
                    onType(t.getElementType());

                    break;

                case Type.OBJECT:
                    onClass(t.getClassName());

                    break;
            }
        }

        /**
         * Analyses dependencies of given object type.
         *
         * @param objType The object type to process.
         */
        void onInternalTypeName(String objType) {
            if (objType == null)
                return;

            assert objType.length() > 1 : objType;

            if (objType.charAt(0) == '[')
                // handle array. In this case this is a type descriptor notation, like "[Ljava/lang/Object;"
                onType(objType);
            else {
                assert objType.indexOf('.') == -1 : objType; // Must be slash-separated FQN.

                String clsName = objType.replace('/', '.'); // Convert it to dot notation.

                onClass(clsName); // Process.
            }
        }

        /**
         * Type description analyser.
         *
         * @param desc The description.
         */
        void onType(String desc) {
            if (!F.isEmpty(desc)) {
                if (desc.length() <= 1)
                    return; // Optimization: filter out primitive types in early stage.

                Type t = Type.getType(desc);

                onType(t);
            }
        }
    }

    /**
     * Annotation visitor.
     */
    private static class CollectingAnnotationVisitor extends AnnotationVisitor {
        /** */
        final CollectingContext ctx;

        /**
         * Annotation visitor.
         *
         * @param ctx The collector.
         */
        CollectingAnnotationVisitor(CollectingContext ctx) {
            super(Opcodes.ASM4);

            this.ctx = ctx;
        }

        /** {@inheritDoc} */
        @Override public AnnotationVisitor visitAnnotation(String name, String desc) {
            if (ctx.found)
                return null;

            ctx.onType(desc);

            return this;
        }

        /** {@inheritDoc} */
        @Override public void visitEnum(String name, String desc, String val) {
            if (ctx.found)
                return;

            ctx.onType(desc);
        }

        /** {@inheritDoc} */
        @Override public AnnotationVisitor visitArray(String name) {
            return ctx.found ? null : this;
        }

        /** {@inheritDoc} */
        @Override public void visit(String name, Object val) {
            if (ctx.found)
                return;

            if (val instanceof Type)
                ctx.onType((Type)val);
        }

        /** {@inheritDoc} */
        @Override public void visitEnd() {
            // No-op.
        }
    }

    /**
     * Field visitor.
     */
    private static class CollectingFieldVisitor extends FieldVisitor {
        /** Collector. */
        private final CollectingContext ctx;

        /** Annotation visitor. */
        private final AnnotationVisitor av;

        /**
         * Constructor.
         */
        CollectingFieldVisitor(CollectingContext ctx, AnnotationVisitor av) {
            super(Opcodes.ASM4);

            this.ctx = ctx;
            this.av = av;
        }

        /** {@inheritDoc} */
        @Override public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            if (ctx.found)
                return null;

            ctx.onType(desc);

            return ctx.found ? null : av;
        }

        /** {@inheritDoc} */
        @Override public void visitAttribute(Attribute attr) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void visitEnd() {
            // No-op.
        }
    }

    /**
     * Class visitor.
     */
    private static class CollectingClassVisitor extends ClassVisitor {
        /** Collector. */
        private final CollectingContext ctx;

        /** Annotation visitor. */
        private final AnnotationVisitor av;

        /** Method visitor. */
        private final MethodVisitor mv;

        /** Field visitor. */
        private final FieldVisitor fv;

        /**
         * Constructor.
         *
         * @param ctx Collector.
         * @param av Annotation visitor.
         * @param mv Method visitor.
         * @param fv Field visitor.
         */
        CollectingClassVisitor(CollectingContext ctx, AnnotationVisitor av, MethodVisitor mv, FieldVisitor fv) {
            super(Opcodes.ASM4);

            this.ctx = ctx;
            this.av = av;
            this.mv = mv;
            this.fv = fv;
        }

        /** {@inheritDoc} */
        @Override public void visit(int i, int i2, String name, String signature, String superName, String[] ifaces) {
            if (ctx.found)
                return;

            ctx.onInternalTypeName(superName);

            if (ctx.found)
                return;

            if (ifaces != null) {
                for (String iface : ifaces) {
                    ctx.onInternalTypeName(iface);

                    if (ctx.found)
                        return;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            if (ctx.found)
                return null;

            ctx.onType(desc);

            return ctx.found ? null : av;
        }

        /** {@inheritDoc} */
        @Override public void visitInnerClass(String name, String outerName, String innerName, int i) {
            if (ctx.found)
                return;

            ctx.onInternalTypeName(name);
        }

        /** {@inheritDoc} */
        @Override public FieldVisitor visitField(int i, String name, String desc, String signature, Object val) {
            if (ctx.found)
                return null;

            ctx.onType(desc);

            return ctx.found ? null : fv;
        }

        /** {@inheritDoc} */
        @Override public MethodVisitor visitMethod(int i, String name, String desc, String signature,
            String[] exceptions) {
            if (ctx.found)
                return null;

            ctx.onMethodsDesc(desc);

            // Process declared method exceptions:
            if (exceptions != null) {
                for (String e : exceptions)
                    ctx.onInternalTypeName(e);
            }

            return ctx.found ? null : mv;
        }
    }

    /**
     * Method visitor.
     */
    private static class CollectingMethodVisitor extends MethodVisitor {
        /** Collector. */
        private final CollectingContext ctx;

        /** Annotation visitor. */
        private final AnnotationVisitor av;

        /**
         * Constructor.
         *
         * @param ctx Collector.
         * @param av Annotation visitor.
         */
        private CollectingMethodVisitor(CollectingContext ctx, AnnotationVisitor av) {
            super(Opcodes.ASM4);

            this.ctx = ctx;
            this.av = av;
        }

        /** {@inheritDoc} */
        @Override public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            if (ctx.found)
                return null;

            ctx.onType(desc);

            return ctx.found ? null : av;
        }

        /** {@inheritDoc} */
        @Override public AnnotationVisitor visitParameterAnnotation(int i, String desc, boolean b) {
            if (ctx.found)
                return null;

            ctx.onType(desc);

            return ctx.found ? null : av;
        }

        /** {@inheritDoc} */
        @Override public AnnotationVisitor visitAnnotationDefault() {
            return ctx.found ? null : av;
        }

        /** {@inheritDoc} */
        @Override public void visitFieldInsn(int opcode, String owner, String name, String desc) {
            if (ctx.found)
                return;

            ctx.onInternalTypeName(owner);

            if (ctx.found)
                return;

            ctx.onType(desc);
        }

        /** {@inheritDoc} */
        @Override public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void visitFrame(int type, int nLoc, Object[] locTypes, int nStack, Object[] stackTypes) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void visitLocalVariable(String name, String desc, String signature, Label lb,
            Label lb2, int i) {
            if (ctx.found)
                return;

            ctx.onType(desc);
        }

        /** {@inheritDoc} */
        @Override public void visitMethodInsn(int i, String owner, String name, String desc) {
            if (ctx.found)
                return;

            ctx.onInternalTypeName(owner);

            if (ctx.found)
                return;

            ctx.onMethodsDesc(desc);
        }

        /** {@inheritDoc} */
        @Override public void visitMultiANewArrayInsn(String desc, int dim) {
            if (ctx.found)
                return;

            ctx.onType(desc);
        }

        /** {@inheritDoc} */
        @Override public void visitTryCatchBlock(Label start, Label end, Label hndl, String typeStr) {
            if (ctx.found)
                return;

            ctx.onInternalTypeName(typeStr);
        }

        /** {@inheritDoc} */
        @Override public void visitTypeInsn(int opcode, String type) {
            if (ctx.found)
                return;

            ctx.onInternalTypeName(type);
        }
    }
}
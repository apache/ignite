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
package org.apache.ignite.internal.processors.hadoop.common;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.hadoop.HadoopClassLoader;
import org.apache.ignite.internal.processors.hadoop.HadoopHelper;
import org.apache.ignite.internal.util.typedef.F;
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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods for Hadoop classloader required to avoid direct 3rd-party dependencies in class loader.
 */
public class HadoopHelperImpl implements HadoopHelper {
    /** Cache for resolved dependency info. */
    private static final Map<String, Boolean> dependenciesCache = new ConcurrentHashMap8<>();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Common class loader. */
    private volatile HadoopClassLoader ldr;

    /**
     * Default constructor.
     */
    public HadoopHelperImpl() {
        this(null);
    }

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public HadoopHelperImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public HadoopClassLoader commonClassLoader() {
        HadoopClassLoader res = ldr;

        if (res == null) {
            synchronized (this) {
                res = ldr;

                if (res == null) {
                    String[] libNames = null;

                    if (ctx != null && ctx.config().getHadoopConfiguration() != null)
                        libNames = ctx.config().getHadoopConfiguration().getNativeLibraryNames();

                    res = new HadoopClassLoader(null, "hadoop-common", libNames, this);

                    ldr = res;
                }
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public byte[] loadReplace(InputStream in, final String originalName, final String replaceName) {
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
            String replaceType = replaceName.replace('.', '/');

            /** */
            String nameType = originalName.replace('.', '/');

            @Override public String map(String type) {
                if (type.equals(replaceType))
                    return nameType;

                return type;
            }
        }), ClassReader.EXPAND_FRAMES);

        return w.toByteArray();
    }

    /** {@inheritDoc} */
    @Override public boolean isHadoop(String cls) {
        return cls.startsWith("org.apache.hadoop.");
    }

    /** {@inheritDoc} */
    @Override public boolean isHadoopIgfs(String cls) {
        String ignitePkgPrefix = "org.apache.ignite";

        int len = ignitePkgPrefix.length();

        return cls.startsWith(ignitePkgPrefix) && (
            cls.indexOf("igfs.", len) != -1 ||
                cls.indexOf(".fs.", len) != -1 ||
                cls.indexOf("hadoop.", len) != -1);
    }

    /** {@inheritDoc} */
    @Override @Nullable public InputStream loadClassBytes(ClassLoader ldr, String clsName) {
        return ldr.getResourceAsStream(clsName.replace('.', '/') + ".class");
    }

    /** {@inheritDoc} */
    @Override public boolean hasExternalDependencies(String clsName, ClassLoader parentClsLdr) {
        Boolean hasDeps = dependenciesCache.get(clsName);

        if (hasDeps == null) {
            CollectingContext ctx = new CollectingContext(parentClsLdr);

            ctx.annVisitor = new CollectingAnnotationVisitor(ctx);
            ctx.mthdVisitor = new CollectingMethodVisitor(ctx, ctx.annVisitor);
            ctx.fldVisitor = new CollectingFieldVisitor(ctx, ctx.annVisitor);
            ctx.clsVisitor = new CollectingClassVisitor(ctx, ctx.annVisitor, ctx.mthdVisitor, ctx.fldVisitor);

            hasDeps = hasExternalDependencies(clsName, parentClsLdr, ctx);

            dependenciesCache.put(clsName, hasDeps);
        }

        return hasDeps;
    }

    /**
     * Check whether class has external dependencies on Hadoop.
     *
     * @param clsName Class name.
     * @param parentClsLdr Parent class loader.
     * @param ctx Context.
     * @return {@code true} If the class has external dependencies.
     */
    private boolean hasExternalDependencies(String clsName, ClassLoader parentClsLdr, CollectingContext ctx) {
        if (isHadoop(clsName)) // Hadoop must not be in classpath but Idea sucks, so filtering explicitly as external.
            return true;

        // Try to get from parent to check if the type accessible.
        InputStream in = loadClassBytes(parentClsLdr, clsName);

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

        Boolean res = dependenciesCache.get(parentCls);

        if (res == null)
            res = hasExternalDependencies(parentCls, parentClsLdr, ctx);

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
     * Context for dependencies collection.
     */
    private class CollectingContext {
        /** Visited classes. */
        private final Set<String> visited = new HashSet<>();

        /** Parent class loader. */
        private final ClassLoader parentClsLdr;

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
         * Constrcutor.
         *
         * @param parentClsLdr Parent class loader.
         */
        private CollectingContext(ClassLoader parentClsLdr) {
            this.parentClsLdr = parentClsLdr;
        }

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

            Boolean res = dependenciesCache.get(depCls);

            if (res == Boolean.TRUE || (res == null && hasExternalDependencies(depCls, parentClsLdr, this)))
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

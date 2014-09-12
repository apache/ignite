package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.util.typedef.X;
import org.springframework.asm.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class loader allowing explicitly load classes without delegation to parent class loader.
 * Also supports class parsing for finding dependencies which contain transitive dependencies
 * unavailable for parent.
 */
public class GridHadoopClassLoader0 extends URLClassLoader {
    /** */
    public static final String GRIDGAIN_KERNAL_PKG = "org.gridgain.grid.kernal.";

    /** */
    private final Map<String, Boolean> cache;

    /**
     * @param urls   Urls.
     * @param parent Parent class loader.
     * @param cache Type cache.
     */
    public GridHadoopClassLoader0(URL[] urls, ClassLoader parent, Map<String, Boolean> cache) {
        super(urls, parent);

        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (name.startsWith(GRIDGAIN_KERNAL_PKG)) {
            Boolean res = cache.get(name);

            if (res == Boolean.TRUE || (res == null && hasExternalDependencies(name, new HashSet<String>())))
                return loadClassDirect0(name, resolve);
        }

        return super.loadClass(name, resolve);
    }

    /**
     * @param name Class name.
     * @param resolve Resolve class.
     * @return Class.
     * @throws ClassNotFoundException If failed.
     */
    private Class<?> loadClassDirect0(String name, boolean resolve) throws ClassNotFoundException {
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
     * Allows to explicitly load class by this classloader which must have external dependencies.
     *
     * @param clsName Class name.
     * @return Class.
     * @throws ClassNotFoundException If failed.
     */
    public Class<?> loadClassDirect(String clsName) throws ClassNotFoundException {
        if (!cache.containsKey(clsName)) {
            boolean res = hasExternalDependencies(clsName, new HashSet<String>());

            assert res; // Otherwise it does not make sense to load it directly here.
            assert cache.containsKey(clsName); // It must be added to cache.
        }

        return loadClass(clsName);
    }

    /**
     * @param clsName Class name.
     * @return {@code true} If the class has external dependencies.
     */
    private boolean hasExternalDependencies(String clsName, final Set<String> visited) {
        // Try to get from parent to check if the type accessible.
        InputStream in = getParent().getResourceAsStream(clsName.replace('.', '/') + ".class");

        if (in == null)  // The class is external itself, will be found in a usual way, no need to save its name.
            return true;

        if (!clsName.startsWith(GRIDGAIN_KERNAL_PKG)) // Check only our own classes.
            return false;

        ClassReader rdr;

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
                    onType(desc, true);

                    return av;
                }
            };

            MethodVisitor mv = new MethodVisitor(Opcodes.ASM4) {
                @Override public AnnotationVisitor visitAnnotation(String desc, boolean b) {
                    onType(desc, true);

                    return av;
                }

                @Override public AnnotationVisitor visitParameterAnnotation(int i, String desc, boolean b) {
                    onType(desc, true);

                    return av;
                }

                @Override public AnnotationVisitor visitAnnotationDefault() {
                    return av;
                }

                @Override public void visitFieldInsn(int i, String owner, String name, String desc) {
                    onType(owner, false);
                    onType(desc, true);
                }

                @Override public void visitFrame(int i, int i2, Object[] localTypes, int i3, Object[] stackTypes) {
                    for (Object o : localTypes) {
                        if (o instanceof String)
                            onType((String)o, false);
                    }

                    for (Object o : stackTypes) {
                        if (o instanceof String)
                            onType((String)o, false);
                    }
                }

                @Override public void visitLocalVariable(String name, String desc, String signature, Label label,
                    Label label2, int i) {
                    onType(desc, true);
                }

                @Override public void visitMethodInsn(int i, String owner, String name, String desc) {
                    onType(owner, false);
                }

                @Override public void visitMultiANewArrayInsn(String desc, int dim) {
                    X.println("-arr: " + desc);
                }

                @Override public void visitTryCatchBlock(Label label, Label label2, Label label3, String exception) {
                    onType(exception, false);
                }
            };

            void onClass(String depCls) {
                if (visited.contains(depCls))
                    return;

                Boolean res = cache.get(depCls);

                if (res == Boolean.TRUE || (res == null && hasExternalDependencies(depCls, visited)))
                    hasDeps.set(true);
            }

            void onType(String type, boolean internal) {
                if (type == null)
                    return;

                if (internal && type.charAt(0) == 'L')
                    type = type.substring(1, type.length() - 1);

                type = type.replace('/', '.');

                onClass(type);
            }

            @Override public void visit(int i, int i2, String name, String signature, String superName,
                String[] ifaces) {
                onType(superName, true);

                if (ifaces != null) {
                    for (String iface : ifaces)
                        onType(iface, true);
                }
            }

            @Override public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
                onType(desc, true);

                return av;
            }

            @Override public void visitInnerClass(String name, String outerName, String innerName, int i) {
                onType(name, false);
            }

            @Override public FieldVisitor visitField(int i, String name, String desc, String signature, Object val) {
                onType(desc, true);

                return fv;
            }

            @Override public MethodVisitor visitMethod(int i, String name, String desc, String signature,
                String[] exceptions) {
                if (exceptions != null) {
                    for (String e : exceptions)
                        onType(e, false);
                }

                return mv;
            }
        }, 0);

        cache.put(clsName, hasDeps.get());

        return hasDeps.get();
    }
}

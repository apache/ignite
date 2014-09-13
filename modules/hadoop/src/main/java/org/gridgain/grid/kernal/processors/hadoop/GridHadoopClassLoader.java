/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;
import org.springframework.asm.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Class loader allowing explicitly load classes without delegation to parent class loader.
 * Also supports class parsing for finding dependencies which contain transitive dependencies
 * unavailable for parent.
 */
public class GridHadoopClassLoader extends URLClassLoader {
    /** */
    private static final URLClassLoader APP_CLS_LDR = (URLClassLoader)GridHadoopClassLoader.class.getClassLoader();

    /** */
    private static final Collection<URL> appJars = F.asList(APP_CLS_LDR.getURLs());

    /** */
    private static volatile Collection<URL> hadoopJars;

    /** */
    private static final Map<String, Boolean> cache = new ConcurrentHashMap8<>();

    /**
     * @param urls Urls.
     */
    public GridHadoopClassLoader(URL[] urls) {
        super(addHadoopUrls(urls), APP_CLS_LDR);

        assert !(getParent() instanceof GridHadoopClassLoader);
    }

    /**
     * Need to parse only GridGain Hadoop and GGFS classes.
     *
     * @param cls Class name.
     * @return {@code true} if we need to check this class.
     */
    private static boolean isGgfsOrGgHadoop(String cls) {
        String gg = "org.gridgain.grid.";
        int len = gg.length();

        return cls.startsWith(gg) && (cls.indexOf("ggfs.", len) != -1 || cls.indexOf("hadoop.", len) != -1);
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
        if (isHadoop(name)) // Always load Hadoop classes explicitly, since Hadoop can be available in App classpath.
            return loadClassExplicitly(name, resolve);

        if (isGgfsOrGgHadoop(name)) { // For GG Hadoop and GGFS classes we have to check if they depend on Hadoop.
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
     * @param clsName Class name.
     * @return {@code true} If the class has external dependencies.
     */
    boolean hasExternalDependencies(String clsName, final Set<String> visited) {
        if (isHadoop(clsName)) // Hadoop must not be in classpath but Idea sucks, so filtering explicitly as external.
            return true;

        // Try to get from parent to check if the type accessible.
        InputStream in = getParent().getResourceAsStream(clsName.replace('.', '/') + ".class");

        if (in == null) // The class is external itself, it must be loaded from this class loader.
            return true;

        if (!isGgfsOrGgHadoop(clsName)) // Other classes should not have external dependencies.
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
                    onType(desc, true);
                }

                @Override public void visitTryCatchBlock(Label label, Label label2, Label label3, String exception) {
                    onType(exception, false);
                }
            };

            void onClass(String depCls) {
                if (depCls.startsWith("java.")) // Filter out platform classes.
                    return;

                if (visited.contains(depCls))
                    return;

                Boolean res = cache.get(depCls);

                if (res == Boolean.TRUE || (res == null && hasExternalDependencies(depCls, visited)))
                    hasDeps.set(true);
            }

            void onType(String type, boolean internal) {
                if (type == null)
                    return;

                if (!internal && type.charAt(0) == '[')
                    internal = true;

                if (internal) {
                    int off = 0;

                    while (type.charAt(off) == '[')
                        off++; // Handle arrays.

                    if (type.charAt(off) != 'L')
                        return; // Skip primitives.

                    type = type.substring(off + 1, type.length() - 1);
                }

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
        catch (GridException e) {
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
     * @throws GridException If failed.
     */
    public static Collection<URL> hadoopUrls() throws GridException {
        Collection<URL> hadoopUrls = hadoopJars;

        if (hadoopUrls != null)
            return hadoopUrls;

        synchronized (GridHadoopClassLoader.class) {
            hadoopUrls = hadoopJars;

            if (hadoopUrls != null)
                return hadoopUrls;

            hadoopUrls = new ArrayList<>();

            String hadoopPrefix = hadoopHome();

            if (F.isEmpty(hadoopPrefix))
                throw new GridException("Failed resolve Hadoop installation location. Either HADOOP_PREFIX or " +
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
                throw new GridException(e);
            }

            hadoopJars = hadoopUrls;

            return hadoopUrls;
        }
    }
}

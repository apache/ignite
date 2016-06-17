package org.apache.ignite.internal.processors.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.lang.System.out;
import static java.lang.System.err;

/**
 * Main class to compose Hadoop classpath depending on the environment.
 * Note that this class should not depend on any classes or libraries except the JDK default runtime.
 */
public class HadoopClasspathMain {
    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        final char sep = File.pathSeparatorChar;

        List<String> cp = getAsProcessClasspath();

        for (String s: cp) {
            if (s != null && s.length() > 0) {
                out.print(s);
                out.print(sep);
            }
        }

        out.println();
    }

    /**
     *
     * @return
     */
    private static List<String> getAsProcessClasspath() throws IOException {
        Collection<DirAndMask> dams = getClasspathBaseDirectories();

        List<String> list = new ArrayList<>(32);

        for (DirAndMask dam: dams)
            addAsJavaProcessClasspathElement(list, dam.file, dam.mask);

        // Sort the classpath elements to make it more reliable.
        Collections.sort(list);

        return list;
    }

    /**
     *
     * @return
     * @throws IOException
     */
    public static List<URL> getAsUrlList() throws IOException {
        Collection<DirAndMask> dams = getClasspathBaseDirectories();

        List<URL> list = new ArrayList<>(32);

        for (DirAndMask dam: dams)
            // Note that this procedure does not use '*' classpath patterns,
            // but adds all the children explicitly:
            addUrls(list, dam.file, dam.mask);

        Collections.sort(list, new Comparator<URL>() {
            @Override public int compare(URL u1, URL u2) {
                String s1 = String.valueOf(u1);
                String s2 = String.valueOf(u2);

                return s1.compareTo(s2);
            }
        });

        for (URL u: list)
            err.println(u);

        return list;
    }

    /**
     * @param res Result.
     * @param dir Directory.
     * @param startsWith Starts with prefix.
     * @throws MalformedURLException If failed.
     */
    private static void addUrls(Collection<URL> res, File dir, final String startsWith) throws IOException {
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
     * @param res Result.
     * @param dir Directory.
     * @param startsWith Starts with prefix.
     * @throws MalformedURLException If failed.
     */
    private static void addAsJavaProcessClasspathElement(Collection<String> res, File dir, final String startsWith) throws IOException {
        if (!dir.exists() || !dir.isDirectory() || !dir.canRead())
            throw new IOException("Path is not an existing readable directory: " + dir);

        if (startsWith == null)
            res.add(dir.getAbsolutePath() + File.separator + '*');
        else {
            File[] files = dir.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.startsWith(startsWith);
                }
            });

            if (files == null)
                throw new IOException("Path is not a directory: " + dir);

            for (File file : files)
                res.add(file.getAbsolutePath());
        }
    }

    /**
     * @return HADOOP_HOME Variable.
     */
    public static String hadoopHome() {
        String prefix = getEnv("HADOOP_PREFIX", null);

        return getEnv("HADOOP_HOME", prefix);
    }

    /**
     *
     * @return
     * @throws FileNotFoundException
     */
    public static Collection<DirAndMask> getClasspathBaseDirectories() throws FileNotFoundException {
        final String hadoopHome = hadoopHome();

        String commonHome = resolveLocation("HADOOP_COMMON_HOME", hadoopHome, "/share/hadoop/common");
        String hdfsHome = resolveLocation("HADOOP_HDFS_HOME", hadoopHome, "/share/hadoop/hdfs");
        String mapredHome = resolveLocation("HADOOP_MAPRED_HOME", hadoopHome, "/share/hadoop/mapreduce");

        Collection<DirAndMask> c = new ArrayList<>();

        c.add(new DirAndMask(new File(commonHome, "lib"), null));
        c.add(new DirAndMask(new File(hdfsHome, "lib"), null));
        c.add(new DirAndMask(new File(mapredHome, "lib"), null));

        c.add(new DirAndMask(new File(commonHome), "hadoop-common-"));
        c.add(new DirAndMask(new File(commonHome), "hadoop-auth-"));
        c.add(new DirAndMask(new File(commonHome, "lib"), "hadoop-auth-"));

        c.add(new DirAndMask(new File(hdfsHome), "hadoop-hdfs-"));

        c.add(new DirAndMask(new File(mapredHome), "hadoop-mapreduce-client-common"));
        c.add(new DirAndMask(new File(mapredHome), "hadoop-mapreduce-client-core"));

        return c;
    }

    public static class DirAndMask {
        DirAndMask(File f, String m) {
            file = f;
            mask = m;
        }
        public final File file;
        public final String mask;
    }

    /**
     * Checks if the variable is empty.
     *
     * @param envVarName Environment variable name.
     * @param hadoopHome The current value.
     * @param expHadoopHomeRelativePath The path relative to Hadoop home.
     * @throws FileNotFoundException If the value is empty.
     */
    private static String resolveLocation(String envVarName, String hadoopHome,
        String expHadoopHomeRelativePath) throws FileNotFoundException {
        String val = getEnv(envVarName, null);

        if (val == null) {
            // The env. variable is not set. Try to resolve the location relative HADOOP_HOME:
            if (!isExistingDirectory(hadoopHome))
                throw new FileNotFoundException("Failed to resolve Hadoop installation location. " +
                        envVarName + " or HADOOP_HOME environment variable should be set.");

            val = hadoopHome + expHadoopHomeRelativePath;
        }

        if (!isExistingDirectory(val))
            throw new FileNotFoundException("Failed to resolve Hadoop location. [path=" + val + ']');

        err.println(envVarName + " resolved to " + val);

        return val;
    }

    /**
     * Note that this method does not treat empty value as an absent value.
     *
     * @param name Variable name.
     * @param dflt Default.
     * @return Value.
     */
    private static String getEnv(String name, String dflt) {
        String res = System.getProperty(name);

        if (res == null)
            res = System.getenv(name);

        return res == null ? dflt : res;
    }

    /**
     * Answers if the given path denotes existing directory.
     *
     * @param path The directory path.
     * @return 'true' if the given path denotes an existing directory.
     */
    private static boolean isExistingDirectory(String path) {
        if (path == null)
            return false;

        Path p = Paths.get(path);

        return Files.exists(p) && Files.isDirectory(p) && Files.isReadable(p);
    }
}

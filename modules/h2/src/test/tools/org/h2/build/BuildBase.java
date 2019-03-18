/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarOutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


/**
 * This class is a complete pure Java build tool. It allows to build this
 * project without any external dependencies except a JDK.
 * Advantages: ability to debug the build, extensible, flexible,
 * no XML, a bit faster.
 */
public class BuildBase {

    /**
     * Stores descriptions for methods which can be invoked as build targets.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public static @interface Description {
        String summary() default "";
    }

    /**
     * A list of strings.
     */
    public static class StringList extends ArrayList<String> {

        private static final long serialVersionUID = 1L;

        StringList(String... args) {
            super();
            addAll(Arrays.asList(args));
        }

        /**
         * Add a list of strings.
         *
         * @param args the list to add
         * @return the new list
         */
        public StringList plus(String...args) {
            StringList newList = new StringList();
            newList.addAll(this);
            newList.addAll(Arrays.asList(args));
            return newList;
        }

        /**
         * Convert this list to a string array.
         *
         * @return the string array
         */
        public String[] array() {
            String[] list = new String[size()];
            for (int i = 0; i < size(); i++) {
                list[i] = get(i);
            }
            return list;
        }

    }

    /**
     * A list of files.
     */
    public static class FileList extends ArrayList<File> {

        private static final long serialVersionUID = 1L;

        /**
         * Remove the files that match from the list.
         * Patterns must start or end with a *.
         *
         * @param pattern the pattern of the file names to remove
         * @return the new file list
         */
        public FileList exclude(String pattern) {
            return filter(false, pattern);
        }

        /**
         * Only keep the files that match.
         * Patterns must start or end with a *.
         *
         * @param pattern the pattern of the file names to keep
         * @return the new file list
         */
        public FileList keep(String pattern) {
            return filter(true, pattern);
        }

        /**
         * Filter a list of file names.
         *
         * @param keep if matching file names should be kept or removed
         * @param pattern the file name pattern
         * @return the filtered file list
         */
        private FileList filter(boolean keep, String pattern) {
            boolean start = false;
            if (pattern.endsWith("*")) {
                pattern = pattern.substring(0, pattern.length() - 1);
                start = true;
            } else if (pattern.startsWith("*")) {
                pattern = pattern.substring(1);
            }
            if (pattern.indexOf('*') >= 0) {
                throw new RuntimeException(
                        "Unsupported pattern, may only start or end with *:"
                                + pattern);
            }
            // normalize / and \
            pattern = BuildBase.replaceAll(pattern, "/", File.separator);
            FileList list = new FileList();
            for (File f : this) {
                String path = f.getPath();
                boolean match = start ? path.startsWith(pattern) : path.endsWith(pattern);
                if (match == keep) {
                    list.add(f);
                }
            }
            return list;
        }

    }

    /**
     * The output stream (System.out).
     */
    protected final PrintStream sysOut = System.out;

    /**
     * If output should be disabled.
     */
    protected boolean quiet;

    /**
     * The full path to the executable of the current JRE.
     */
    protected final String javaExecutable = System.getProperty("java.home") +
            File.separator + "bin" + File.separator + "java";

    /**
     * The full path to the tools jar of the current JDK.
     */
    protected final String javaToolsJar = System.getProperty("java.home") + File.separator + ".." +
            File.separator + "lib" + File.separator + "tools.jar";

    /**
     * This method should be called by the main method.
     *
     * @param args the command line parameters
     */
    protected void run(String... args) {
        long time = System.nanoTime();
        if (args.length == 0) {
            all();
        } else {
            for (String a : args) {
                if ("-quiet".equals(a)) {
                    quiet = true;
                } else if ("-".equals(a)) {
                    runShell();
                    return;
                } else if (a.startsWith("-D")) {
                    String value;
                    String key = a.substring(2);
                    int valueIndex = key.indexOf('=');
                    if (valueIndex >= 0) {
                        value = key.substring(valueIndex + 1);
                        key = key.substring(0, valueIndex);
                    } else {
                        value = "true";
                    }
                    System.setProperty(key, value);
                } else {
                    if (!runTarget(a)) {
                        break;
                    }
                }
            }
        }
        println("Done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time) + " ms");
    }

    private boolean runTarget(String target) {
        Method m = null;
        try {
            m = getClass().getMethod(target);
        } catch (Exception e) {
            sysOut.println("Unknown target: " + target);
            projectHelp();
            return false;
        }
        println("Target: " + target);
        invoke(m, this, new Object[0]);
        return true;
    }

    private void runShell() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String last = "", line;
        System.out.println("Shell mode. Type the target, then [Enter]. " +
                "Just [Enter] repeats the last target.");
        while (true) {
            System.out.print("build> ");
            try {
                line = reader.readLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (line == null || line.equals("exit") || line.equals("quit")) {
                break;
            } else if (line.length() == 0) {
                line = last;
            }
            long time = System.nanoTime();
            try {
                runTarget(line);
            } catch (Exception e) {
                System.out.println(e);
            }
            println("Done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time) + " ms");
            last = line;
        }
    }

    private static Object invoke(Method m, Object instance, Object[] args) {
        try {
            try {
                return m.invoke(instance, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        } catch (Error e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is called if no other target is specified in the command
     * line. The default behavior is to call projectHelp(). Override this method
     * if you want another default behavior.
     */
    protected void all() {
        projectHelp();
    }

    /**
     * Emit a beep.
     */
    protected void beep() {
        sysOut.print("\007");
        sysOut.flush();
    }

    /**
     * Lists all targets (all public methods non-static methods without
     * parameters).
     */
    protected void projectHelp() {
        Method[] methods = getClass().getDeclaredMethods();
        Arrays.sort(methods, new Comparator<Method>() {
            @Override
            public int compare(Method a, Method b) {
                return a.getName().compareTo(b.getName());
            }
        });
        sysOut.println("Targets:");
        String description;
        for (Method m : methods) {
            int mod = m.getModifiers();
            if (!Modifier.isStatic(mod) && Modifier.isPublic(mod)
                    && m.getParameterTypes().length == 0) {
                if (m.isAnnotationPresent(Description.class)) {
                    description = String.format("%1$-20s %2$s",
                            m.getName(), m.getAnnotation(Description.class).summary());
                } else {
                    description = m.getName();
                }
                sysOut.println(description);
            }
        }
        sysOut.println();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("windows");
    }

    /**
     * Execute a script in a separate process.
     * In Windows, the batch file with this name (.bat) is run.
     *
     * @param script the program to run
     * @param args the command line parameters
     * @return the exit value
     */
    protected int execScript(String script, StringList args) {
        if (isWindows()) {
            // Under windows, we use the "cmd" command interpreter since it will
            // search the path for us without us having to hard-code an
            // extension for the script we want. (Sometimes we don't know if the
            // extension will be .bat or .cmd)
            StringList newArgs = new StringList();
            newArgs.add("/C");
            newArgs.add(script);
            newArgs.addAll(args);
            return exec("cmd", newArgs);
        }
        return exec(script, args);
    }

    /**
     * Execute java in a separate process, but using the java executable of the
     * current JRE.
     *
     * @param args the command line parameters for the java command
     * @return the exit value
     */
    protected int execJava(StringList args) {
        return exec(javaExecutable, args);
    }

    /**
     * Execute a program in a separate process.
     *
     * @param command the program to run
     * @param args the command line parameters
     * @return the exit value
     */
    protected int exec(String command, StringList args) {
        try {
            print(command);
            StringList cmd = new StringList();
            cmd = cmd.plus(command);
            if (args != null) {
                for (String a : args) {
                    print(" " + a);
                }
                cmd.addAll(args);
            }
            println("");
            ProcessBuilder pb = new ProcessBuilder();
            pb.command(cmd.array());
            pb.redirectErrorStream(true);
            Process p = pb.start();
            copyInThread(p.getInputStream(), quiet ? null : sysOut);
            p.waitFor();
            return p.exitValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void copyInThread(final InputStream in, final OutputStream out) {
        new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {
                        int x = in.read();
                        if (x < 0) {
                            return;
                        }
                        if (out != null) {
                            out.write(x);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
    }

    /**
     * Read a final static field in a class using reflection.
     *
     * @param className the name of the class
     * @param fieldName the field name
     * @return the value as a string
     */
    protected static String getStaticField(String className, String fieldName) {
        try {
            Class<?> clazz = Class.forName(className);
            Field field = clazz.getField(fieldName);
            return field.get(null).toString();
        } catch (Exception e) {
            throw new RuntimeException("Can not read field " + className + "."
                    + fieldName, e);
        }
    }

    /**
     * Reads the value from a static method of a class using reflection.
     *
     * @param className the name of the class
     * @param methodName the field name
     * @return the value as a string
     */
    protected static String getStaticValue(String className, String methodName) {
        try {
            Class<?> clazz = Class.forName(className);
            Method method = clazz.getMethod(methodName);
            return method.invoke(null).toString();
        } catch (Exception e) {
            throw new RuntimeException("Can not read value " + className + "."
                    + methodName + "()", e);
        }
    }

    /**
     * Copy files to the specified target directory.
     *
     * @param targetDir the target directory
     * @param files the list of files to copy
     * @param baseDir the base directory
     */
    protected void copy(String targetDir, FileList files, String baseDir) {
        File target = new File(targetDir);
        File base = new File(baseDir);
        println("Copying " + files.size() + " files to " + target.getPath());
        String basePath = base.getPath();
        for (File f : files) {
            File t = new File(target, removeBase(basePath, f.getPath()));
            byte[] data = readFile(f);
            mkdirs(t.getParentFile());
            writeFile(t, data);
        }
    }

    private static PrintStream filter(PrintStream out, final String[] exclude) {
        return new PrintStream(new FilterOutputStream(out) {
            private ByteArrayOutputStream buff = new ByteArrayOutputStream();

            @Override
            public void write(byte[] b) throws IOException {
                write(b, 0, b.length);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                for (int i = off; i < len; i++) {
                    write(b[i]);
                }
            }

            public void write(byte b) throws IOException {
                buff.write(b);
                if (b == '\n') {
                    byte[] data = buff.toByteArray();
                    String line = new String(data, StandardCharsets.UTF_8);
                    boolean print = true;
                    for (String l : exclude) {
                        if (line.startsWith(l)) {
                            print = false;
                            break;
                        }
                    }
                    if (print) {
                        out.write(data);
                    }
                    buff.reset();
                }
            }

            @Override
            public void close() throws IOException {
                write('\n');
            }
        });
    }

    /**
     * Run a Javadoc task.
     *
     * @param args the command line arguments to pass
     */
    protected void javadoc(String...args) {
        int result;
        PrintStream old = System.out;
        try {
            println("Javadoc");
            if (quiet) {
                System.setOut(filter(System.out, new String[] {
                        "Loading source files for package",
                        "Constructing Javadoc information...",
                        "Generating ",
                        "Standard Doclet",
                        "Building tree for all the packages and classes...",
                        "Building index for all the packages and classes...",
                        "Building index for all classes..."
                }));
            } else {
                System.setOut(filter(System.out, new String[] {
                        "Loading source files for package ",
                        "Generating ",
                }));
            }
            Class<?> clazz = Class.forName("com.sun.tools.javadoc.Main");
            Method execute = clazz.getMethod("execute", String[].class);
            result = (Integer) invoke(execute, null, new Object[] { args });
        } catch (Exception e) {
            result = exec("javadoc", args(args));
        } finally {
            System.setOut(old);
        }
        if (result != 0) {
            throw new RuntimeException("An error occurred, result=" + result);
        }
    }

    private static String convertBytesToString(byte[] value) {
        StringBuilder buff = new StringBuilder(value.length * 2);
        for (byte c : value) {
            int x = c & 0xff;
            buff.append(Integer.toString(x >> 4, 16)).
                append(Integer.toString(x & 0xf, 16));
        }
        return buff.toString();
    }

    /**
     * Generate the SHA1 checksum of a byte array.
     *
     * @param data the byte array
     * @return the SHA1 checksum
     */
    protected static String getSHA1(byte[] data) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
            return convertBytesToString(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Download a file if it does not yet exist. Maven is used if installed, so
     * that the file is first downloaded to the local repository and then copied
     * from there.
     *
     * @param target the target file name
     * @param group the Maven group id
     * @param artifact the Maven artifact id
     * @param version the Maven version id
     * @param sha1Checksum the SHA-1 checksum or null
     */
    protected void downloadUsingMaven(String target, String group,
            String artifact, String version, String sha1Checksum) {
        String repoDir = "http://repo1.maven.org/maven2";
        File targetFile = new File(target);
        if (targetFile.exists()) {
            return;
        }
        String repoFile = group.replace('.', '/') + "/" + artifact + "/" + version + "/"
                + artifact + "-" + version + ".jar";
        mkdirs(targetFile.getAbsoluteFile().getParentFile());
        String localMavenDir = getLocalMavenDir();
        if (new File(localMavenDir).exists()) {
            File f = new File(localMavenDir, repoFile);
            if (!f.exists()) {
                try {
                    execScript("mvn", args(
                            "org.apache.maven.plugins:maven-dependency-plugin:2.1:get",
                            "-D" + "repoUrl=" + repoDir,
                            "-D" + "artifact="+ group +":"+ artifact +":" + version));
                } catch (RuntimeException e) {
                    println("Could not download using Maven: " + e.toString());
                }
            }
            if (f.exists()) {
                byte[] data = readFile(f);
                String got = getSHA1(data);
                if (sha1Checksum == null) {
                    println("SHA1 checksum: " + got);
                } else {
                    if (!got.equals(sha1Checksum)) {
                        throw new RuntimeException(
                                "SHA1 checksum mismatch; got: " + got +
                                        " expected: " + sha1Checksum +
                                        " for file " + f.getAbsolutePath());
                    }
                }
                writeFile(targetFile, data);
                return;
            }
        }
        String fileURL = repoDir + "/" + repoFile;
        download(target, fileURL, sha1Checksum);
    }

    protected String getLocalMavenDir() {
        return System.getProperty("user.home") + "/.m2/repository";
    }

    /**
     * Download a file if it does not yet exist.
     * If no checksum is used (that is, if the parameter is null), the
     * checksum is printed. For security, checksums should always be used.
     *
     * @param target the target file name
     * @param fileURL the source url of the file
     * @param sha1Checksum the SHA-1 checksum or null
     */
    protected void download(String target, String fileURL, String sha1Checksum) {
        File targetFile = new File(target);
        if (targetFile.exists()) {
            return;
        }
        mkdirs(targetFile.getAbsoluteFile().getParentFile());
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        try {
            println("Downloading " + fileURL);
            URL url = new URL(fileURL);
            InputStream in = new BufferedInputStream(url.openStream());
            long last = System.nanoTime();
            int len = 0;
            while (true) {
                long now = System.nanoTime();
                if (now > last + TimeUnit.SECONDS.toNanos(1)) {
                    println("Downloaded " + len + " bytes");
                    last = now;
                }
                int x = in.read();
                len++;
                if (x < 0) {
                    break;
                }
                buff.write(x);
            }
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Error downloading " + fileURL + " to " + target, e);
        }
        byte[] data = buff.toByteArray();
        String got = getSHA1(data);
        if (sha1Checksum == null) {
            println("SHA1 checksum: " + got);
        } else {
            if (!got.equals(sha1Checksum)) {
                throw new RuntimeException("SHA1 checksum mismatch; got: " +
                        got + " expected: " + sha1Checksum + " for file " + target);

            }
        }
        writeFile(targetFile, data);
    }

    /**
     * Get the list of files in the given directory and all subdirectories.
     *
     * @param dir the source directory
     * @return the file list
     */
    protected FileList files(String dir) {
        FileList list = new FileList();
        addFiles(list, new File(dir));
        return list;
    }

    /**
     * Create a string list.
     *
     * @param args the arguments
     * @return the string list
     */
    protected static StringList args(String...args) {
        return new StringList(args);
    }

    private void addFiles(FileList list, File file) {
        if (file.getName().startsWith(".svn")) {
            // ignore
        } else if (file.isDirectory()) {
            String path = file.getPath();
            for (String fileName : file.list()) {
                addFiles(list, new File(path, fileName));
            }
        } else {
            list.add(file);
        }
    }

    private static String removeBase(String basePath, String path) {
        if (path.startsWith(basePath)) {
            path = path.substring(basePath.length());
        }
        path = path.replace('\\', '/');
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }

    /**
     * Create or overwrite a file.
     *
     * @param file the file
     * @param data the data to write
     */
    public static void writeFile(File file, byte[] data) {
        try {
            RandomAccessFile ra = new RandomAccessFile(file, "rw");
            ra.write(data);
            ra.setLength(data.length);
            ra.close();
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file " + file, e);
        }
    }

    /**
     * Read a file. The maximum file size is Integer.MAX_VALUE.
     *
     * @param file the file
     * @return the data
     */
    public static byte[] readFile(File file) {
        RandomAccessFile ra = null;
        try {
            ra = new RandomAccessFile(file, "r");
            long len = ra.length();
            if (len >= Integer.MAX_VALUE) {
                throw new RuntimeException("File " + file.getPath() + " is too large");
            }
            byte[] buffer = new byte[(int) len];
            ra.readFully(buffer);
            ra.close();
            return buffer;
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file " + file, e);
        } finally {
            if (ra != null) {
                try {
                    ra.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Get the file name suffix.
     *
     * @param fileName the file name
     * @return the suffix or an empty string if there is none
     */
    static String getSuffix(String fileName) {
        int idx = fileName.lastIndexOf('.');
        return idx < 0 ? "" : fileName.substring(idx);
    }

    /**
     * Create a jar file.
     *
     * @param destFile the target file name
     * @param files the file list
     * @param basePath the base path
     * @return the size of the jar file in KB
     */
    protected long jar(String destFile, FileList files, String basePath) {
        long kb = zipOrJar(destFile, files, basePath, false, false, true);
        println("Jar " + destFile + " (" + kb + " KB)");
        return kb;
    }

    /**
     * Create a zip file.
     *
     * @param destFile the target file name
     * @param files the file list
     * @param basePath the base path
     * @param storeOnly if the files should not be compressed
     * @param sortBySuffix if the file should be sorted by the file suffix
     */
    protected void zip(String destFile, FileList files, String basePath,
            boolean storeOnly, boolean sortBySuffix) {
        long kb = zipOrJar(destFile, files, basePath, storeOnly, sortBySuffix, false);
        println("Zip " + destFile + " (" + kb + " KB)");
    }

    private static long zipOrJar(String destFile, FileList files,
            String basePath, boolean storeOnly, boolean sortBySuffix, boolean jar) {
        if (sortBySuffix) {
            // for better compressibility, sort by suffix, then name
            Collections.sort(files, new Comparator<File>() {
                @Override
                public int compare(File f1, File f2) {
                    String p1 = f1.getPath();
                    String p2 = f2.getPath();
                    int comp = getSuffix(p1).compareTo(getSuffix(p2));
                    if (comp == 0) {
                        comp = p1.compareTo(p2);
                    }
                    return comp;
                }
            });
        } else if (jar) {
            Collections.sort(files, new Comparator<File>() {
                private int priority(String path) {
                    if (path.startsWith("META-INF/")) {
                        if (path.equals("META-INF/MANIFEST.MF")) {
                            return 0;
                        }
                        if (path.startsWith("services/", 9)) {
                            return 1;
                        }
                        return 2;
                    }
                    if (!path.endsWith(".zip")) {
                        return 3;
                    }
                    return 4;
                }

                @Override
                public int compare(File f1, File f2) {
                    String p1 = f1.getPath();
                    String p2 = f2.getPath();
                    int comp = Integer.compare(priority(p1), priority(p2));
                    if (comp != 0) {
                        return comp;
                    }
                    return p1.compareTo(p2);
                }
            });
        }
        mkdirs(new File(destFile).getAbsoluteFile().getParentFile());
        // normalize the path (replace / with \ if required)
        basePath = new File(basePath).getPath();
        try {
            if (new File(destFile).isDirectory()) {
                throw new IOException(
                        "Can't create the file as a directory with this name already exists: "
                                + destFile);
            }
            OutputStream out = new BufferedOutputStream(new FileOutputStream(destFile));
            ZipOutputStream zipOut;
            if (jar) {
                zipOut = new JarOutputStream(out);
            } else {
                zipOut = new ZipOutputStream(out);
            }
            if (storeOnly) {
                zipOut.setMethod(ZipOutputStream.STORED);
            }
            zipOut.setLevel(Deflater.BEST_COMPRESSION);
            for (File file : files) {
                String fileName = file.getPath();
                String entryName = removeBase(basePath, fileName);
                byte[] data = readFile(file);
                ZipEntry entry = new ZipEntry(entryName);
                CRC32 crc = new CRC32();
                crc.update(data);
                entry.setSize(file.length());
                entry.setCrc(crc.getValue());
                zipOut.putNextEntry(entry);
                zipOut.write(data);
                zipOut.closeEntry();
            }
            zipOut.closeEntry();
            zipOut.close();
            return new File(destFile).length() / 1024;
        } catch (IOException e) {
            throw new RuntimeException("Error creating file " + destFile, e);
        }
    }

    /**
     * Get the current java specification version (for example, 1.4).
     *
     * @return the java specification version
     */
    protected static String getJavaSpecVersion() {
        return System.getProperty("java.specification.version");
    }

    private static List<String> getPaths(FileList files) {
        StringList list = new StringList();
        for (File f : files) {
            list.add(f.getPath());
        }
        return list;
    }

    /**
     * Compile the files.
     *
     * @param args the command line parameters
     * @param files the file list
     */
    protected void javac(StringList args, FileList files) {
        println("Compiling " + files.size() + " classes");
        StringList params = new StringList();
        params.addAll(args);
        params.addAll(getPaths(files.keep(".java")));
        String[] array = params.array();
        int result;
        PrintStream old = System.err;
        try {
            Class<?> clazz = Class.forName("com.sun.tools.javac.Main");
            if (quiet) {
                System.setErr(filter(System.err, new String[] {
                        "Note:"
                }));
            }
            Method compile = clazz.getMethod("compile", new Class<?>[] { String[].class });
            Object instance = clazz.newInstance();
            result = (Integer) invoke(compile, instance, new Object[] { array });
        } catch (Exception e) {
            e.printStackTrace();
            result = exec("javac", new StringList(array));
        } finally {
            System.setErr(old);
        }
        if (result != 0) {
            throw new RuntimeException("An error occurred");
        }
    }

    /**
     * Call the main method of the given Java class using reflection.
     *
     * @param className the class name
     * @param args the command line parameters to pass
     */
    protected void java(String className, StringList args) {
        println("Running " + className);
        String[] array = args == null ? new String[0] : args.array();
        try {
            Method main = Class.forName(className).getMethod("main", String[].class);
            invoke(main, null, new Object[] { array });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create the directory including the parent directories if they don't
     * exist.
     *
     * @param dir the directory to create
     */
    protected static void mkdir(String dir) {
        File f = new File(dir);
        if (f.exists()) {
            if (f.isFile()) {
                throw new RuntimeException("Can not create directory " + dir
                        + " because a file with this name exists");
            }
        } else {
            mkdirs(f);
        }
    }

    private static void mkdirs(File f) {
        if (!f.exists()) {
            if (!f.mkdirs()) {
                throw new RuntimeException("Can not create directory " + f.getAbsolutePath());
            }
        }
    }

    /**
     * Delete all files in the given directory and all subdirectories.
     *
     * @param dir the name of the directory
     */
    protected void delete(String dir) {
        println("Deleting " + dir);
        delete(new File(dir));
    }

    /**
     * Delete all files in the list.
     *
     * @param files the name of the files to delete
     */
    protected void delete(FileList files) {
        for (File f : files) {
            delete(f);
        }
    }

    private void delete(File file) {
        if (file.exists()) {
            if (file.isDirectory()) {
                String path = file.getPath();
                for (String fileName : file.list()) {
                    delete(new File(path, fileName));
                }
            }
            if (!file.delete()) {
                throw new RuntimeException("Can not delete " + file.getPath());
            }
        }
    }

    /**
     * Replace each substring in a given string. Regular expression is not used.
     *
     * @param s the original text
     * @param before the old substring
     * @param after the new substring
     * @return the string with the string replaced
     */
    protected static String replaceAll(String s, String before, String after) {
        int index = 0;
        while (true) {
            int next = s.indexOf(before, index);
            if (next < 0) {
                return s;
            }
            s = s.substring(0, next) + after + s.substring(next + before.length());
            index = next + after.length();
        }
    }

    /**
     * Print a line to the output unless the quiet mode is enabled.
     *
     * @param s the text to write
     */
    protected void println(String s) {
        if (!quiet) {
            sysOut.println(s);
        }
    }

    /**
     * Print a message to the output unless the quiet mode is enabled.
     *
     * @param s the message to write
     */
    protected void print(String s) {
        if (!quiet) {
            sysOut.print(s);
        }
    }

}

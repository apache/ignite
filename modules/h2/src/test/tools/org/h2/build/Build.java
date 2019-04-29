/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.h2.build.doc.XMLParser;

/**
 * The build definition.
 */
public class Build extends BuildBase {

    private boolean filesMissing;

    /**
     * Run the build.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) {
        new Build().run(args);
    }

    /**
     * Run the benchmarks.
     */
    @Description(summary = "Run the benchmarks.")
    public void benchmark() {
        downloadUsingMaven("ext/hsqldb-2.3.2.jar",
                "org/hsqldb", "hsqldb", "2.3.2",
                "970fd7b8f635e2c19305160459649569655b843c");
        downloadUsingMaven("ext/derby-10.10.1.1.jar",
                "org/apache/derby", "derby", "10.10.1.1",
                "09f6f910f0373adc1b23c10f9b4bb151b7e7449f");
        downloadUsingMaven("ext/derbyclient-10.10.1.1.jar",
                "org/apache/derby", "derbyclient", "10.10.1.1",
                "42d5293b4ac5c5f082583c3564c10f78bd34a4cb");
        downloadUsingMaven("ext/derbynet-10.10.1.1.jar",
                "org/apache/derby", "derbynet", "10.10.1.1",
                "912b08dca73663d4665e09cd317be1218412d93e");
        downloadUsingMaven("ext/postgresql-42.2.5.jre7",
                "org.postgresql", "postgresql", "42.2.5.jre7",
                "ec74f6f7885b7e791f84c7219a97964e9d0121e4");
        downloadUsingMaven("ext/mysql-connector-java-5.1.6.jar",
                "mysql", "mysql-connector-java", "5.1.6",
                "380ef5226de2c85ff3b38cbfefeea881c5fce09d");
        compile();

        String cp = "temp" +
                File.pathSeparator + "bin/h2" + getJarSuffix() +
                File.pathSeparator + "ext/hsqldb.jar" +
                File.pathSeparator + "ext/hsqldb-2.3.2.jar" +
                File.pathSeparator + "ext/derby-10.10.1.1.jar" +
                File.pathSeparator + "ext/derbyclient-10.10.1.1.jar" +
                File.pathSeparator + "ext/derbynet-10.10.1.1.jar" +
                File.pathSeparator + "ext/postgresql-42.2.5.jre7" +
                File.pathSeparator + "ext/mysql-connector-java-5.1.6.jar";
        StringList args = args("-Xmx128m",
                "-cp", cp, "org.h2.test.bench.TestPerformance");
        execJava(args.plus("-init", "-db", "1"));
        execJava(args.plus("-db", "2"));
        execJava(args.plus("-db", "3", "-out", "pe.html"));
        execJava(args.plus("-init", "-db", "4"));
        execJava(args.plus("-db", "5", "-exit"));
        execJava(args.plus("-db", "6"));
        execJava(args.plus("-db", "7"));
        execJava(args.plus("-db", "8", "-out", "ps.html"));
    }

    /**
     * Clean all jar files, classes, and generated documentation.
     */
    @Description(summary = "Clean all jar files, classes, and generated documentation.")
    public void clean() {
        delete("temp");
        delete("docs");
        mkdir("docs");
        mkdir("bin");
        delete(files(".").keep("*/Thumbs.db"));
    }

    /**
     * Compile all classes.
     */
    @Description(summary = "Compile all classes.")
    public void compile() {
        compile(true, false, false);
    }

    private void compileTools() {
        mkdir("temp");
        FileList files = files("src/tools").keep("src/tools/org/h2/build/*");
        StringList args = args("-d", "temp", "-sourcepath", "src/tools" +
                File.pathSeparator + "src/test" +
                File.pathSeparator + "src/main");
        String version = getTargetJavaVersion();
        if (version != null) {
            args = args.plus("-target", version, "-source", version);
        }
        javac(args, files);
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer, 0, buffer.length)) >= 0) {
            out.write(buffer, 0, read);
        }
    }

    /**
     * Run the JaCoco code coverage.
     */
    @Description(summary = "Run the JaCoco code coverage.")
    public void coverage() {
        compile();
        downloadTest();
        downloadUsingMaven("ext/org.jacoco.agent-0.8.2.jar",
                "org.jacoco", "org.jacoco.agent", "0.8.2",
                "1402427761df5c7601ff6e06280764833ed727b5");
        try (ZipFile zipFile = new ZipFile(new File("ext/org.jacoco.agent-0.8.2.jar"))) {
            final Enumeration<? extends ZipEntry> e = zipFile.entries();
            while (e.hasMoreElements()) {
                final ZipEntry zipEntry = e.nextElement();
                final String name = zipEntry.getName();
                if (name.equals("jacocoagent.jar")) {
                    try (InputStream in = zipFile.getInputStream(zipEntry);
                            FileOutputStream out = new FileOutputStream("ext/jacocoagent.jar")) {
                        copy(in, out);
                    }
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        downloadUsingMaven("ext/org.jacoco.cli-0.8.2.jar",
                "org.jacoco", "org.jacoco.cli", "0.8.2",
                "9595c53358d0306900183b5a7e6a70c88171ab4c");
        downloadUsingMaven("ext/org.jacoco.core-0.8.2.jar",
                "org.jacoco", "org.jacoco.core", "0.8.2",
                "977b33afe2344a9ee801fd3317c54d8e1f9d7a79");
        downloadUsingMaven("ext/org.jacoco.report-0.8.2.jar",
                "org.jacoco", "org.jacoco.report", "0.8.2",
                "50e133cdfd2d31ca5702b73615be70f801d3ae26");
        downloadUsingMaven("ext/asm-commons-7.0.jar",
                "org.ow2.asm", "asm-commons", "7.0",
                "478006d07b7c561ae3a92ddc1829bca81ae0cdd1");
        downloadUsingMaven("ext/asm-tree-7.0.jar",
                "org.ow2.asm", "asm-tree", "7.0",
                "29bc62dcb85573af6e62e5b2d735ef65966c4180");
        downloadUsingMaven("ext/args4j-2.33.jar",
                "args4j", "args4j", "2.33",
                "bd87a75374a6d6523de82fef51fc3cfe9baf9fc9");

        delete(files("coverage"));
        // Use own copy
        copy("coverage/bin", files("temp"), "temp");
        // JaCoCo does not support multiple versions of the same classes
        delete(files("coverage/bin/META-INF/versions"));
        String cp = "coverage/bin" +
            File.pathSeparator + "ext/postgresql-42.2.5.jre7" +
            File.pathSeparator + "ext/servlet-api-3.1.0.jar" +
            File.pathSeparator + "ext/lucene-core-5.5.5.jar" +
            File.pathSeparator + "ext/lucene-analyzers-common-5.5.5.jar" +
            File.pathSeparator + "ext/lucene-queryparser-5.5.5.jar" +
            File.pathSeparator + "ext/h2mig_pagestore_addon.jar" +
            File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
            File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
            File.pathSeparator + "ext/jts-core-1.15.0.jar" +
            File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
            File.pathSeparator + "ext/slf4j-nop-1.6.0.jar" +
            File.pathSeparator + javaToolsJar;
        // Run tests
        execJava(args(
                "-Xmx128m",
                "-javaagent:ext/jacocoagent.jar=destfile=coverage/jacoco.exec,"
                        + "excludes=org.h2.test.*:org.h2.tools.*:org.h2.sample.*",
                "-cp", cp,
                "org.h2.test.TestAll", "codeCoverage"));
        // Remove classes that we don't want to include in report
        delete(files("coverage/bin/org/h2/test"));
        delete(files("coverage/bin/org/h2/tools"));
        delete(files("coverage/bin/org/h2/sample"));
        // Generate report
        execJava(args("-cp",
                "ext/org.jacoco.cli-0.8.2.jar" + File.pathSeparator
                + "ext/org.jacoco.core-0.8.2.jar" + File.pathSeparator
                + "ext/org.jacoco.report-0.8.2.jar" + File.pathSeparator
                + "ext/asm-7.0.jar" + File.pathSeparator
                + "ext/asm-commons-7.0.jar" + File.pathSeparator
                + "ext/asm-tree-7.0.jar" + File.pathSeparator
                + "ext/args4j-2.33.jar",
                "org.jacoco.cli.internal.Main", "report", "coverage/jacoco.exec",
                "--classfiles", "coverage/bin",
                "--html", "coverage/report", "--sourcefiles", "h2/src/main"));
        try {
            tryOpenCoverageInBrowser();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private static void tryOpenCoverageInBrowser() throws Exception {
        Class<?> desktop = Class.forName("java.awt.Desktop");
        Method m = desktop.getMethod("getDesktop");
        Object d = m.invoke(null);
        m = d.getClass().getMethod("open", File.class);
        m.invoke(d, new File("coverage/report/index.html"));
    }

    private static String getTargetJavaVersion() {
        return System.getProperty("version");
    }

    private void compileMVStore(boolean debugInfo) {
        clean();
        mkdir("temp");
        String classpath = "temp";
        FileList files;
        files = files("src/main/org/h2/mvstore").
                exclude("src/main/org/h2/mvstore/db/*");
        StringList args = args();
        if (debugInfo) {
            args = args.plus("-Xlint:unchecked", "-d", "temp", "-sourcepath",
                    "src/main", "-classpath", classpath);
        } else {
            args = args.plus("-Xlint:unchecked", "-g:none", "-d", "temp",
                    "-sourcepath", "src/main", "-classpath", classpath);
        }
        String version = getTargetJavaVersion();
        if (version != null) {
            args = args.plus("-target", version, "-source", version);
        }
        javac(args, files);
    }

    private void compile(boolean debugInfo, boolean clientOnly,
            boolean basicResourcesOnly) {
        clean();
        mkdir("temp");
        download();
        String classpath = "temp" +
                File.pathSeparator + "ext/servlet-api-3.1.0.jar" +
                File.pathSeparator + "ext/lucene-core-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-analyzers-common-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-queryparser-5.5.5.jar" +
                File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
                File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
                File.pathSeparator + "ext/jts-core-1.15.0.jar" +
                File.pathSeparator + "ext/asm-7.0.jar" +
                File.pathSeparator + javaToolsJar;
        FileList files;
        if (clientOnly) {
            files = files("src/main/org/h2/Driver.java");
            files.addAll(files("src/main/org/h2/jdbc"));
            files.addAll(files("src/main/org/h2/jdbcx"));
        } else {
            files = files("src/main");
        }
        StringList args = args();
        if (debugInfo) {
            args = args.plus("-Xlint:unchecked",
                    "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath);
        } else {
            args = args.plus("-Xlint:unchecked", "-g:none",
                    "-d", "temp", "-sourcepath", "src/main", "-classpath", classpath);
        }
        String version = getTargetJavaVersion();
        if (version != null) {
            args = args.plus("-target", version, "-source", version);
        }
        javac(args, files);

        files = files("src/main/META-INF/services");
        copy("temp", files, "src/main");

        if (!clientOnly) {
            files = files("src/test");
            files.addAll(files("src/tools"));
            //we don't use Junit for this test framework
            files = files.exclude("src/test/org/h2/test/TestAllJunit.java");
            args = args("-Xlint:unchecked", "-Xlint:deprecation",
                    "-d", "temp", "-sourcepath", "src/test" + File.pathSeparator + "src/tools",
                    "-classpath", classpath);
            if (version != null) {
                args = args.plus("-target", version, "-source", version);
            }
            javac(args, files);
            files = files("src/test").
                exclude("*.java").
                exclude("*/package.html");
            copy("temp", files, "src/test");
        }
        resources(clientOnly, basicResourcesOnly);
    }

    private static void filter(String source, String target, String old,
            String replacement) {
        String text = new String(readFile(new File(source)));
        text = replaceAll(text, old, replacement);
        writeFile(new File(target), text.getBytes());
    }

    /**
     * Create the documentation from the documentation sources. API Javadocs are
     * created as well.
     */
    @Description(summary = "Create the documentation from sources (incl. API Javadocs).")
    public void docs() {
        javadoc();
        copy("docs", files("src/docsrc/index.html"), "src/docsrc");
        java("org.h2.build.doc.XMLChecker", null);
        java("org.h2.build.code.CheckJavadoc", null);
        java("org.h2.build.code.CheckTextFiles", null);
        java("org.h2.build.doc.GenerateDoc", null);
        java("org.h2.build.doc.GenerateHelp", null);
        java("org.h2.build.i18n.PrepareTranslation", null);
        java("org.h2.build.indexer.Indexer", null);
        java("org.h2.build.doc.MergeDocs", null);
        java("org.h2.build.doc.WebSite", null);
        java("org.h2.build.doc.LinkChecker", null);
        java("org.h2.build.doc.XMLChecker", null);
        java("org.h2.build.doc.SpellChecker", null);
        java("org.h2.build.code.CheckTextFiles", null);
        beep();
    }

    /**
     * Download all required jar files. Actually those are only compile time
     * dependencies. The database can be used without any dependencies.
     */
    @Description(summary = "Download all required jar files.")
    public void download() {
        downloadOrVerify(false);
    }

    private void downloadOrVerify(boolean offline) {
        downloadOrVerify("ext/servlet-api-3.1.0.jar",
                "javax/servlet", "javax.servlet-api", "3.1.0",
                "3cd63d075497751784b2fa84be59432f4905bf7c", offline);
        downloadOrVerify("ext/lucene-core-5.5.5.jar",
                "org/apache/lucene", "lucene-core", "5.5.5",
                "c34bcd9274859dc07cfed2a935aaca90c4f4b861", offline);
        downloadOrVerify("ext/lucene-analyzers-common-5.5.5.jar",
                "org/apache/lucene", "lucene-analyzers-common", "5.5.5",
                "e6b3f5d1b33ed24da7eef0a72f8062bd4652700c", offline);
        downloadOrVerify("ext/lucene-queryparser-5.5.5.jar",
                "org/apache/lucene", "lucene-queryparser", "5.5.5",
                "6c965eb5838a2ba58b0de0fd860a420dcda11937", offline);
        downloadOrVerify("ext/slf4j-api-1.6.0.jar",
                "org/slf4j", "slf4j-api", "1.6.0",
                "b353147a7d51fcfcd818d8aa6784839783db0915", offline);
        downloadOrVerify("ext/org.osgi.core-4.2.0.jar",
                "org/osgi", "org.osgi.core", "4.2.0",
                "66ab449ff3aa5c4adfc82c89025cc983b422eb95", offline);
        downloadOrVerify("ext/org.osgi.enterprise-4.2.0.jar",
                "org/osgi", "org.osgi.enterprise", "4.2.0",
                "8634dcb0fc62196e820ed0f1062993c377f74972", offline);
        downloadOrVerify("ext/jts-core-1.15.0.jar",
                "org/locationtech/jts", "jts-core", "1.15.0",
                "705981b7e25d05a76a3654e597dab6ba423eb79e", offline);
        downloadOrVerify("ext/junit-4.12.jar",
                "junit", "junit", "4.12",
                "2973d150c0dc1fefe998f834810d68f278ea58ec", offline);
        downloadUsingMaven("ext/asm-7.0.jar",
                "org.ow2.asm", "asm", "7.0",
                "d74d4ba0dee443f68fb2dcb7fcdb945a2cd89912");
    }

    private void downloadOrVerify(String target, String group, String artifact,
            String version, String sha1Checksum, boolean offline) {
        if (offline) {
            File targetFile = new File(target);
            if (targetFile.exists()) {
                return;
            }
            println("Missing file: " + target);
            filesMissing = true;
        } else {
            downloadUsingMaven(target, group, artifact, version, sha1Checksum);
        }
    }

    private void downloadTest() {
        // for TestUpgrade
        download("ext/h2mig_pagestore_addon.jar",
                "http://h2database.com/h2mig_pagestore_addon.jar",
                "6dfafe1b86959c3ba4f7cf03e99535e8b9719965");
        // for TestOldVersion
        downloadUsingMaven("ext/h2-1.2.127.jar",
                "com/h2database", "h2", "1.2.127",
                "056e784c7cf009483366ab9cd8d21d02fe47031a");
        // for TestPgServer
        downloadUsingMaven("ext/postgresql-42.2.5.jre7.jar",
                "org.postgresql", "postgresql", "42.2.5.jre7",
                "ec74f6f7885b7e791f84c7219a97964e9d0121e4");
        // for TestTraceSystem
        downloadUsingMaven("ext/slf4j-nop-1.6.0.jar",
                "org/slf4j", "slf4j-nop", "1.6.0",
                "4da67bb4a6eea5dc273f99c50ad2333eadb46f86");
    }

    private static String getVersion() {
        return getStaticValue("org.h2.engine.Constants", "getVersion");
    }

    private static String getJarSuffix() {
        return "-" + getVersion() + ".jar";
    }

    /**
     * Create the h2.zip file and the Windows installer.
     */
    @Description(summary = "Create the h2.zip file and the Windows installer.")
    public void installer() {
        delete(files("bin").keep("*.jar"));
        jar();
        docs();
        try {
            exec("soffice", args("-invisible", "macro:///Standard.Module1.H2Pdf"));
            copy("docs", files("../h2web/h2.pdf"), "../h2web");
        } catch (Exception e) {
            println("OpenOffice / LibreOffice is not available or macros H2Pdf is not installed:");
            println(e.toString());
            println("********************************************************************************");
            println("Install and run LibreOffice or OpenOffice.");
            println("Open Tools - Macros - Organize Macros - LibreOffice Basic...");
            println("Navigate to My Macros / Standard / Module1 and press Edit button.");
            println("Put content of h2/src/installer/openoffice.txt here.");
            println("Edit BaseDir variable value:");

            println("    BaseDir = \"" + new File(System.getProperty("user.dir")).getParentFile().toURI() + '"');
            println("Close office application and try to build installer again.");
            println("********************************************************************************");
        }
        delete("docs/html/onePage.html");
        FileList files = files("../h2").keep("../h2/build.*");
        files.addAll(files("../h2/bin").keep("../h2/bin/h2*"));
        files.addAll(files("../h2/docs").exclude("*.jar"));
        files.addAll(files("../h2/service"));
        files.addAll(files("../h2/src"));
        zip("../h2web/h2.zip", files, "../", false, false);
        boolean installer = false;
        try {
            exec("makensis", args("/v2", "src/installer/h2.nsi"));
            installer = true;
        } catch (Exception e) {
            println("NSIS is not available: " + e);
        }
        String buildDate = getStaticField("org.h2.engine.Constants", "BUILD_DATE");
        byte[] data = readFile(new File("../h2web/h2.zip"));
        String sha1Zip = getSHA1(data), sha1Exe = null;
        writeFile(new File("../h2web/h2-" + buildDate + ".zip"), data);
        if (installer) {
            data = readFile(new File("../h2web/h2-setup.exe"));
            sha1Exe = getSHA1(data);
            writeFile(new File("../h2web/h2-setup-" + buildDate + ".exe"), data);
        }
        updateChecksum("../h2web/html/download.html", sha1Zip, sha1Exe);
    }

    private static void updateChecksum(String fileName, String sha1Zip,
            String sha1Exe) {
        String checksums = new String(readFile(new File(fileName)));
        checksums = replaceAll(checksums, "<!-- sha1Zip -->",
                "(SHA1 checksum: " + sha1Zip + ")");
        if (sha1Exe != null) {
            checksums = replaceAll(checksums, "<!-- sha1Exe -->",
                    "(SHA1 checksum: " + sha1Exe + ")");
        }
        writeFile(new File(fileName), checksums.getBytes());
    }

    private static String canonicalPath(File file) {
        try {
            return file.getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private FileList excludeTestMetaInfFiles(FileList files) {
        FileList testMetaInfFiles = files("src/test/META-INF");
        int basePathLength = canonicalPath(new File("src/test")).length();
        for (File file : testMetaInfFiles) {
            files = files.exclude(canonicalPath(file).substring(basePathLength + 1));
        }
        return files;
    }

    /**
     * Add META-INF/versions for Java 9+.
     *
     * @param includeCurrentTimestamp include CurrentTimestamp implementation
     */
    private void addVersions(boolean includeCurrentTimestamp) {
        copy("temp/META-INF/versions/9", files("src/java9/precompiled"), "src/java9/precompiled");
        if (!includeCurrentTimestamp) {
            delete(files("temp/META-INF/versions/9/org/h2/util/CurrentTimestamp.class"));
        }
    }

    /**
     * Create the regular h2.jar file.
     */
    @Description(summary = "Create the regular h2.jar file.")
    public void jar() {
        compile();
        addVersions(true);
        manifest("src/main/META-INF/MANIFEST.MF");
        FileList files = files("temp").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/java/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/server/ftp/*").
            exclude("temp/org/h2/test/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt").
            exclude("*.DS_Store");
        files = excludeTestMetaInfFiles(files);
        jar("bin/h2" + getJarSuffix(), files, "temp");
        filter("src/installer/h2.sh", "bin/h2.sh", "h2.jar", "h2" + getJarSuffix());
        filter("src/installer/h2.bat", "bin/h2.bat", "h2.jar", "h2" + getJarSuffix());
        filter("src/installer/h2w.bat", "bin/h2w.bat", "h2.jar", "h2" + getJarSuffix());
    }

    /**
     * Create the h2client.jar. This only contains the remote JDBC
     * implementation.
     */
    @Description(summary = "Create h2client.jar with only the remote JDBC implementation.")
    public void jarClient() {
        compile(true, true, false);
        addVersions(false);
        manifest("src/installer/client/MANIFEST.MF");
        FileList files = files("temp").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/java/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/mode/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/test/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt").
            exclude("*.DS_Store");
        files = excludeTestMetaInfFiles(files);
        long kb = jar("bin/h2-client" + getJarSuffix(), files, "temp");
        if (kb < 400 || kb > 600) {
            throw new RuntimeException("Expected file size 400 - 600 KB, got: " + kb);
        }
    }

    /**
     * Create the file h2mvstore.jar. This only contains the MVStore.
     */
    @Description(summary = "Create h2mvstore.jar containing only the MVStore.")
    public void jarMVStore() {
        compileMVStore(true);
        addVersions(false);
        manifest("src/installer/mvstore/MANIFEST.MF");
        FileList files = files("temp");
        files.exclude("*.DS_Store");
        files = excludeTestMetaInfFiles(files);
        jar("bin/h2-mvstore" + getJarSuffix(), files, "temp");
    }

    /**
     * Create the file h2small.jar. This only contains the embedded database.
     * Debug information is disabled.
     */
    @Description(summary = "Create h2small.jar containing only the embedded database.")
    public void jarSmall() {
        compile(false, false, true);
        addVersions(true);
        manifest("src/installer/small/MANIFEST.MF");
        FileList files = files("temp").
            exclude("temp/org/h2/build/*").
            exclude("temp/org/h2/dev/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/java/*").
            exclude("temp/org/h2/jcr/*").
            exclude("temp/org/h2/samples/*").
            exclude("temp/org/h2/server/ftp/*").
            exclude("temp/org/h2/test/*").
            exclude("temp/org/h2/bnf/*").
            exclude("temp/org/h2/fulltext/*").
            exclude("temp/org/h2/jdbcx/*").
            exclude("temp/org/h2/jmx/*").
            exclude("temp/org/h2/server/*").
            exclude("temp/org/h2/tools/*").
            exclude("*.bat").
            exclude("*.sh").
            exclude("*.txt").
            exclude("*.DS_Store");
        files = excludeTestMetaInfFiles(files);
        files.add(new File("temp/org/h2/tools/DeleteDbFiles.class"));
        files.add(new File("temp/org/h2/tools/CompressTool.class"));
        jar("bin/h2small" + getJarSuffix(), files, "temp");
    }

    /**
     * Create the Javadocs of the API (incl. the JDBC API) and tools.
     */
    @Description(summary = "Create the API Javadocs (incl. JDBC API and tools).")
    public void javadoc() {
        compileTools();
        delete("docs");
        mkdir("docs/javadoc");
        javadoc("-sourcepath", "src/main", "org.h2.jdbc", "org.h2.jdbcx",
                "org.h2.tools", "org.h2.api", "org.h2.engine", "org.h2.fulltext",
                "-classpath",
                "ext/lucene-core-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-analyzers-common-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-queryparser-5.5.5.jar" +
                File.pathSeparator + "ext/jts-core-1.15.0.jar",
                "-docletpath", "bin" + File.pathSeparator + "temp",
                "-doclet", "org.h2.build.doclet.Doclet");
        copy("docs/javadoc", files("src/docsrc/javadoc"), "src/docsrc/javadoc");
    }

    /**
     * Create the Javadocs of the implementation.
     */
    @Description(summary = "Create the Javadocs of the implementation.")
    public void javadocImpl() {
        compileTools();
        mkdir("docs/javadocImpl2");
        javadoc("-sourcepath", "src/main" +
                // need to be disabled if not enough memory
                // File.pathSeparator + "src/test" +
                File.pathSeparator + "src/tools",
                // need to be disabled for java 7
                // "-Xdoclint:none",
                "-noindex",
                "-tag", "h2.resource",
                "-d", "docs/javadocImpl2",
                "-classpath", javaToolsJar +
                File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
                File.pathSeparator + "ext/servlet-api-3.1.0.jar" +
                File.pathSeparator + "ext/lucene-core-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-analyzers-common-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-queryparser-5.5.5.jar" +
                File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
                File.pathSeparator + "ext/jts-core-1.15.0.jar" +
                File.pathSeparator + "ext/asm-7.0.jar" +
                File.pathSeparator + "ext/junit-4.12.jar",
                "-subpackages", "org.h2");

        mkdir("docs/javadocImpl3");
        javadoc("-sourcepath", "src/main",
                "-noindex",
                "-tag", "h2.resource",
                "-d", "docs/javadocImpl3",
                "-classpath", javaToolsJar +
                File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
                File.pathSeparator + "ext/servlet-api-3.1.0.jar" +
                File.pathSeparator + "ext/lucene-core-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-analyzers-common-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-queryparser-5.5.5.jar" +
                File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
                File.pathSeparator + "ext/jts-core-1.15.0.jar",
                "-subpackages", "org.h2.mvstore",
                "-exclude", "org.h2.mvstore.db");

        System.setProperty("h2.interfacesOnly", "false");
        System.setProperty("h2.javadocDestDir", "docs/javadocImpl");
        javadoc("-sourcepath", "src/main" +
                File.pathSeparator + "src/test" +
                File.pathSeparator + "src/tools",
                "-classpath", javaToolsJar +
                File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
                File.pathSeparator + "ext/servlet-api-3.1.0.jar" +
                File.pathSeparator + "ext/lucene-core-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-analyzers-common-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-queryparser-5.5.5.jar" +
                File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
                File.pathSeparator + "ext/jts-core-1.15.0.jar" +
                File.pathSeparator + "ext/asm-7.0.jar" +
                File.pathSeparator + "ext/junit-4.12.jar",
                "-subpackages", "org.h2",
                "-package",
                "-docletpath", "bin" + File.pathSeparator + "temp",
                "-doclet", "org.h2.build.doclet.Doclet");
        copy("docs/javadocImpl", files("src/docsrc/javadoc"), "src/docsrc/javadoc");
    }

    private static void manifest(String path) {
        String manifest = new String(readFile(new File(path)), StandardCharsets.UTF_8);
        manifest = replaceAll(manifest, "${version}", getVersion());
        manifest = replaceAll(manifest, "${buildJdk}", getJavaSpecVersion());
        String createdBy = System.getProperty("java.runtime.version") +
            " (" + System.getProperty("java.vm.vendor") + ")";
        manifest = replaceAll(manifest, "${createdBy}", createdBy);
        mkdir("temp/META-INF");
        writeFile(new File("temp/META-INF/MANIFEST.MF"), manifest.getBytes());
    }

    /**
     * This will build a release of the H2 .jar files and upload it to
     * file:///data/h2database/m2-repo. This is only required when
     * a new H2 version is made.
     */
    @Description(summary = "Build H2 release jars and upload to file:///data/h2database/m2-repo.")
    public void mavenDeployCentral() {
        // generate and deploy h2*-sources.jar file
        FileList files = files("src/main");
        copy("docs", files, "src/main");
        files = files("docs").keep("docs/org/*").keep("*.java");
        files.addAll(files("docs").keep("docs/META-INF/*"));
        String manifest = new String(readFile(new File(
                "src/installer/source-manifest.mf")));
        manifest = replaceAll(manifest, "${version}", getVersion());
        writeFile(new File("docs/META-INF/MANIFEST.MF"), manifest.getBytes());
        jar("docs/h2-" + getVersion() + "-sources.jar", files, "docs");
        delete("docs/org");
        delete("docs/META-INF");
        // the option -DgeneratePom=false doesn't work with some versions of
        // Maven because of bug http://jira.codehaus.org/browse/MDEPLOY-84
        // as a workaround we generate the pom, but overwrite it later on
        // (that's why the regular jar is created at the very end)
        execScript("mvn", args(
                "deploy:deploy-file",
                "-Dfile=docs/h2-" + getVersion() + "-sources.jar",
                "-Durl=file:///data/h2database/m2-repo",
                "-Dpackaging=jar",
                "-Dclassifier=sources",
                "-Dversion=" + getVersion(),
                "-DartifactId=h2",
                "-DgroupId=com.h2database"
                // ,"-DgeneratePom=false"
                ));

        // generate and deploy the h2*-javadoc.jar file
        javadocImpl();
        files = files("docs/javadocImpl2");
        jar("docs/h2-" + getVersion() + "-javadoc.jar", files, "docs/javadocImpl2");
        execScript("mvn", args(
                "deploy:deploy-file",
                "-Dfile=docs/h2-" + getVersion() + "-javadoc.jar",
                "-Durl=file:///data/h2database/m2-repo",
                "-Dpackaging=jar",
                "-Dclassifier=javadoc",
                "-Dversion=" + getVersion(),
                "-DartifactId=h2",
                "-DgroupId=com.h2database"
                // ,"-DgeneratePom=false"
                ));

        // generate and deploy the h2*.jar file
        jar();
        String pom = new String(readFile(new File("src/installer/pom-template.xml")));
        pom = replaceAll(pom, "@version@", getVersion());
        writeFile(new File("bin/pom.xml"), pom.getBytes());
        execScript("mvn", args(
                "deploy:deploy-file",
                "-Dfile=bin/h2" + getJarSuffix(),
                "-Durl=file:///data/h2database/m2-repo",
                "-Dpackaging=jar",
                "-Dversion=" + getVersion(),
                "-DpomFile=bin/pom.xml",
                "-DartifactId=h2",
                "-DgroupId=com.h2database"));

        // generate the h2-mvstore-*-sources.jar file
        files = files("src/main");
        copy("docs", files, "src/main");
        files = files("docs").keep("docs/org/h2/mvstore/*").
                exclude("docs/org/h2/mvstore/db/*").
                keep("*.java");
        files.addAll(files("docs").keep("docs/META-INF/*"));
        manifest = new String(readFile(new File(
                "src/installer/source-manifest.mf")));
        manifest = replaceAll(manifest, "${version}", getVersion());
        writeFile(new File("docs/META-INF/MANIFEST.MF"), manifest.getBytes());
        jar("docs/h2-mvstore-" + getVersion() + "-sources.jar", files, "docs");
        delete("docs/org");
        delete("docs/META-INF");

        // deploy the h2-mvstore-*-source.jar file
        execScript("mvn", args(
                "deploy:deploy-file",
                "-Dfile=docs/h2-mvstore-" + getVersion() + "-sources.jar",
                "-Durl=file:///data/h2database/m2-repo",
                "-Dpackaging=jar",
                "-Dclassifier=sources",
                "-Dversion=" + getVersion(),
                "-DartifactId=h2-mvstore",
                "-DgroupId=com.h2database"
                // ,"-DgeneratePom=false"
                ));

        // generate and deploy the h2-mvstore-*-javadoc.jar file
        javadocImpl();
        files = files("docs/javadocImpl3");
        jar("docs/h2-mvstore-" + getVersion() + "-javadoc.jar", files, "docs/javadocImpl3");
        execScript("mvn", args(
                "deploy:deploy-file",
                "-Dfile=docs/h2-mvstore-" + getVersion() + "-javadoc.jar",
                "-Durl=file:///data/h2database/m2-repo",
                "-Dpackaging=jar",
                "-Dclassifier=javadoc",
                "-Dversion=" + getVersion(),
                "-DartifactId=h2-mvstore",
                "-DgroupId=com.h2database"
                // ,"-DgeneratePom=false"
                ));

        // generate and deploy the h2-mvstore-*.jar file
        jarMVStore();
        pom = new String(readFile(new File("src/installer/pom-mvstore-template.xml")));
        pom = replaceAll(pom, "@version@", getVersion());
        writeFile(new File("bin/pom.xml"), pom.getBytes());
        execScript("mvn", args(
                "deploy:deploy-file",
                "-Dfile=bin/h2-mvstore" + getJarSuffix(),
                "-Durl=file:///data/h2database/m2-repo",
                "-Dpackaging=jar",
                "-Dversion=" + getVersion(),
                "-DpomFile=bin/pom.xml",
                "-DartifactId=h2-mvstore",
                "-DgroupId=com.h2database"));
    }

    /**
     * This will build a 'snapshot' H2 .jar file and upload it to the local
     * Maven 2 repository.
     */
    @Description(summary = "Build a snapshot H2 jar and upload to local Maven 2 repo.")
    public void mavenInstallLocal() {
        // MVStore
        jarMVStore();
        String pom = new String(readFile(new File("src/installer/pom-mvstore-template.xml")));
        pom = replaceAll(pom, "@version@", "1.0-SNAPSHOT");
        writeFile(new File("bin/pom.xml"), pom.getBytes());
        execScript("mvn", args(
                "install:install-file",
                "-Dversion=1.0-SNAPSHOT",
                "-Dfile=bin/h2-mvstore" + getJarSuffix(),
                "-Dpackaging=jar",
                "-DpomFile=bin/pom.xml",
                "-DartifactId=h2-mvstore",
                "-DgroupId=com.h2database"));
        // database
        jar();
        pom = new String(readFile(new File("src/installer/pom-template.xml")));
        pom = replaceAll(pom, "@version@", "1.0-SNAPSHOT");
        writeFile(new File("bin/pom.xml"), pom.getBytes());
        execScript("mvn", args(
                "install:install-file",
                "-Dversion=1.0-SNAPSHOT",
                "-Dfile=bin/h2" + getJarSuffix(),
                "-Dpackaging=jar",
                "-DpomFile=bin/pom.xml",
                "-DartifactId=h2",
                "-DgroupId=com.h2database"));
    }

    /**
     * Build the jar file without downloading any files over the network. If the
     * required files are missing, they are listed, and the jar file is not
     * built.
     */
    @Description(summary = "Build H2 jar avoiding downloads (list missing files).")
    public void offline() {
        downloadOrVerify(true);
        if (filesMissing) {
            println("Required files are missing");
        } else {
            jar();
        }
    }

    private void resources(boolean clientOnly, boolean basicOnly) {
        if (!clientOnly) {
            java("org.h2.build.doc.GenerateHelp", null);
            javadoc("-sourcepath", "src/main", "org.h2.tools", "org.h2.jmx",
                    "-classpath",
                    "ext/lucene-core-5.5.5.jar" +
                    File.pathSeparator + "ext/lucene-analyzers-common-5.5.5.jar" +
                    File.pathSeparator + "ext/lucene-queryparser-5.5.5.jar" +
                    File.pathSeparator + "ext/jts-core-1.15.0.jar",
                    "-docletpath", "bin" + File.pathSeparator + "temp",
                    "-doclet", "org.h2.build.doclet.ResourceDoclet");
        }
        FileList files = files("src/main").
            exclude("*.MF").
            exclude("*.java").
            exclude("*/package.html").
            exclude("*/java.sql.Driver").
            exclude("*.DS_Store");
        if (basicOnly) {
            files = files.keep("src/main/org/h2/res/_messages_en.*");
        }
        if (clientOnly) {
            files = files.exclude("src/main/org/h2/res/help.csv");
            files = files.exclude("src/main/org/h2/res/h2*");
            files = files.exclude("src/main/org/h2/res/javadoc.properties");
            files = files.exclude("src/main/org/h2/server/*");
        }
        zip("temp/org/h2/util/data.zip", files, "src/main", true, false);
    }

    /**
     * Just run the spellchecker.
     */
    @Description(summary = "Run the spellchecker.")
    public void spellcheck() {
        java("org.h2.build.doc.SpellChecker", null);
    }

    /**
     * Compile and run all tests. This does not include the compile step.
     */
    @Description(summary = "Compile and run all tests (excluding the compile step).")
    public void test() {
        test(false);
    }

    /**
     * Compile and run all fast tests. This does not include the compile step.
     */
    @Description(summary = "Compile and run all tests for Travis (excl. the compile step).")
    public void testTravis() {
        test(true);
    }

    private void test(boolean travis) {
        downloadTest();
        String cp = "temp" + File.pathSeparator + "bin" +
                File.pathSeparator + "ext/postgresql-42.2.5.jre7.jar" +
                File.pathSeparator + "ext/servlet-api-3.1.0.jar" +
                File.pathSeparator + "ext/lucene-core-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-analyzers-common-5.5.5.jar" +
                File.pathSeparator + "ext/lucene-queryparser-5.5.5.jar" +
                File.pathSeparator + "ext/h2mig_pagestore_addon.jar" +
                File.pathSeparator + "ext/org.osgi.core-4.2.0.jar" +
                File.pathSeparator + "ext/org.osgi.enterprise-4.2.0.jar" +
                File.pathSeparator + "ext/jts-core-1.15.0.jar" +
                File.pathSeparator + "ext/slf4j-api-1.6.0.jar" +
                File.pathSeparator + "ext/slf4j-nop-1.6.0.jar" +
                File.pathSeparator + "ext/asm-7.0.jar" +
                File.pathSeparator + javaToolsJar;
        int version = getJavaVersion();
        if (version >= 9) {
            cp = "src/java9/precompiled" + File.pathSeparator + cp;
        }
        int ret;
        if (travis) {
            ret = execJava(args(
                    "-ea",
                    "-Xmx128m",
                    "-XX:MaxDirectMemorySize=2g",
                    "-cp", cp,
                    "org.h2.test.TestAll", "travis"));
        } else {
            ret = execJava(args(
                    "-ea",
                    "-Xmx128m",
                    "-cp", cp,
                    "org.h2.test.TestAll"));
        }
        // return a failure code for Jenkins/Travis/CI builds
        if (ret != 0) {
            System.exit(ret);
        }
    }

    /**
     * Print the system properties.
     */
    @Description(summary = "Print the system properties.")
    public void testSysProperties() {
        System.out.println("environment settings:");
        for (Entry<Object, Object> e : new TreeMap<>(
                System.getProperties()).entrySet()) {
            System.out.println(e);
        }
    }

    /**
     * Test the local network of this machine.
     */
    @Description(summary = "Test the local network of this machine.")
    public void testNetwork() {
        try {
            long start = System.nanoTime();
            System.out.println("localhost:");
            System.out.println("  " + InetAddress.getByName("localhost"));
            for (InetAddress address : InetAddress.getAllByName("localhost")) {
                System.out.println("  " + address);
            }
            InetAddress localhost = InetAddress.getLocalHost();
            System.out.println("getLocalHost:" + localhost);
            for (InetAddress address : InetAddress.getAllByName(localhost
                    .getHostAddress())) {
                System.out.println("  " + address);
            }
            InetAddress address = InetAddress.getByName(localhost.getHostAddress());
            System.out.println("byName:" + address);
            ServerSocket serverSocket;
            try {
                serverSocket = new ServerSocket(0);
            } catch (Exception e) {
                e.printStackTrace();
                serverSocket = new ServerSocket(0);
            }
            System.out.println(serverSocket);
            int port = serverSocket.getLocalPort();
            final ServerSocket accept = serverSocket;
            start = System.nanoTime();
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        System.out.println("server accepting");
                        Socket s = accept.accept();
                        Thread.sleep(100);
                        System.out.println("server accepted:" + s);
                        System.out.println("server read:" + s.getInputStream().read());
                        Thread.sleep(200);
                        s.getOutputStream().write(234);
                        Thread.sleep(100);
                        System.out.println("server closing");
                        s.close();
                        System.out.println("server done");
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            };
            thread.start();
            System.out.println("time: " +
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            Thread.sleep(1000);
            start = System.nanoTime();
            final Socket socket = new Socket();
            socket.setSoTimeout(2000);
            final InetSocketAddress socketAddress = new InetSocketAddress(address, port);
            System.out.println("client:" + socketAddress);
            try {
                Thread t = new Thread() {
                    @Override
                    public void run() {
                        try {
                            socket.connect(socketAddress, 2000);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                };
                t.start();
                t.join(5000);
                if (!socket.isConnected()) {
                    final InetSocketAddress localhostAddress = new InetSocketAddress(
                            "localhost", port);
                    System.out.println("not connected, trying localhost:"
                            + socketAddress);
                    socket.connect(localhostAddress, 2000);
                }
                System.out.println("time: " +
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                Thread.sleep(200);
                start = System.nanoTime();
                System.out.println("client:" + socket.toString());
                socket.getOutputStream().write(123);
                System.out.println("time: " +
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                Thread.sleep(100);
                start = System.nanoTime();
                System.out.println("client read:" + socket.getInputStream().read());
                socket.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
            thread.join(5000);
            System.out.println("time: " +
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            if (thread.isAlive()) {
                System.out.println("thread is still alive, interrupting");
                thread.interrupt();
            }
            Thread.sleep(100);
            System.out.println("done");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This build target is used for the automated build. It copies the result
     * of the automated build (including test results, newsfeed, code coverage)
     * to the public web site.
     */
    @Description(summary = "Upload all build results to the public website.")
    public void uploadBuild() {
        String password = System.getProperty("h2.ftpPassword");
        if (password == null) {
            throw new RuntimeException("h2.ftpPassword not set");
        }
        downloadTest();
        mkdir("temp");
        FileList files = files("src/tools").keep("*/UploadBuild.java");
        StringList args = args("-d", "temp", "-sourcepath", "src/tools" +
                File.pathSeparator + "src/test" + File.pathSeparator + "src/main");
        String version = getTargetJavaVersion();
        if (version != null) {
            args = args.plus("-target", version, "-source", version);
        }
        javac(args, files);
        String cp = "bin" + File.pathSeparator + "temp" +
                File.pathSeparator + "ext/h2mig_pagestore_addon.jar";
        execJava(args("-Xmx512m", "-cp", cp,
                "-Dh2.ftpPassword=" + password,
                "org.h2.build.doc.UploadBuild"));
    }

    /**
     * Build the h2console.war file.
     */
    @Description(summary = "Build the h2console.war file.")
    public void warConsole() {
        jar();
        copy("temp/WEB-INF", files("src/tools/WEB-INF/web.xml"), "src/tools/WEB-INF");
        copy("temp", files("src/tools/WEB-INF/console.html"), "src/tools/WEB-INF");
        copy("temp/WEB-INF/lib", files("bin/h2" + getJarSuffix()), "bin");
        FileList files = files("temp").exclude("temp/org*").exclude("temp/META-INF*");
        files.exclude("*.DS_Store");
        jar("bin/h2console.war", files, "temp");
    }

    @Override
    protected String getLocalMavenDir() {
        String userHome = System.getProperty("user.home", "");
        File file = new File(userHome, ".m2/settings.xml");
        if (!file.exists()) {
            return super.getLocalMavenDir();
        }
        XMLParser p = new XMLParser(new String(BuildBase.readFile(file)));
        HashMap<String, String> prop = new HashMap<>();
        for (String name = ""; p.hasNext();) {
            int event = p.next();
            if (event == XMLParser.START_ELEMENT) {
                name += "/" + p.getName();
            } else if (event == XMLParser.END_ELEMENT) {
                name = name.substring(0, name.lastIndexOf('/'));
            } else if (event == XMLParser.CHARACTERS) {
                String text = p.getText().trim();
                if (text.length() > 0) {
                    prop.put(name, text);
                }
            }
        }
        String local = prop.get("/settings/localRepository");
        if (local == null) {
            local = "${user.home}/.m2/repository";
        }
        local = replaceAll(local, "${user.home}", userHome);
        return local;
    }

}

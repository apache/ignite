/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import com.google.common.base.Joiner;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.GridGgfsPath;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Test of integration with Hadoop client via command line interface.
 */
public class GridHadoopCommandLineTest extends GridCommonAbstractTest {
    /** GGFS instance. */
    private GridGgfsEx ggfs;

    /** */
    private static final String ggfsName = "ggfs";

    /** */
    private static File testWorkDir;

    /** */
    private static String hadoopHome;

    /** */
    private static String hiveHome;

    /** */
    private static File examplesJar;

    /**
     *
     * @param path File name.
     * @param wordCounts Words and counts.
     * @throws Exception If failed.
     */
    private void generateTestFile(File path, Object... wordCounts) throws Exception {
        List<String> wordsArr = new ArrayList<>();

        //Generating
        for (int i = 0; i < wordCounts.length; i += 2) {
            String word = (String) wordCounts[i];
            int cnt = (Integer) wordCounts[i + 1];

            while (cnt-- > 0)
                wordsArr.add(word);
        }

        //Shuffling
        for (int i = 0; i < wordsArr.size(); i++) {
            int j = (int)(Math.random() * wordsArr.size());

            Collections.swap(wordsArr, i, j);
        }

        //Input file preparing
        PrintWriter testInputFileWriter = new PrintWriter(path);

        int j = 0;

        while (j < wordsArr.size()) {
            int i = 5 + (int)(Math.random() * 5);

            List<String> subList = wordsArr.subList(j, Math.min(j + i, wordsArr.size()));
            j += i;

            testInputFileWriter.println(Joiner.on(' ').join(subList));
        }

        testInputFileWriter.close();
    }

    /**
     * Generates two data files to join its with Hive.
     *
     * @throws FileNotFoundException If failed.
     */
    private void generateHiveTestFiles() throws FileNotFoundException {
        Random random = new Random();

        try (PrintWriter writerA = new PrintWriter(new File(testWorkDir, "data-a"));
             PrintWriter writerB = new PrintWriter(new File(testWorkDir, "data-b"))) {
            char sep = '\t';

            int idB = 0;
            int idA = 0;

            for (int i = 0; i < 1000; i++) {
                writerA.print(idA++);
                writerA.print(sep);
                writerA.println(idB);

                writerB.print(idB++);
                writerB.print(sep);
                writerB.println(random.nextInt(1000));

                writerB.print(idB++);
                writerB.print(sep);
                writerB.println(random.nextInt(1000));
            }

            writerA.flush();
            writerB.flush();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        hiveHome = GridSystemProperties.getString("HIVE_HOME");

        assertFalse("HIVE_HOME hasn't been set.", F.isEmpty(hiveHome));

        hadoopHome = GridSystemProperties.getString("HADOOP_HOME");

        assertFalse("HADOOP_HOME hasn't been set.", F.isEmpty(hadoopHome));

        String mapredHome = hadoopHome + "/share/hadoop/mapreduce";

        File[] fileList = new File(mapredHome).listFiles(new FileFilter() {
            @Override public boolean accept(File pathname) {
                return pathname.getName().startsWith("hadoop-mapreduce-examples-") &&
                    pathname.getName().endsWith(".jar");
            }
        });

        assertEquals("Invalid hadoop distribution.", 1, fileList.length);

        examplesJar = fileList[0];

        testWorkDir = Files.createTempDirectory("hadoop-cli-test").toFile();

        U.copy(U.resolveGridGainPath("docs/core-site.gridgain.xml"), new File(testWorkDir, "core-site.xml"), false);
        U.copy(U.resolveGridGainPath("docs/mapred-site.gridgain.xml"), new File(testWorkDir, "mapred-site.xml"), false);

        generateTestFile(new File(testWorkDir, "test-data"), "key1", 100, "key2", 200);

        generateHiveTestFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        U.delete(testWorkDir);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ggfs = (GridGgfsEx)GridGain.start("config/hadoop/default-config.xml").ggfs(ggfsName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * Creates the process build with appropriate environment to run Hadoop CLI.
     *
     * @return Process builder.
     */
    private ProcessBuilder createProcessBuilder() {
        String sep = ":";

        String ggClsPath = GridHadoopJob.class.getProtectionDomain().getCodeSource().getLocation().getPath() + sep +
                GridHadoopJobTracker.class.getProtectionDomain().getCodeSource().getLocation().getPath() + sep +
                ConcurrentHashMap8.class.getProtectionDomain().getCodeSource().getLocation().getPath();

        ProcessBuilder res = new ProcessBuilder();

        res.environment().put("HADOOP_HOME", hadoopHome);
        res.environment().put("HADOOP_CLASSPATH", ggClsPath);
        res.environment().put("HADOOP_CONF_DIR", testWorkDir.getAbsolutePath());

        res.redirectErrorStream(true);

        return res;
    }

    /**
     * Waits for process exit and prints the its output.
     *
     * @param proc Process.
     * @return Exit code.
     * @throws Exception If failed.
     */
    private int watchProcess(Process proc) throws Exception {
        int res = proc.waitFor();

        InputStream in = proc.getInputStream();

        byte[] buf = new byte[in.available()];

        in.read(buf);

        for (String l : new String(buf).split("\n"))
            log().info(l);

        return res;
    }

    /**
     * Executes Hadoop command line tool.
     *
     * @param args Arguments for Hadoop command line tool.
     * @return Process exit code.
     * @throws Exception If failed.
     */
    private int executeHadoopCmd(String... args) throws Exception {
        ProcessBuilder procBuilder = createProcessBuilder();

        List<String> cmd = new ArrayList<>();

        cmd.add(hadoopHome + "/bin/hadoop");
        cmd.addAll(Arrays.asList(args));

        procBuilder.command(cmd);

        log().info("Execute: " + procBuilder.command());

        return watchProcess(procBuilder.start());
    }

    /**
     * Executes Hive query.
     *
     * @param qry Query.
     * @return Process exit code.
     * @throws Exception If failed.
     */
    private int executeHiveQuery(String qry) throws Exception {
        ProcessBuilder procBuilder = createProcessBuilder();

        List<String> cmd = new ArrayList<>();

        procBuilder.command(cmd);

        cmd.add(hiveHome + "/bin/hive");

        cmd.add("--hiveconf");
        cmd.add("hive.rpc.query.plan=true");

        cmd.add("--hiveconf");
        cmd.add("javax.jdo.option.ConnectionURL=jdbc:derby:" + testWorkDir.getAbsolutePath() + "/metastore_db;" +
            "databaseName=metastore_db;create=true");

        cmd.add("-e");
        cmd.add(qry);

        procBuilder.command(cmd);

        log().info("Execute: " + procBuilder.command());

        return watchProcess(procBuilder.start());
    }

    /**
     * Tests Hadoop command line integration.
     */
    public void testHadoopCommandLine() throws Exception {
        assertEquals(0, executeHadoopCmd("fs", "-ls", "/"));

        assertEquals(0, executeHadoopCmd("fs", "-mkdir", "/input"));

        assertEquals(0, executeHadoopCmd("fs", "-put", new File(testWorkDir, "test-data").getAbsolutePath(), "/input"));

        assertTrue(ggfs.exists(new GridGgfsPath("/input/test-data")));

        assertEquals(0, executeHadoopCmd("jar", examplesJar.getAbsolutePath(), "wordcount", "/input", "/output"));

        assertTrue(ggfs.exists(new GridGgfsPath("/output")));
    }

    /**
     * Tests Hive integration.
     */
    public void testHiveCommandLine() throws Exception {
        assertEquals(0, executeHiveQuery(
            "create table table_a (" +
                "id_a int," +
                "id_b int" +
            ") " +
            "row format delimited fields terminated by '\\t'" +
            "stored as textfile " +
            "location '/table-a'"
        ));

        assertEquals(0, executeHadoopCmd("fs", "-put", new File(testWorkDir, "data-a").getAbsolutePath(), "/table-a"));

        assertEquals(0, executeHiveQuery(
            "create table table_b (" +
                "id_b int," +
                "rndv int" +
            ") " +
            "row format delimited fields terminated by '\\t'" +
            "stored as textfile " +
            "location '/table-b'"
        ));

        assertEquals(0, executeHadoopCmd("fs", "-put", new File(testWorkDir, "data-b").getAbsolutePath(), "/table-b"));

        assertEquals(0, executeHiveQuery("select * from table_a limit 10"));

        assertEquals(0, executeHiveQuery("select count(id_b) from table_b"));

        assertEquals(0, executeHiveQuery("select * from table_a a inner join table_b b on a.id_b = b.id_b limit 10"));

        assertEquals(0, executeHiveQuery("select count(b.id_b) from table_a a inner join table_b b on a.id_b = b.id_b"));
    }
}

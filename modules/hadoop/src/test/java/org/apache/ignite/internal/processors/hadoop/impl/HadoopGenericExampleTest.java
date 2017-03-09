package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomTextWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorDescriptor;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorJobBase;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.multijvm2.HadoopAbstract2Test;
import org.apache.ignite.testframework.junits.multijvm2.IgniteNodeProxy2;

/**
 * Provides generic framework to run a generic Hadoop example.
 */
public abstract class HadoopGenericExampleTest extends HadoopAbstract2Test {
    /**
     * Utility method to add leading zeroes.
     *
     * @param len The desired length.
     * @param n The number to nullify.
     */
    static String nullifyToLen(int len, int n) {
        String res = String.valueOf(n);

        int zero = len - res.length();

        if (zero <= 0)
            return res;

        StringBuilder sb = new StringBuilder();

        for (int i=0; i<zero; i++)
            sb.append("0");

        return sb.toString() + res;
    }

    /**
     * Class representing Hadoop sample job execution parameters.
     */
    static class FrameworkParameters {
        /** */
        private final int maps;

        /** */
        private final int reduces;

        /** */
        private final String workDir;

        /** */
        private final String user;

        /**
         * The constructor.
         *
         * @param numMaps The number of maps.
         * @param numReduces The number of reduces.
         * @param workDirBase The work directory base.
         * @param user The user name this example is run on behalf of.
         */
        public FrameworkParameters(int numMaps, int numReduces, String workDirBase, String user) {
            this.maps = numMaps;
            this.reduces = numReduces;
            this.workDir = workDirBase;
            this.user = user;
        }

        /**
         * @return The number of maps.
         */
        public int numMaps() {
            return maps;
        }

        /**
         * @return The number of reduces.
         */
        public int reduces() {
            return reduces;
        }

        /**
         * @param exampleName The example name.
         * @return gets the work directory path.
         */
        public String getWorkDir(String exampleName) {
            return workDir + "/" + user + "/" + exampleName;
        }

        /**
         * @return The user name.
         */
        public String user() {
            return user;
        }
    }

    /**
     * Abstract class representing an example.
     */
    static abstract class GenericHadoopExample {
        /** */
        protected final Random random = new Random(0L);

        /**
         * @return Extracts "words" array from class RandomTextWriter.
         */
        public static String[] getWords() {
            try {
                Field wordsField = RandomTextWriter.class.getDeclaredField("words");

                wordsField.setAccessible(true);

                return (String[])wordsField.get(null);
            }
            catch (Throwable t) {
                throw new IgniteException(t);
            }
        }

        /**
         * @param random The random.
         * @param noWords The number of words.
         * @param os The stream.
         * @throws IOException On error.
         */
        public static void generateSentence(Random random, int noWords, OutputStream os) throws IOException {
            String[] words = getWords();

            assertEquals(1000, words.length);

            try (Writer w = new OutputStreamWriter(os)) {
                String space = " ";

                for (int i = 0; i < noWords; ++i) {
                    w.write(words[random.nextInt(words.length)]);

                    w.write(space);

                    if (random.nextInt(13) == 0)
                        w.write("\n");
                }
            }
        }

        /**
         * Gets the example name.
         */
        protected final String name() {
            // Cannot use Class#getSimpleName() because for inner classes it returns empty string.
            String x = tool().getClass().getName();

            int lastDot = x.lastIndexOf('.');

            if (lastDot < 0)
                return x;

            return x.substring(lastDot + 1);
        }

        /**
         * Gets input dir.
         *
         * @param fp Framework parameters.
         * @return The input dir.
         */
        protected final String inDir(FrameworkParameters fp) {
            return fp.getWorkDir(name()) + "/in";
        }

        /**
         * Gets the output dir.
         *
         * @param fp The parameters.
         * @return The output directory.
         */
        protected final String outDir(FrameworkParameters fp) {
            return fp.getWorkDir(name()) + "/out";
        }

        /**
         *  Utility method to generate predictable random text input.
         *
         * @param numFiles Number of data files to generate.
         * @param conf The Job configuration.
         * @param params The execution parameters.
         * @throws IOException On error.
         */
        protected final void generateTextInput(int numFiles, JobConf conf,
            FrameworkParameters params) throws IOException {
            // We cannot directly use Hadoop's RandomTextWriter since it is really random, but here
            // we need definitely reproducible input data.
            try (FileSystem fs = FileSystem.get(conf)) {
                for (int i=0; i<numFiles; i++) {
                    try (OutputStream os = fs.create(new Path(inDir(params) + "/in-" + i), true)) {
                        generateSentence(random, 2000, os);
                    }
                }
            }
        }

        /**
         * Performs pre-execution preparation.
         */
        void prepare(JobConf conf, FrameworkParameters fp) throws Exception {
            // noop
        }

        /**
         * Gets the String parameters to be passed to the Tool upon execution.
         */
        abstract String[] parameters(FrameworkParameters fp);

        /**
         * Gets the tool to execute.
         */
        abstract Tool tool();

        /**
         * Checks example calculation validity.
         */
        abstract void verify(String[] parameters) throws Exception;
    }

    /**
     * @return The example.
     */
    protected abstract GenericHadoopExample example();

    /**
     * Gets base directory.
     * Note that this directory will be completely deleted in the and of the test.
     *
     * @return The base directory.
     */
    protected String getFsBase() {
        return "/tmp/hadoop-test-" + getUser();
    }

    /**
     * Desired number of maps in TeraSort job.
     * @return The number of maps.
     */
    protected int numMaps() {
        return gridCount();
    }

    /**
     * The user to run Hadoop job on behalf of.
     * @return The user to run Hadoop job on behalf of.
     */
    protected String getUser() {
        return System.getProperty("user.name");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteNodeProxy2.stopAll();

        // Delete the files used:
        getFileSystem().delete(new Path(getFsBase()), true);
    }

    /**
     * @return The execution parameters.
     */
    protected FrameworkParameters frameworkParameters() {
        return new FrameworkParameters(numMaps(), gridCount(), getFsBase(), getUser());
    }

    /**
     * Makes preparations to the configuration.
     *
     * @param conf The configuration object.
     */
    protected void prepareConf(Configuration conf) {
        // Simplify the situation using just local file system:
        conf.set("fs.defaultFS", "file:///");

        // Set the Ignite framework and the address:
        conf.set(MRConfig.FRAMEWORK_NAME, "ignite");
        conf.set(MRConfig.MASTER_ADDRESS, "localhost:11211");

        // To execute the sample on local MR engine use this:
        //conf.set(MRConfig.FRAMEWORK_NAME, "local");
        //conf.unset(MRConfig.MASTER_ADDRESS);
    }

    /**
     * Runs the example.
     */
    protected final void runExampleTest() throws Exception {
        X.println("Running the test.");

        final GenericHadoopExample ex = example();

        final JobConf conf = new JobConf();

        prepareConf(conf);

        FrameworkParameters fp = frameworkParameters();

        runExample(conf, fp, ex);
    }

    /**
     * Runs the given example.
     *
     * @param conf Th econfiguration.
     * @param fp The framework parameters.
     * @param ex Th example to run.
     * @throws Exception On error.
     */
    protected final void runExample(JobConf conf, FrameworkParameters fp, GenericHadoopExample ex) throws Exception {
        ex.prepare(conf, fp);

        String[] args = ex.parameters(fp);

        System.out.println("Running Hadoop example [" + ex.name() + "] with " + args.length + " parameters: " +
            Arrays.toString(args));

        int res = ToolRunner.run(conf, ex.tool(), args);

        System.out.println("Return status = " + res);

        assertEquals(0, res);

        System.out.println("Verifying...");

        ex.verify(args);
    }

    /**
     * Original version of method org.apache.hadoop.mapreduce.lib.aggregate
     *     .ValueAggregatorJob#setAggregatorDescriptors(java.lang.Class[]): it adds correct "." to the property name.
     *
     * @param conf The configuration.
     * @param descriptors The descriptors.
     */
    @SuppressWarnings("unused")
    public static void setAggregatorDescriptors_WRONG(Configuration conf,
            Class<? extends ValueAggregatorDescriptor>[] descriptors) {
        conf.setInt(ValueAggregatorJobBase.DESCRIPTOR_NUM, descriptors.length);

        for(int i = 0; i < descriptors.length; ++i)
            conf.set(ValueAggregatorJobBase.DESCRIPTOR + i, "UserDefined," + descriptors[i].getName());
    }

    /**
     * Fixed version of method org.apache.hadoop.mapreduce.lib.aggregate
     *     .ValueAggregatorJob#setAggregatorDescriptors(java.lang.Class[]): it adds correct "." to the property name.
     *
     * @param conf The configuration.
     * @param descriptors The descriptors.
     */
    public static void setAggregatorDescriptors_CORRECT(Configuration conf,
        Class<? extends ValueAggregatorDescriptor>[] descriptors) {
        conf.setInt(ValueAggregatorJobBase.DESCRIPTOR_NUM, descriptors.length);

        //specify the aggregator descriptors
        for(int i=0; i< descriptors.length; i++)
            conf.set(ValueAggregatorJobBase.DESCRIPTOR + "." + i, "UserDefined," + descriptors[i].getName());
    }

    /**
     * Gets the file system we work upon.
     * @return The file system.
     * @throws Exception
     */
    protected FileSystem getFileSystem() throws Exception{
        return FileSystem.get(new URI(getFsBase()), new Configuration());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        FileSystem fs = getFileSystem();

        fs.delete(new Path(getFsBase()), true);
        fs.mkdirs(new Path(getFsBase()));

        super.beforeTest();

        startNodes();
    }

    /**
     * Runs the example test.l
     *
     * @throws Exception If failed.
     */
    public void testExample() throws Exception {
        runExampleTest();
    }

    /**
     * Creates Hadoop configuration for the test.
     * @return The {@link HadoopConfiguration}.
     */
    @Override protected HadoopConfiguration hadoopConfiguration(int idx, String name) {
        HadoopConfiguration hadoopCfg = super.hadoopConfiguration(idx, name);

        // See org.apache.ignite.configuration.HadoopConfiguration.DFLT_MAX_TASK_QUEUE_SIZE
        hadoopCfg.setMaxTaskQueueSize(30_000);

        return hadoopCfg;
    }

    /**
     * A generic walker with callbacks to analyse the output files.
     */
    public static class OutputFileChecker {
        /** */
        private final FileSystem fs;

        /** */
        private final String file;

        /**
         * Constructor.
         *
         * @param fs The file system.
         * @param file The file.
         */
        public OutputFileChecker(FileSystem fs, String file) {
            this.fs = fs;

            this.file = file;
        }

        /**
         * Template method to check the file.
         *
         * @throws Exception On error.
         */
        public final void check() throws Exception {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(file))))) {
                int cnt = 0;

                String line = null;

                while (true) {
                    String line0 = br.readLine();

                    if (line0 == null)
                        break;

                    line = line0;

                    cnt++;

                    if (cnt == 1)
                        onFirstLine(line); // The first line

                    onLine(line, cnt);
                }

                onLastLine(line); // The last line

                onFileEnd(cnt); // EOF.
            }
        }

        /**
         * Line callback. invoked on each line, including the 1st and the last one.
         *
         * @param line The line.
         * @param cnt The 0-based count.
         */
        void onLine(String line, int cnt) {}

        /**
         * First line callback. Invoked on the 1st line.
         *
         * @param line The line.
         */
        void onFirstLine(String line) {}

        /**
         * The last line callback.
         *
         * @param line The line.
         */
        void onLastLine(String line) {}

        /**
         * File finish callback,
         *
         * @param lineCnt The total file line count.
         */
        void onFileEnd(int lineCnt) {}
    }
}

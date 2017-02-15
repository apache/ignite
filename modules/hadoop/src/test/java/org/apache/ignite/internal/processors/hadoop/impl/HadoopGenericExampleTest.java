package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
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
 * Runs Hadoop Quasi Monte Carlo Pi estimation example.
 */
public abstract class HadoopGenericExampleTest extends HadoopAbstract2Test {
    /**
     * Class representing the sample execution parameters.
     */
    static class FrameworkParameters {
        /** */
        int maps;

        /** */
        int reduces = 1;

        /** */
        String workDir;

        /** */
        String user;

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

        /** Gets the example name. */
        final String name() {
            return tool().getClass().getSimpleName();
        }

        /**
         *
         * @param fp
         * @return
         */
        protected final String inDir(FrameworkParameters fp) {
            return fp.getWorkDir(name()) + "/in";
        }

        /**
         *
         * @param fp
         * @return
         */
        protected final String outDir(FrameworkParameters fp) {
            return fp.getWorkDir(name()) + "/out";
        }

        /**
         *  Utility method to generate predictable random text input.
         *
         * @param numFiles
         * @param conf
         * @param params
         * @throws IOException
         */
        protected final void generateTextInput(int numFiles, JobConf conf,
            FrameworkParameters params) throws IOException {
            // We cannot directly use Hadoop's RandomTextWriter since it is really random, but here
            // we need definitely reproducible input data.
            try (FileSystem fs = FileSystem.get(conf)) {
                final int files = 11;

                for (int i=0; i<files; i++) {
                    try (OutputStream os = fs.create(new Path(inDir(params) + "/in-" + i), true)) {
                        generateSentence(random, 2000, os);
                    }
                }
            }
        }

        /** Performs pre-execution preparation. */
        void prepare(JobConf conf, FrameworkParameters fp) throws Exception {
            // noop
        }

        /** Gets the String parameters to be passed to the Tool upon execution. */
        abstract String[] parameters(FrameworkParameters fp);

        /** Gets the tool to execute. */
        abstract Tool tool();

        /** Checks example calculation validity. */
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
        return "file:///tmp/hadoop-test-" + getUser();
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

        // TODO: Delete files used:
        //getFileSystem().delete(new Path(getFsBase()), true);
    }

    /**
     * @return The execution parameters.
     */
    protected FrameworkParameters frameworkParameters() {
        return new FrameworkParameters(numMaps(), gridCount(), getFsBase(), getUser());
    }

    /**
     * Does actual calculation through Ignite API
     *
     * @param gzip Whether to use GZIP.
     */
    protected final void testImpl(boolean gzip) throws Exception {
        final GenericHadoopExample ex = example();

        System.out.println(ex.name() + ": ===============================================================");

        final JobConf conf = new JobConf();

        conf.set("fs.defaultFS", getFsBase());

        log().info("Desired number of maps: " + numMaps());

//        // Ignite specific job properties:
//        conf.setBoolean(HadoopJobProperty.SHUFFLE_MAPPER_STRIPED_OUTPUT.propertyName(), true);
//        conf.setInt(HadoopJobProperty.SHUFFLE_MSG_SIZE.propertyName(), 4096);
//
//        if (gzip)
//            conf.setBoolean(HadoopJobProperty.SHUFFLE_MSG_GZIP.propertyName(), true);

        // Set the Ignite framework and the address:
        conf.set(MRConfig.FRAMEWORK_NAME,  "ignite");
        conf.set(MRConfig.MASTER_ADDRESS, "localhost:11211");

//        conf.set(MRConfig.FRAMEWORK_NAME,  "local");

        // Start real calculation:
        FrameworkParameters fp = frameworkParameters();

        ex.prepare(conf, fp);

        String[] args = ex.parameters(fp);

        X.println("#### Running job with parameters: " + Arrays.toString(args));

        int res = ToolRunner.run(conf, ex.tool(), args);

        assertEquals(0, res);

        ex.verify(args);
    }

    /**
     * Fixed version of method org.apache.hadoop.mapreduce.lib.aggregate
     *     .ValueAggregatorJob#setAggregatorDescriptors(java.lang.Class[]): it adds correct "." to the property name.
     *
     * @param conf
     * @param descriptors
     */
    static void setAggregatorDescriptors(Configuration conf,
        Class<? extends ValueAggregatorDescriptor>[] descriptors) {
        conf.setInt(ValueAggregatorJobBase.DESCRIPTOR_NUM, descriptors.length);
        //specify the aggregator descriptors
        for(int i=0; i< descriptors.length; i++) {
            conf.set(ValueAggregatorJobBase.DESCRIPTOR + "." + i,
                "UserDefined," + descriptors[i].getName());
        }
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
        testImpl(false);
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
}

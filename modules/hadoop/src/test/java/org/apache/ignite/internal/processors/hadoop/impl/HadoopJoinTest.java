package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.hadoop.examples.Join;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Tool;

/**
 * Runs Join test.
 */
public class HadoopJoinTest extends HadoopGenericExampleTest {
    /**
     * Gets base directory.
     * Note that this directory will be completely deleted in the and of the test.
     * @return The base directory.
     */
    protected String getFsBase() {
        return "file:///tmp/" + getUser() + "/hadoop-join-test";
    }

    /**
     * Desired number of maps in TeraSort job.
     * @return The number of maps.
     */
    protected int numMaps() {
        return gridCount() /* * 16 */;
    }

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final Join tool = new Join();

        @Override String[] parameters(FrameworkParameters fp) {
            String in = fp.getWorkDir(name()) + "/in" ;

            System.out.println("############### In: " + in);

            // "Usage: "+getClass().getName()+" <nMaps> <nSamples>");
            return new String[] { "-r", String.valueOf(fp.reduces()),
                "-inFormat", SequenceFileInputFormat.class.getName(),
                "-outFormat", TextOutputFormat.class.getName(),
                "-outKey",  org.apache.hadoop.io.LongWritable.class.getName(),
                "-outValue", org.apache.hadoop.mapreduce.lib.join.TupleWritable.class.getName(),
                fp.getWorkDir(name()) + "/in",
                fp.getWorkDir(name()) + "/out"
            };
        }

        @Override Tool tool() {
            return tool;
        }

        @Override void prepare(JobConf conf, FrameworkParameters fp) throws Exception {
            conf.set(FileOutputFormat.OUTDIR, new Path(fp.getWorkDir(name()) + "/in").toString());

            //FileOutputFormat.setOutputPath(conf, new Path(fp.getWorkDir(name()) + "/in"));
            //FileOutputFormat.setWorkOutputPath(conf, new Path(fp.getWorkDir(name()) + "/in"));

            try (FileSystem fs = FileSystem.get(conf)) {
                SequenceFileOutputFormat of = new SequenceFileOutputFormat();

                //final String val0 = conf.get(MRJobConfig.TASK_ATTEMPT_ID);

                TaskAttemptContext tac = new TaskAttemptContextImpl(conf, TaskAttemptID.forName("attempt_200707121733_0003_m_000005_0"));

                RecordWriter rw = of.getRecordWriter(tac);
                try {
                    LongWritable lw = new LongWritable();
                    Text t = new Text();

                    for (int i=0; i<100; i++) {
                        lw.set(i);
                        t.set("" + i);

                        rw.write(lw, t);
                    }
                }
                finally {
                    rw.close(tac);
                }

                boolean ok = fs.rename(
                    new Path(fp.getWorkDir(name() + "/in" + "/_temporary/0/_temporary/attempt_200707121733_0003_m_000005_0/part-m-00005")),
                    new Path(fp.getWorkDir(name() + "/in/in00")) );

                assert ok;

                Path p = new Path(fp.getWorkDir(name()) + "/in");

                RemoteIterator<LocatedFileStatus> ri = fs.listFiles(p, true);

                LocatedFileStatus lfs;

                while (ri.hasNext()) {
                    lfs = ri.next();

                    System.out.println("#### " + lfs);

                    assert lfs.isFile();
                    assert lfs.getLen() > 0;
                }

//                for (FileStatus s: fs.listStatus(p)) {
//                    System.out.println("#### " + s);
//                }

                // TODO: Local MR execution works okay, but manual setting of this
                // TODO: id for some reason is required for Ignite.
                // TODO: investigate, why this happens.
                conf.set(MRJobConfig.TASK_ATTEMPT_ID, "attempt_200707121733_0003_m_000005_0");
            }

            conf.unset(FileOutputFormat.OUTDIR);
        }

        @Override void verify(String[] parameters) throws Exception {
            String outDir = parameters[11];

            new OutputFileChecker(getFileSystem(), outDir + "/part-r-" + nullifyToLen(5, 0)) {
                @Override void checkFirstLine(String line) {
                    assertEquals("0\t[0]", line);
                }

                @Override void checkLastLine(String line) {
                    assertEquals("99\t[99]", line);
                }

                @Override void checkLineCount(int cnt) {
                    assertEquals(34, cnt);
                }
            }.check();

            new OutputFileChecker(getFileSystem(), outDir + "/part-r-" + nullifyToLen(5, 2)) {
                @Override void checkFirstLine(String line) {
                    assertEquals("2\t[2]", line);
                }

                @Override void checkLastLine(String line) {
                    assertEquals("98\t[98]", line);
                }

                @Override void checkLineCount(int cnt) {
                    assertEquals(33, cnt);
                }
            }.check();
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}

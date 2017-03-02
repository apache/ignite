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
     * Desired number of maps in TeraSort job.
     * @return The number of maps.
     */
    protected int numMaps() {
        return gridCount() * 3;
    }

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final Join tool = new Join();

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] {
                "-r", String.valueOf(fp.reduces()),
                "-inFormat", SequenceFileInputFormat.class.getName(),
                "-outFormat", TextOutputFormat.class.getName(),
                "-outKey",  org.apache.hadoop.io.LongWritable.class.getName(),
                "-outValue", org.apache.hadoop.mapreduce.lib.join.TupleWritable.class.getName(),
                fp.getWorkDir(name()) + "/in",
                fp.getWorkDir(name()) + "/out"
            };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return tool;
        }

        /** {@inheritDoc} */
        @Override void prepare(JobConf conf, FrameworkParameters fp) throws Exception {
            conf.set(FileOutputFormat.OUTDIR, new Path(fp.getWorkDir(name()) + "/in").toString());

            // Create a sequence file:
            try (FileSystem fs = FileSystem.get(conf)) {
                SequenceFileOutputFormat<LongWritable, Text> of = new SequenceFileOutputFormat<>();

                final String mapId = "00008";
                final String id = "attempt_111111111111_2222_m_0" + mapId + "_0";

                TaskAttemptContext tac = new TaskAttemptContextImpl(conf, TaskAttemptID.forName(id));

                RecordWriter<LongWritable, Text> rw = of.getRecordWriter(tac);
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
                    new Path(fp.getWorkDir(name() + "/in" + "/_temporary/0/_temporary/" + id + "/part-m-" + mapId)),
                    new Path(fp.getWorkDir(name() + "/in/in00")) );

                assert ok;

                Path p = new Path(fp.getWorkDir(name()) + "/in");

                RemoteIterator<LocatedFileStatus> ri = fs.listFiles(p, true);

                LocatedFileStatus lfs;

                while (ri.hasNext()) {
                    lfs = ri.next();

                    assert lfs.isFile();
                    assert lfs.getLen() > 0;
                }

//                TODO: Local MR execution works okay, but manual setting of this
//                TODO: id for some reason is required for Ignite.
//                TODO: investigate, why this happens.
                conf.set(MRJobConfig.TASK_ATTEMPT_ID, "attempt_000000000000_0000_m_000000_0");
            }

            conf.unset(FileOutputFormat.OUTDIR);
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) throws Exception {
            String outDir = parameters[11];

            new OutputFileChecker(getFileSystem(), outDir + "/part-r-" + nullifyToLen(5, 0)) {
                /** {@inheritDoc} */
                @Override void onFirstLine(String line) {
                    assertEquals("0\t[0]", line);
                }

                /** {@inheritDoc} */
                @Override void onLastLine(String line) {
                    assertEquals("99\t[99]", line);
                }

                /** {@inheritDoc} */
                @Override void onFileEnd(int cnt) {
                    assertEquals(34, cnt);
                }
            }.check();

            new OutputFileChecker(getFileSystem(), outDir + "/part-r-" + nullifyToLen(5, 2)) {
                /** {@inheritDoc} */
                @Override void onFirstLine(String line) {
                    assertEquals("2\t[2]", line);
                }

                /** {@inheritDoc} */
                @Override void onLastLine(String line) {
                    assertEquals("98\t[98]", line);
                }

                /** {@inheritDoc} */
                @Override void onFileEnd(int cnt) {
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

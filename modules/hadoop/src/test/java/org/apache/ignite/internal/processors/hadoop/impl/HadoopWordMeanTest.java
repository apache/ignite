package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.RandomTextWriter;
import org.apache.hadoop.examples.WordMean;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.ignite.IgniteException;

/**
 *
 */
public class HadoopWordMeanTest extends HadoopGenericTest {

    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final WordMean impl = new WordMean();

        private final Random random = new Random(0L);

        @Override String name() {
            return "WordMean";
        }

        private String[] getWords() {
            try {
                Field wordsField = RandomTextWriter.class.getDeclaredField("words");

                wordsField.setAccessible(true);

                return (String[])wordsField.get(null);
            }
            catch (Throwable t) {
                throw new IgniteException(t);
            }
        }

        private void generateSentence(int noWords, OutputStream os) throws IOException {
            String[] words = getWords();

            try (Writer w = new OutputStreamWriter(os)) {
                String space = " ";

                for (int i = 0; i < noWords; ++i) {
                    w.write(words[random.nextInt(words.length)]);

                    w.write(space);
                }
            }
        }

        private String inDir(FrameworkParameters fp) {
            return fp.getWorkDir(name()) + "/in";
        }

        @Override void prepare(Configuration conf, FrameworkParameters params) throws IOException {
            // We cannot directly use Hadoop's RandomTextWriter since it is really random, but here
            // we need definitely reproducible input data.
            try (FileSystem fs = FileSystem.get(conf)) {
                try (OutputStream os = fs.create(new Path(inDir(params) + "/in-00"), true)) {
                    generateSentence(2000, os);
                }
            }
        }

        @Override String[] parameters(FrameworkParameters fp) {
            // wordmean <in> <out>
            return new String[] {
                inDir(fp),
                fp.getWorkDir(name()) + "/out" };
        }

        @Override Tool tool() {
            return impl;
        }

        @Override void verify(String[] parameters) {
            assertEquals(9.588, impl.getMean(), 1e-3);
        }
    };

    @Override protected GenericHadoopExample example() {
        return ex;
    }
}

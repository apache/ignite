package org.apache.ignite.internal.processors.hadoop.examples;

import java.io.IOException;
import org.apache.ignite.internal.processors.hadoop.ErrorSimulator;

/**
 * Created by ivan on 24.02.16.
 */
public class HadoopWordCount2Combiner extends HadoopWordCount2Reducer {

    @Override protected void configError() {
        ErrorSimulator.instance().onCombineConfigure();
    }

    @Override protected void setupError() throws IOException, InterruptedException {
        ErrorSimulator.instance().onCombineSetup();
    }

    @Override protected void reduceError() throws IOException, InterruptedException {
        ErrorSimulator.instance().onCombine();
    }

    @Override protected void cleanupError() throws IOException, InterruptedException {
        ErrorSimulator.instance().onCombineCleanup();
    }
}

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.IgniteException;

/**
 * Same test as HadoopMapReduceTest, but with enabled Snappy output compression.
 */
public class SnappyHadoopMapReduceTest extends HadoopMapReduceTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        try {
            SnappyUtil.addHadoopNativeLibToJavaLibraryPath();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected boolean compressOutputSnappy() {
        return true;
    }
}

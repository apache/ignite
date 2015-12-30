package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.fs.FileSystem;

/**
 * Class contains casting to a Hadoop type.
 */
public abstract class HadoopCasting <T> {
    /** */
    public abstract T create();

    /** */
    public void consume(T t) {
        // noop
    }

    /** */
    void test(HadoopCasting<FileSystem> c) {
        FileSystem fs = c.create();

        c.consume(fs);
    }
}

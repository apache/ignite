package org.apache.ignite.internal.processors.hadoop.cls;

import java.util.List;
import org.apache.hadoop.fs.FileSystem;

/**
 * Uses Hadoop type in a static initializer.
 */
public class HadoopStaticInitializer {
    /** */
    static final List x;

    static {
        x = FileSystem.getAllStatistics();
    }
}

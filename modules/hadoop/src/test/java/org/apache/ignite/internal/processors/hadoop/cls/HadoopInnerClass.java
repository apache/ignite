package org.apache.ignite.internal.processors.hadoop.cls;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;

/**
 * Has a *static* inner class depending on Hadoop.
 */
public class HadoopInnerClass {
    /** */
    private static abstract class Foo implements Configurable {
        // nothing
    }
}

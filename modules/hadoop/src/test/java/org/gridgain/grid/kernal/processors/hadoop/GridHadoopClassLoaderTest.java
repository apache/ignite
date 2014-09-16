/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import junit.framework.TestCase;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 */
public class GridHadoopClassLoaderTest extends TestCase {
    /** */
    GridHadoopClassLoader ldr = new GridHadoopClassLoader(null);

    /**
     * @throws Exception If failed.
     */
    public void testClassLoading() throws Exception {
        assertNotSame(Test1.class, ldr.loadClass(Test1.class.getName()));
        assertNotSame(Test2.class, ldr.loadClass(Test2.class.getName()));
        assertSame(Test3.class, ldr.loadClass(Test3.class.getName()));
    }

//    public void testDependencySearch() {
//        assertTrue(ldr.hasExternalDependencies(Test1.class.getName(), new HashSet<String>()));
//        assertTrue(ldr.hasExternalDependencies(Test2.class.getName(), new HashSet<String>()));
//    }

    /**
     *
     */
    private static class Test1 {
        /** */
        Test2 t2;

        /** */
        Job[][] jobs = new Job[4][4];
    }

    /**
     *
     */
    private static abstract class Test2 {
        /** */
        abstract Test1 t1();
    }

    /**
     *
     */
    private static class Test3 {
        // No-op.
    }
}
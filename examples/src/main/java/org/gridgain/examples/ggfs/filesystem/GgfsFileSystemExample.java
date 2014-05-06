/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.ggfs.filesystem;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * This example shows usage of {@code GridGgfsHadoopFileSystem Hadoop FS driver}.
 * <p>
 * Before running this example you must start at least one remote node using
 * {@link GgfsFileSystemNodeStartup}.
 */
@GridOnlyAvailableIn(HADOOP)
public class GgfsFileSystemExample {
    /** Path to the default hadoop configuration. */
    private static final String HADOOP_FS_CFG = "examples/config/hadoop/core-site.xml";

    /** Flag to mark HDFS installation is configured, started and available for this example. */
    private static final boolean USE_HDFS = false;

    /** Default path to the folder to copy in case it is not specified explicitly in arguments. */
    private static final String DFLT_PATH = "examples/src/main/java/org/gridgain/examples";

    /**
     * Executes example.
     *
     * @param args Command line arguments. Expected 1 argument - path to folder to copy relative to GRIDGAIN_HOME).
     *             In case omitted, "examples/java/org/gridgain/examples" will be used.
     * @throws IOException If failed.
     */
    @SuppressWarnings("TooBroadScope")
    public static void main(String[] args) throws IOException {
        try {
            System.out.println();
            System.out.println(">>> GGFS file system example started.");

            String path = args.length > 0 ? args[0] : DFLT_PATH;

            /** Local FS home path. */
            Path locHome = new Path("file:///" + U.getGridGainHome() + "/");

            /** GGFS home path. */
            Path ggfsHome = new Path("ggfs://ipc");

            /** HDFS path to name node. */
            Path hdfsHome = new Path(System.getProperty("HDFS_HOME", "hdfs://localhost:9000/"));

            Configuration cfg = new Configuration(true);

            cfg.addResource(U.resolveGridGainUrl(HADOOP_FS_CFG));
            cfg.setInt("io.file.buffer.size", 65536);

            FileSystem loc = FileSystem.get(locHome.toUri(), cfg);
            FileSystem ggfs = FileSystem.get(ggfsHome.toUri(), cfg);
            FileSystem hdfs = USE_HDFS ? FileSystem.get(hdfsHome.toUri(), cfg) : null;

            System.out.println(">>> FILE: " + loc);
            System.out.println(">>> GGFS: " + ggfs);
            System.out.println(">>> HDFS: " + hdfs);

            Path locSrc = new Path(locHome, path);

            Path locTmp = new Path(locHome, "work/tmp");
            Path ggfsTmp1 = new Path(ggfsHome, "/tmp1");
            Path ggfsTmp2 = new Path(ggfsHome, "/tmp2");
            Path hdfsTmp1 = new Path(hdfsHome, "/tmp1");
            Path hdfsTmp2 = new Path(hdfsHome, "/tmp2");

            copy("LOC => GGFS", loc, locSrc, ggfs, ggfsTmp1);
            copy("LOC => HDFS", loc, locSrc, hdfs, hdfsTmp1);

            copy("GGFS => LOC", ggfs, ggfsTmp1, loc, locTmp);
            copy("HDFS => LOC", hdfs, hdfsTmp1, loc, locTmp);

            copy("GGFS => GGFS", ggfs, ggfsTmp1, ggfs, ggfsTmp2);
            copy("HDFS => HDFS", hdfs, hdfsTmp1, hdfs, hdfsTmp2);

            copy("GGFS => HDFS", ggfs, ggfsTmp1, hdfs, hdfsTmp2);
            copy("HDFS => GGFS", hdfs, hdfsTmp1, ggfs, ggfsTmp2);
        }
        finally {
            FileSystem.closeAll();
        }
    }

    /**
     * Copy files from one FS to another.
     *
     * @param msg Info message to display after copiing finishes.
     * @param srcFs Source file system.
     * @param src Source path to copy from.
     * @param destFs Destination file system.
     * @param dest Destination path to copy to.
     * @throws IOException If failed.
     */
    public static void copy(String msg, @Nullable FileSystem srcFs, Path src, @Nullable FileSystem destFs, Path dest)
        throws IOException {
        // Ignore operation if source or destination file system is not defined.
        if (srcFs == null || destFs == null)
            return;

        if (!destFs.delete(dest, true) && destFs.exists(dest))
            throw new IOException("Failed to remove destination file: " + dest);

        destFs.mkdirs(dest);

        Configuration conf = new Configuration(true);

        long time = System.currentTimeMillis();

        FileUtil.copy(srcFs, src, destFs, dest, false, true, conf);

        time = System.currentTimeMillis() - time;

        System.out.println(">>> " + msg + " [time=" + time + "ms, src=" + src + ", dest=" + dest + ']');
    }
}

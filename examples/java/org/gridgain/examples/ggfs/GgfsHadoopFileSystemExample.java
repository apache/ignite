// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.ggfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * This example shows usage of {@link GridGgfsHadoopFileSystem Hadoop FS driver}.
 * To start remote node, you can run {@link GgfsEndpointNodeStartup} class.
 * <p>
 * Note that this example is configured to work with only one node per physical box.
 * <p>
 * You can also start a stand-alone GridGain instance by passing the path
 * to configuration file to {@code 'ggstart.{sh|bat}'} script, like so:
 * {@code 'ggstart.sh examples/config/example-ggfs.xml'}. Before doing this you should
 * uncomment {@code ipcEndpointConfiguration} in this XML file. Note that shared memory
 * IPC is not supported on Windows, so you should use loopback endpoint configuration
 * for this operating system.
 * <p>
 * To use GGFS with your Hadoop instance
 * <ul>
 *     <li>Copy configuration '/config/hadoop/core-site.xml' into your $HADOOP_HOME/conf folder.</li>
 *     <li>Provide GridGain classpath in $HADOOP_HOME/conf/hadoop-env.sh with the following code snippet:</li>
 * </ul>
 * <pre>
 *     export GRIDGAIN_HOME=/path/to/gridgain/installation
 *     export HADOOP_CLASSPATH=$GRIDGAIN_HOME/gridgain-x.x.x.jar
 *
 *     for f in $GRIDGAIN_HOME/libs/*.jar; do
 *         export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f;
 *     done
 * </pre>
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(HADOOP)
public class GgfsHadoopFileSystemExample {
    /** Path to the default hadoop configuration. */
    private static final String HADOOP_FS_CFG = "/config/hadoop/core-site.xml";

    /** Flag to mark HDFS installation is configured, started and available for this example. */
    private static final boolean USE_HDFS = false;

    /** Default path to the folder to copy in case it is not specified explicitly in arguments. */
    private static final String DFLT_PATH = "os/examples/java/org/gridgain/examples";

    /**
     * <tt>Hadoop FS driver</tt> example shows configuration and simple operations
     * for different Hadoop file systems: local files, GGFS and HDFS.
     *
     * @param args Command line arguments. Expected 1 argument - path to folder to copy relative to GRIDGAIN_HOME).
     *             In case omitted, "examples/java/org/gridgain/examples" will be used.
     * @throws IOException If failed.
     */
    @SuppressWarnings("TooBroadScope")
    public static void main(String[] args) throws IOException {
        try {
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

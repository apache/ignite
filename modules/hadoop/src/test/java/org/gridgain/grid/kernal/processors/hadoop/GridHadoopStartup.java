/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Hadoop node startup.
 */
public class GridHadoopStartup {
    /**
     * @param args Arguments.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        G.start("config/hadoop/default-config.xml");
    }

    /**
     * @return Configuration for job run.
     */
    @SuppressWarnings("UnnecessaryFullyQualifiedName")
    public static Configuration configuration() {
        Configuration cfg = new Configuration();

        cfg.set("fs.defaultFS", "ggfs://ggfs@localhost");

        cfg.set("fs.ggfs.impl", org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.ggfs.impl", org.gridgain.grid.ggfs.hadoop.v2.GridGgfsHadoopFileSystem.class.getName());

        cfg.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

        cfg.set("mapreduce.framework.name", "gridgain");
        cfg.set("mapreduce.jobtracker.address", "localhost:11211");

        return cfg;
    }
}

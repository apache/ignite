package org.apache.ignite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Created by tledkov on 30.09.16.
 */
public class HadoopTest {
    /**
     * @param args q
     * @throws Exception if failed
     */
    public static void main(String [] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> ri = fs.listFiles(fs.getHomeDirectory(), false);
        while (ri.hasNext()) {
            LocatedFileStatus lfs = ri.next();
            System.out.println(lfs.getPath().toString());
        }
        fs.close();
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Utility to run regular Ignite tests with {@link org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi}.
 */
public class ZookeeperDiscoverySpiTestUtil {
    /**
     * @param instances Number of instances in cluster.
     * @return Test cluster.
     */
    public static TestingCluster createTestingCluster(int instances) {
        return createTestingCluster(instances, null);
    }

    /**
     * @param instances Number of instances in cluster.
     * @param customProps Custom configuration properties for every server.
     * @return Test cluster.
     */
    public static TestingCluster createTestingCluster(int instances, @Nullable Map<String,Object>[] customProps) {
        String tmpDir;

        tmpDir = System.getenv("TMPFS_ROOT") != null
            ? System.getenv("TMPFS_ROOT") : System.getProperty("java.io.tmpdir");

        List<InstanceSpec> specs = new ArrayList<>();

        for (int i = 0; i < instances; i++) {
            File file = new File(tmpDir, "apacheIgniteTestZk-" + i);

            if (file.isDirectory())
                deleteRecursively0(file);
            else {
                if (!file.mkdirs())
                    throw new IgniteException("Failed to create directory for test Zookeeper server: " + file.getAbsolutePath());
            }

            Map<String,Object> props = customProps != null ? customProps[i] : null;

            specs.add(new InstanceSpec(file, -1, -1, -1, true, -1, -1, 500, props));
        }

        return new TestingCluster(specs);
    }

    /**
     * @param file File or directory to delete.
     */
    private static void deleteRecursively0(File file) {
        File[] files = file.listFiles();

        if (files == null)
            return;

        for (File f : files) {
            if (f.isDirectory())
                deleteRecursively0(f);
            else {
                if (!f.delete())
                    throw new IgniteException("Failed to delete file: " + f.getAbsolutePath());
            }
        }
    }

}

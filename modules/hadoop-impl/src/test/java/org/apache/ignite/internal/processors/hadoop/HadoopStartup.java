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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem;
import org.apache.ignite.internal.util.typedef.G;

/**
 * Hadoop node startup.
 */
public class HadoopStartup {
    /**
     * @param args Arguments.
     */
    public static void main(String[] args) {
        G.start("config/hadoop/default-config.xml");
    }

    /**
     * @return Configuration for job run.
     */
    @SuppressWarnings("UnnecessaryFullyQualifiedName")
    public static Configuration configuration() {
        Configuration cfg = new Configuration();

        cfg.set("fs.defaultFS", "igfs://igfs@localhost");

        cfg.set("fs.igfs.impl", org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.igfs.impl", IgniteHadoopFileSystem.class.getName());

        cfg.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

        cfg.set("mapreduce.framework.name", "ignite");
        cfg.set("mapreduce.jobtracker.address", "localhost:11211");

        return cfg;
    }
}
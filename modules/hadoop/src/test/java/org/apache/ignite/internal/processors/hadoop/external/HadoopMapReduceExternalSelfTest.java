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

package org.apache.ignite.internal.processors.hadoop.external;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReduceEmbeddedSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReduceTest;
import org.apache.ignite.internal.processors.hadoop.HadoopSharedMap;
import org.apache.ignite.internal.processors.hadoop.examples.HadoopWordCount1;
import org.apache.ignite.internal.processors.hadoop.examples.HadoopWordCount2;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.createJobInfo;

/**
 * Tests map-reduce execution with embedded mode.
 */
public class HadoopMapReduceExternalSelfTest extends HadoopMapReduceEmbeddedSelfTest {
    /** {@inheritDoc} */
    @Override public HadoopConfiguration hadoopConfiguration(String gridName) {
        HadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setExternalExecution(true);

        return cfg;
    }
}
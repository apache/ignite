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

package org.apache.ignite.internal.processors.hadoop.taskexecutor;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopAbstractSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopExternalTaskExecutionSelfTest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.createJobInfo;

/**
 * Job tracker self test.
 */
public class HadoopEmbeddedTaskExecutionSelfTest extends HadoopExternalTaskExecutionSelfTest {
    /** {@inheritDoc} */
    @Override public HadoopConfiguration hadoopConfiguration(String gridName) {
        HadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setExternalExecution(false);

        return cfg;
    }
}
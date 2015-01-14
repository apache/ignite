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

package org.apache.ignite.spi.checkpoint.s3;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

/**
 * Management bean for {@link GridS3CheckpointSpi}.
 */
@IgniteMBeanDescription("MBean that provides access to S3 checkpoint SPI configuration.")
public interface GridS3CheckpointSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets S3 bucket name to use.
     *
     * @return S3 bucket name to use.
     */
    @IgniteMBeanDescription("S3 bucket name.")
    public String getBucketName();

    /**
     * @return S3 access key.
     */
    @IgniteMBeanDescription("S3 access key.")
    public String getAccessKey();

    /**
     * @return HTTP proxy host.
     */
    @IgniteMBeanDescription("HTTP proxy host.")
    public String getProxyHost();

    /**
     * @return HTTP proxy port
     */
    @IgniteMBeanDescription("HTTP proxy port.")
    public int getProxyPort();

    /**
     * @return HTTP proxy user name.
     */
    @IgniteMBeanDescription("HTTP proxy user name.")
    public String getProxyUsername();
}

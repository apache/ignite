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

package org.apache.ignite.spi.swapspace.file;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management bean for {@link FileSwapSpaceSpi}.
 */
@MXBeanDescription("MBean that provides configuration information on file-based swapspace SPI.")
public interface FileSwapSpaceSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets base directory.
     *
     * @return Base directory.
     */
    @MXBeanDescription("Base directory.")
    public String getBaseDirectory();

    /**
     * Gets maximum sparsity.
     *
     * @return Maximum sparsity.
     */
    @MXBeanDescription("Maximum sparsity.")
    public float getMaximumSparsity();

    /**
     * Gets write buffer size in bytes.
     *
     * @return Write buffer size in bytes.
     */
    @MXBeanDescription("Write buffer size in bytes.")
    public int getWriteBufferSize();

    /**
     * Gets max write queue size in bytes.
     *
     * @return Max write queue size in bytes.
     */
    @MXBeanDescription("Max write queue size in bytes.")
    public int getMaxWriteQueueSize();

    /**
     * Gets read pool size.
     *
     * @return Read pool size.
     */
    @MXBeanDescription("Read pool size.")
    public int getReadStripesNumber();
}
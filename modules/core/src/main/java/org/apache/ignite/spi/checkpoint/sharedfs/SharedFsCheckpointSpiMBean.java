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

package org.apache.ignite.spi.checkpoint.sharedfs;

import java.util.Collection;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management bean that provides general administrative and configuration information
 * about shared file system checkpoints.
 */
@MXBeanDescription("MBean for shared file system based checkpoint SPI.")
public interface SharedFsCheckpointSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets path to the directory where all checkpoints are saved.
     *
     * @return Path to the checkpoints directory.
     */
    @MXBeanDescription("Gets path to the directory where all checkpoints are saved.")
    public String getCurrentDirectoryPath();


    /**
     * Gets collection of all configured paths where checkpoints can be saved.
     *
     * @return Collection of all configured paths.
     */
    @MXBeanDescription("Gets collection of all configured paths where checkpoints can be saved.")
    public Collection<String> getDirectoryPaths();
}
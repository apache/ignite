/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.deployment.uri;

import java.util.List;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management bean for {@link UriDeploymentSpi}.
 */
@MXBeanDescription("MBean that provides access to URI deployment SPI configuration.")
public interface UriDeploymentSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets temporary directory path.
     *
     * @return Temporary directory path.
     */
    @MXBeanDescription("Temporary directory path.")
    public String getTemporaryDirectoryPath();

    /**
     * Gets list of URIs that are processed by SPI.
     *
     * @return List of URIs.
     */
    @MXBeanDescription("List of URIs.")
    public List<String> getUriList();

    /**
     * Indicates if this SPI should check new deployment units md5 for redundancy.
     *
     * @return if files are ckecked for redundancy.
     */
    @MXBeanDescription("Indicates if MD5 check is enabled.")
    public boolean isCheckMd5();
}
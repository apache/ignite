/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.mxbean;

import java.util.UUID;
import javax.management.JMException;

/**
 * MX Bean allows to access information about cluster ID and tag and change tag.
 */
@MXBeanDescription("MBean that provides access to information about cluster ID and tag.")
public interface IgniteClusterMXBean {
    /**
     * Gets cluster ID.
     *
     * @return Cluster ID.
     */
    @MXBeanDescription("Unique identifier of the cluster.")
    public UUID getId();

    /**
     * Gets current cluster tag.
     *
     * @return Current cluster tag.
     */
    @MXBeanDescription("User-defined cluster tag.")
    public String getTag();

    /**
     * Changes cluster tag to provided value.
     *
     * @param newTag New value to be set as cluster tag.
     * @throws JMException If provided value failed validation or concurrent change tag operation succeeded.
     */
    @MXBeanDescription("Set new cluster tag value.")
    @MXBeanParametersNames("newTag")
    @MXBeanParametersDescriptions(
        "New tag value to be set."
    )
    public void tag(String newTag) throws JMException;
}

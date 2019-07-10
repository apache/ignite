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

package org.apache.ignite.cluster;

/**
 * Cluster start node result information.
 */
public interface ClusterStartNodeResult {
    /**
     * Gets host name.
     *
     * @return Host name.
     */
    public String getHostName();
    
    /**
     * Gets result of success or failure.
     *
     * @return Success or failure. <code>True</code> if success.
     */
    public boolean isSuccess();
    
    /**
     * Gets error message if any.
     *
     * @return Error massage or {@code null} if start node result is success.
     */
    public String getError();
}
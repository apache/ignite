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

package org.apache.ignite.cluster;

import org.jetbrains.annotations.Nullable;

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
     * @return Error massage.
     */
    @Nullable public String getError();
}
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

package org.apache.ignite.mxbean;

import org.apache.ignite.configuration.DataStorageConfiguration;

/**
 * An MX bean allowing to monitor and tune persistence.
 */
public interface DataStorageMXBean {
    @MXBeanDescription("ZIP compression level to WAL compaction.")
    int getWalCompactionLevel();

    /**
     * Sets ZIP compression level to WAL compaction.
     * {@link DataStorageConfiguration#setWalCompactionLevel(int)} configuration property.
     *
     * @param walCompactionLevel ZIP compression level.
     */
    @MXBeanDescription("Sets ZIP compression level to WAL compaction.")
    void setWalCompactionLevel(
        @MXBeanParameter(name = "walCompactionLevel", description = "ZIP compression level.") int walCompactionLevel
    );
}

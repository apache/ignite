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
package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.IgniteCheckedException;

/**
 * Listener for events of metastore lifecycle.
 *
 * Database manager is responsible for initializing metastore on node startup
 * and notifying other components about its readiness.
 */
public interface MetastorageLifecycleListener {
    /**
     * Is called when metastorage is made ready for read-only operations very early on node startup phase.
     *
     * Reference for read-only metastorage should be used only within this method and shouldn't be stored
     * to any field.
     *
     * @param metastorage Read-only meta storage.
     */
    default void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException { };

    /**
     * Fully functional metastore capable of performing reading and writing operations.
     *
     * Components interested in using metastore are allowed to keep reference passed into the method
     * in their fields.
     *
     * @param metastorage Fully functional meta storage.
     */
    default void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException { };
}

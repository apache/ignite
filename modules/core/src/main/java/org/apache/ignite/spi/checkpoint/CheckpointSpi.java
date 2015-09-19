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

package org.apache.ignite.spi.checkpoint;

import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

/**
 * Checkpoint SPI provides an ability to save an intermediate job state. It can
 * be useful when long running jobs need to store some intermediate state to
 * protect from system or application failures. Grid job can save intermediate
 * state in certain points of the execution (e.g., periodically) and upon start
 * check if previously saved state exists. This allows job to restart from the last
 * save checkpoint in case of preemption or other types of failover.
 * <p>
 * Note, that since a job can execute on different nodes, checkpoints need to
 * be accessible by all nodes.
 * <p>
 * To manipulate checkpoints from grid job the following public methods are available
 * on task session (that can be injected into grid job):
 * <ul>
 * <li>{@link org.apache.ignite.compute.ComputeTaskSession#loadCheckpoint(String)}</li>
 * <li>{@link org.apache.ignite.compute.ComputeTaskSession#removeCheckpoint(String)}</li>
 * <li>{@link org.apache.ignite.compute.ComputeTaskSession#saveCheckpoint(String, Object)}</li>
 * <li>{@link org.apache.ignite.compute.ComputeTaskSession#saveCheckpoint(String, Object, org.apache.ignite.compute.ComputeTaskSessionScope, long)}</li>
 * <li>{@link org.apache.ignite.compute.ComputeTaskSession#saveCheckpoint(String, Object, org.apache.ignite.compute.ComputeTaskSessionScope, long, boolean)}</li>
 * </ul>
 * <p>
 * Ignite provides the following {@code GridCheckpointSpi} implementations:
 * <ul>
 * <li>{@link org.apache.ignite.spi.checkpoint.noop.NoopCheckpointSpi} - default</li>
 * <li>{@link org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi}</li>
 * <li>{@ignitelink org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpi}</li>
 * <li>{@link org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi}</li>
 * <li>{@link org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpi}</li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface CheckpointSpi extends IgniteSpi {
    /**
     * Loads checkpoint from storage by its unique key.
     *
     * @param key Checkpoint key.
     * @return Loaded data or {@code null} if there is no data for a given
     *      key.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown in case of any error while loading
     *      checkpoint data. Note that in case when given {@code key} is not
     *      found this method will return {@code null}.
     */
    @Nullable public byte[] loadCheckpoint(String key) throws IgniteSpiException;

    /**
     * Saves checkpoint to the storage.
     *
     * @param key Checkpoint unique key.
     * @param state Saved data.
     * @param timeout Every intermediate data stored by checkpoint provider
     *      should have a timeout. Timeout allows for effective resource
     *      management by checkpoint provider by cleaning saved data that are not
     *      needed anymore. Generally, the user should choose the minimum
     *      possible timeout to avoid long-term resource acquisition by checkpoint
     *      provider. Value {@code 0} means that timeout will never expire.
     * @param overwrite Whether or not overwrite checkpoint if it already exists.
     * @return {@code true} if checkpoint has been actually saved, {@code false} otherwise.
     * @throws org.apache.ignite.spi.IgniteSpiException Thrown in case of any error while saving
     *    checkpoint data.
     */
    public boolean saveCheckpoint(String key, byte[] state, long timeout, boolean overwrite) throws IgniteSpiException;

    /**
     * This method instructs the checkpoint provider to clean saved data for a
     * given {@code key}.
     *
     * @param key Key for the checkpoint to remove.
     * @return {@code true} if data has been actually removed, {@code false}
     *      otherwise.
     */
    public boolean removeCheckpoint(String key);

    /**
     * Sets the checkpoint listener.
     *
     * @param lsnr The listener to set or {@code null}.
     */
    public void setCheckpointListener(CheckpointListener lsnr);
}
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

package org.apache.ignite.streamer.index;

import org.apache.ignite.*;

/**
 * Represents an actual instance of an index. Used by a {@link org.apache.ignite.streamer.StreamerWindow}
 * to perform event indexing.
 * <p>
 * To configure index for a streamer window, use
 * {@link org.apache.ignite.streamer.window.StreamerWindowAdapter#setIndexes(StreamerIndexProvider[])}.
 */
public interface StreamerIndexProvider<E, K, V> extends StreamerIndexProviderMBean {
    /**
     * Gets index name.
     *
     * @return Name of the index.
     */
    public String getName();

    /**
     * Gets user view for this index. This view is a snapshot
     * of a current index state. Once returned, it does not
     * change over time.
     *
     * @return User view for this index.
     */
    public StreamerIndex<E, K, V> index();

    /**
     * Initializes the index.
     */
    public void initialize();

    /**
     * Resets the index to an initial empty state.
     */
    public void reset();

    /**
     * Disposes the index.
     */
    public void dispose();

    /**
     * Adds an event to index.
     *
     * @param sync Index update synchronizer.
     * @param evt Event to add to an index.
     * @throws IgniteCheckedException If failed to add event to an index.
     */
    public void add(StreamerIndexUpdateSync sync, E evt) throws IgniteCheckedException;

    /**
     * Removes an event from index.
     *
     * @param sync Index update synchronizer.
     * @param evt Event to remove from index.
     * @throws IgniteCheckedException If failed to add event to an index.
     */
    public void remove(StreamerIndexUpdateSync sync, E evt) throws IgniteCheckedException;

    /**
     * Gets event indexing policy, which defines how events
     * are tracked within an index.
     *
     * @return index policy.
     */
    public StreamerIndexPolicy getPolicy();

    /**
     * Checks whether this index is unique or not. If it is, equal events
     * are not allowed, which means that if a newly-added event is found
     * to be equal to one of the already present events
     * ({@link Object#equals(Object)} returns {@code true}), an exception
     * is thrown.
     *
     * @return {@code True} for unique index.
     */
    public boolean isUnique();

    /**
     * Finalizes an update operation.
     *
     * @param sync Index update synchronizer.
     * @param evt Updated event.
     * @param rollback Rollback flag. If {@code true}, a rollback was made.
     * @param rmv Remove flag. If {@code true}, the event was removed from index.
     */
    public void endUpdate(StreamerIndexUpdateSync sync, E evt, boolean rollback, boolean rmv);
}

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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStore;

/**
 * A collection that contains {@link PageStore} elements.
 */
public interface PageStoreCollection {
    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return Page store for the corresponding parameters.
     * @throws IgniteCheckedException If cache or partition with the given ID was not created.
     */
    public PageStore getStore(int grpId, int partId) throws IgniteCheckedException;

    /**
     * @param grpId Cache group ID.
     * @return Collection of related page stores.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<PageStore> getStores(int grpId) throws IgniteCheckedException;
}

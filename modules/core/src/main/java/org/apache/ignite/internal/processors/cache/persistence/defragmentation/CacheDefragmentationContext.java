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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.util.GridSpinBusyLock;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CacheDefragmentationContext {
    private Map<Integer, List> partitionsByGroupId = new HashMap<>();

    private Map<Integer, File> cacheWorkDirsByGroupId = new HashMap<>();

    private Map<Integer, List> cacheStoresByGroupId = new HashMap<>();

    private Map<Integer, CacheGroupContext> groupContextsByGroupId = new HashMap<>();

    public final IgniteLogger log;

    private final DataRegion dataRegion;

    private volatile GridSpinBusyLock busyLock;

    public CacheDefragmentationContext(DataRegion dataRegion, IgniteLogger log) {
        this.dataRegion = dataRegion;

        this.log = log;
    }

    public File workDirForGroupId(int grpId) {
        return cacheWorkDirsByGroupId.get(grpId);
    }

    public GridSpinBusyLock busyLock() {
        return busyLock;
    }

    public Set<Integer> groupIdsForDefragmentation() {
        return partitionsByGroupId.keySet();
    }

    public Collection<Integer> partitionsForGroupId(int grpId) {
        return partitionsByGroupId.get(grpId);
    }

    public CacheGroupContext groupContextByGroupId(int grpId) {
        return groupContextsByGroupId.get(grpId);
    }

    public void onPageStoreCreated(int grpId, File cacheWorkDir, int p) {
        cacheWorkDirsByGroupId.computeIfAbsent(grpId, k -> cacheWorkDir);
    }

    public DataRegion defragmenationDataRegion() {
        return dataRegion;
    }

    public void onCacheStoreCreated(CacheGroupContext grp, int p, GridSpinBusyLock busyLock) {
        if (!grp.userCache())
            return;

        if (this.busyLock == null)
            this.busyLock = busyLock;

        groupContextsByGroupId.putIfAbsent(grp.groupId(), grp);

        try {
            if (!grp.shared().pageStore().exists(grp.groupId(), p))
                return;

            partitionsByGroupId.compute(grp.groupId(), (grpId, list) -> {
               if (list == null)
                   list = new ArrayList();

               list.add(p);

               return list;
            });

        } catch (IgniteCheckedException e) {
            // No-op.
        }
    }
}

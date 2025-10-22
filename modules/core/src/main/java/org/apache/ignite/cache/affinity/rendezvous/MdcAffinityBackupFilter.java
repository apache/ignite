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

package org.apache.ignite.cache.affinity.rendezvous;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiPredicate;

/** */
public class MdcAffinityBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
    /** */
    private static final long serialVersionUID = 1L;

    /** */
    private final int dcsCount;

    /** */
    private final int primaryAndBackups;

    /** */
    private final Map<String, Integer> partsDistrMap;

    /**
     * @param dcsCount
     * @param backups
     */
    public MdcAffinityBackupFilter(int dcsCount, int backups) {
        this.dcsCount = dcsCount;
        partsDistrMap = new HashMap<>(dcsCount + 1);
        primaryAndBackups = backups + 1;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode node, List<ClusterNode> list) {
        if (list.size() == 1) //account for primary node which is assigned beforehand
            partsDistrMap.put(list.get(0).dataCenterId(), 1);

        String candidateDcId = node.dataCenterId();
        Integer candDcPartsCopies = partsDistrMap.get(candidateDcId);
        boolean res = false;

        if (candDcPartsCopies == null || candDcPartsCopies == -1) {
            partsDistrMap.put(candidateDcId, 1);

            res = true;
        }
        else {
            int partCopiesPerDc = primaryAndBackups / dcsCount;

            if (candDcPartsCopies < partCopiesPerDc) {
                partsDistrMap.put(candidateDcId, candDcPartsCopies + 1);

                res = true;
            }
        }

        Optional<Integer> sum = partsDistrMap.values().stream().reduce(Integer::sum);

        if (sum.isPresent() && sum.get() == primaryAndBackups)
            partsDistrMap.replaceAll((e, v) -> -1);

        return res;
    }
}

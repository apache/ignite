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

package org.apache.ignite.compatibility.persistence;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** */
public class CompatibilityTestCore {
    /** */
    protected boolean customConsId;

    /** */
    protected boolean customSnpPath;

    /** */
    protected CacheGroupInfo cacheGrpInfo;

    /** */
    public CompatibilityTestCore(boolean customConsId, boolean customSnpPath, boolean testCacheGrp) {
        this.customConsId = customConsId;
        this.customSnpPath = customSnpPath;

        cacheGrpInfo = new CacheGroupInfo("test-cache", testCacheGrp ? 2 : 1);
    }

    /** */
    public CacheGroupInfo cacheGrpInfo() {
        return cacheGrpInfo;
    }



    /** */
    public String snpPath(String workDirPath, String snpName, boolean delIfExist) throws IgniteCheckedException {
        return Paths.get(snpDir(customSnpPath, workDirPath, delIfExist), snpName).toString();
    }


}

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

package org.apache.ignite.internal.visor.node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Data transfer object for memory configuration.
 */
public class VisorMemoryConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File cache allocation path. */
    private String fileCacheAllocationPath;

    /** Amount of memory allocated for the page cache. */
    private long pageCacheSize;

    /** Page size. */
    private int pageSize;

    /** Concurrency level. */
    private int concLvl;

    /** Name of MemoryPolicy to be used as default. */
    private String dfltMemPlcName;

    /** Memory policies. */
    private List<VisorMemoryPolicyConfiguration> memPlcs;

    /**
     * Create data transfer object.
     *
     * @param memCfg Memory configuration.
     */
    public VisorMemoryConfiguration(MemoryConfiguration memCfg) {
        assert memCfg != null;

        pageSize = memCfg.getPageSize();
        concLvl = memCfg.getConcurrencyLevel();
        dfltMemPlcName = memCfg.getDefaultMemoryPolicyName();

        MemoryPolicyConfiguration[] plcs = memCfg.getMemoryPolicies();

        if (!F.isEmpty(plcs)) {
            memPlcs = new ArrayList<>(plcs.length);

            for (MemoryPolicyConfiguration plc : plcs)
                memPlcs.add(new VisorMemoryPolicyConfiguration(plc));
        }
    }

    /**
     * @return Concurrency level.
     */
    public int getConcurrencyLevel() {
        return concLvl;
    }

    /**
     * @return File allocation path.
     */
    public String fileCacheAllocationPath() {
        return fileCacheAllocationPath;
    }

    /**
     * @return Page cache size, in bytes.
     */
    public long pageCacheSize() {
        return pageCacheSize;
    }

    /**
     * @return Page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @return Name of MemoryPolicy to be used as default.
     */
    public String getDefaultMemoryPolicyName() {
        return dfltMemPlcName;
    }

    /**
     * @return Collection of MemoryPolicyConfiguration objects.
     */
    public List<VisorMemoryPolicyConfiguration> getMemoryPolicies() {
        return memPlcs;
    }
}

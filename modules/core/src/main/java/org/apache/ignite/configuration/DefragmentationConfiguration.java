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

package org.apache.ignite.configuration;

import java.io.Serializable;

/**
 *
 */
public final class DefragmentationConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    public static final long DFLT_MAPPIN_REGION_SIZE = 256L * 1024 * 1024;

    /**
     *
     */
    private long regionSize = DataStorageConfiguration.DFLT_DATA_REGION_MAX_SIZE;

    /**
     *
     */
    private long mappingRegionSize = DFLT_MAPPIN_REGION_SIZE;

    /**
     * @return Region size.
     */
    public long getRegionSize() {
        return regionSize;
    }

    /**
     * @param regionSize
     */
    public DefragmentationConfiguration setRegionSize(long regionSize) {
        this.regionSize = regionSize;

        return this;
    }

    /**
     * @return Mapping region size.
     */
    public long getMappingRegionSize() {
        return mappingRegionSize;
    }

    /**
     * @param mappingRegionSize
     */
    public DefragmentationConfiguration setMappingRegionSize(long mappingRegionSize) {
        this.mappingRegionSize = mappingRegionSize;

        return this;
    }
}

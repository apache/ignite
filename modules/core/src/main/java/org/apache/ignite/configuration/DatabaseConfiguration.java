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
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Database configuration used to configure database.
 */
public class DatabaseConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default cache size is 1Gb. */
    public static final long DFLT_PAGE_CACHE_SIZE = 1024 * 1024 * 1024;

    /** Default page size. */
    public static final int DFLT_PAGE_SIZE = 2 * 1024;

    /** Page size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** File cache allocation path. */
    private String fileCacheAllocationPath;

    /** Amount of memory allocated for the page cache. */
    private long pageCacheSize = DFLT_PAGE_CACHE_SIZE;

    /** Concurrency level. */
    private int concLvl;

    /**
     * @return Page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @param pageSize Page size.
     */
    public void setPageSize(int pageSize) {
        A.ensure(pageSize >= 1024 && pageSize <= 16 * 1024, "Page size must be between 1kB and 16kB.");

        this.pageSize = pageSize;
    }

    /**
     * @return File allocation path.
     */
    public String getFileCacheAllocationPath() {
        return fileCacheAllocationPath;
    }

    /**
     * @param fileCacheAllocationPath File allocation path.
     */
    public void setFileCacheAllocationPath(String fileCacheAllocationPath) {
        this.fileCacheAllocationPath = fileCacheAllocationPath;
    }

    /**
     * @return Page cache size, in bytes.
     */
    public long getPageCacheSize() {
        return pageCacheSize;
    }

    /**
     * @param pageCacheSize Page cache size, in bytes.
     */
    public void setPageCacheSize(long pageCacheSize) {
        this.pageCacheSize = pageCacheSize;
    }

    /**
     * @return Concurrency level.
     */
    public int getConcurrencyLevel() {
        return concLvl;
    }

    /**
     * @param concLvl Concurrency level.
     */
    public void setConcurrencyLevel(int concLvl) {
        this.concLvl = concLvl;
    }
}

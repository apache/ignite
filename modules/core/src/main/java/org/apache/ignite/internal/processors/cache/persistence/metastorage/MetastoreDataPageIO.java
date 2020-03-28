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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;

/**
 * Data pages IO for using with meta store.
 */
public class MetastoreDataPageIO extends SimpleDataPageIO {
    /** */
    public static final IOVersions<MetastoreDataPageIO> VERSIONS = new IOVersions<>(
        new MetastoreDataPageIO(1)
    );

    /**
     * @param ver Version.
     */
    public MetastoreDataPageIO(int ver) {
        super(T_DATA_METASTORAGE, ver);
    }
}

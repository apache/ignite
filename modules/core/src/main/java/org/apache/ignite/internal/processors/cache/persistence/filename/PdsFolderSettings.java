/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class holds information required for folder generation for ignite persistent store
 */
public class PdsFolderSettings {
    private final Object consistentId;

    /** folder name containing consistent ID and optionally node index */
    private final String folderName;
    private final boolean compatible;

    public PdsFolderSettings(Object consistentId, boolean compatible) {
        this.consistentId = consistentId;
        this.compatible = compatible;
        this.folderName = U.maskForFileName(consistentId.toString());
    }

    public PdsFolderSettings(UUID consistentId, String folderName, int nodeIdx, boolean compatible) {
        this.consistentId = consistentId;
        this.folderName = folderName;
        this.compatible = compatible;
    }

    public String folderName() {
        return folderName;
    }
}

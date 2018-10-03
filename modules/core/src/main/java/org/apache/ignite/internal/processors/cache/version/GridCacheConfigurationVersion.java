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
package org.apache.ignite.internal.processors.cache.version;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

public class GridCacheConfigurationVersion implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final int id;

    private final GridCacheConfigurationChangeAction lastAction;

    private final String name;

    private final String groupName;

    public GridCacheConfigurationVersion(String name, String groupName) {
        this(0, null, name, groupName);
    }

    private GridCacheConfigurationVersion(
        int id,
        GridCacheConfigurationChangeAction action,
        String name,
        String groupName
    ) {
        this.id = id;
        this.lastAction = action;
        this.name = name;
        this.groupName = groupName;
    }

    public int id() {
        return id;
    }

    public GridCacheConfigurationChangeAction lastAction() {
        return lastAction;
    }

    public GridCacheConfigurationVersion nextVersion(@NotNull GridCacheConfigurationChangeAction action) {
            return new GridCacheConfigurationVersion(id + 1, action, name, groupName);
    }

    public boolean isNeedUpdateVersion(@NotNull GridCacheConfigurationChangeAction action){
        if(action == GridCacheConfigurationChangeAction.META_CHANGED)
            return true;

        return lastAction != action;
    }

    public String groupName(){ return groupName; }

    public String name(){ return name; }

     /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheConfigurationVersion.class, this);
    }

}
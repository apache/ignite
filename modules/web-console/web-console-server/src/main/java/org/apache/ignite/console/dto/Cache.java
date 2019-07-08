/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.dto;

import java.util.UUID;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.springframework.context.support.MessageSourceAccessor;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * DTO for cluster cache.
 */
public class Cache extends DataObject {
    /** */
    private String name;

    /** */
    private CacheMode cacheMode;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private int backups;

    /**
     * @param json JSON data.
     * @return New instance of cache DTO.
     */
    public static Cache fromJson(JsonObject json) {
        UUID id = json.getUuid("id");
        MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

        if (id == null)
            throw new IllegalStateException(messages.getMessage("err.cache-id-not-found"));

        return new Cache(
            id,
            json.getString("name"),
            CacheMode.valueOf(json.getString("cacheMode", PARTITIONED.name())),
            CacheAtomicityMode.valueOf(json.getString("atomicityMode", ATOMIC.name())),
            json.getInteger("backups", 0),
            toJson(json)
        );
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param name Cache name.
     * @param json JSON payload.
     */
    public Cache(
        UUID id,
        String name,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups,
        String json
    ) {
        super(id, json);

        this.name = name;
        this.cacheMode = cacheMode;
        this.atomicityMode = atomicityMode;
        this.backups = backups;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode cacheMode() {
        return cacheMode;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @return Cache backups.
     */
    public int backups() {
        return backups;
    }

    /** {@inheritDoc} */
    @Override public JsonObject shortView() {
        return new JsonObject()
            .add("id", getId())
            .add("name", name)
            .add("cacheMode", cacheMode)
            .add("atomicityMode", atomicityMode)
            .add("backups", backups);
    }
}

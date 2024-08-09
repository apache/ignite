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

import io.vertx.core.json.JsonObject;


/**
 * Abstract data object.
 */
public abstract class DataObject extends AbstractDto {
    /** */
    private String json;

    /**
     * Default constructor.
     */
    protected DataObject() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param json JSON encoded payload.
     */
    protected DataObject(UUID id, String json) {
        super(id);

        this.json = json;
    }

    /**
     * @return JSON encoded payload.
     */
    public String json() {
        return json;
    }

    /**
     * @param json JSON encoded payload.
     */
    public void json(String json) {
        this.json = json;
    }

    /**
     * @return JSON value suitable for short lists.
     */
    public abstract JsonObject shortView();
}

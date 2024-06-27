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
 * Base class for DTO objects.
 */
public abstract class AbstractDto implements java.io.Serializable{
    /** */
    protected UUID id;

    /**
     * Default constructor.
     */
    protected AbstractDto() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     */
    protected AbstractDto(UUID id) {
        this.id = id;
    }
    
    /**
     * Full constructor.
     *
     * @param id ID.
     */
    protected AbstractDto(String id) {
        this.id = UUID.fromString(id);
    }


    /**
     * @return Object ID.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id Object ID.
     */
    public void setId(UUID id) {
        this.id = id;
    }
    
    public static UUID getUUID(JsonObject json,String field) {
    	Object guid = json.getMap().get(field);
    	if(guid instanceof UUID) {
    		return (UUID)guid;
    	}
    	return UUID.fromString(guid.toString());
    }
}

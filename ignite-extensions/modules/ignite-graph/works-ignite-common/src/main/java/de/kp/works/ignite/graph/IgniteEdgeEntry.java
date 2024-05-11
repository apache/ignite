package de.kp.works.ignite.graph;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

public class IgniteEdgeEntry {

    public final String cacheKey;

    public String id;
    public String idType;
    public String label;
    public String toId;
    public String toIdType;
    public String fromId;
    public String fromIdType;
    public Long createdAt;
    public Long updatedAt;
    public String propKey;
    public String propType;
    public Object propValue;

    public IgniteEdgeEntry(           
            String id,
            String idType,
            String label,
            String toId,
            String toIdType,
            String fromId,
            String fromIdType,
            Long createdAt,
            Long updatedAt,
            String propKey,
            String propType,
            Object propValue) {
    	// String cacheKey = UUID.randomUUID().toString();
        this.cacheKey =  id + ':' + propKey;
        this.id = id;
        this.idType = idType;

        this.label = label;

        this.toId = toId;
        this.toIdType = toIdType;

        this.fromId = fromId;
        this.fromIdType = fromIdType;

        this.createdAt = createdAt;
        this.updatedAt = updatedAt;

        this.propKey   = propKey;
        this.propType  = propType;
        this.propValue = propValue;

    }
    
    public IgniteEdgeEntry(
    		String cacheKey,
            String id,
            String idType,
            String label,
            String toId,
            String toIdType,
            String fromId,
            String fromIdType,
            Long createdAt,
            Long updatedAt,
            String propKey,
            String propType,
            Object propValue) {    	
        this.cacheKey =  cacheKey;
        this.id = id;
        this.idType = idType;

        this.label = label;

        this.toId = toId;
        this.toIdType = toIdType;

        this.fromId = fromId;
        this.fromIdType = fromIdType;

        this.createdAt = createdAt;
        this.updatedAt = updatedAt;

        this.propKey   = propKey;
        this.propType  = propType;
        this.propValue = propValue;

    }

}

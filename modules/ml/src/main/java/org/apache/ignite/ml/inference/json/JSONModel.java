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

package org.apache.ignite.ml.inference.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.ignite.ml.IgniteModel;

/** Basic class for all non-trivial model data serialization. */
public abstract class JSONModel {
    /** Basic Ignite version. */
    @JsonIgnore
    public static final String JSON_MODEL_FORMAT_VERSION = "1";

    /** Ignite version. */
    public String formatVersion = JSON_MODEL_FORMAT_VERSION;

    /** Timestamp in ms from System.currentTimeMillis() method. */
    public Long timestamp;

    /** Unique string indetifier. */
    public String uid;

    /** String description of model class. */
    public String modelClass;

    /** Convert JSON string to IgniteModel object. */
    public abstract IgniteModel convert();

    /** */
    public JSONModel(Long timestamp, String uid, String modelClass) {
        this.timestamp = timestamp;
        this.uid = uid;
        this.modelClass = modelClass;
    }

    /** */
    @JsonCreator
    public JSONModel() {
    }
}

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

package org.apache.ignite.ml.xgboost;

import java.util.HashMap;
import java.util.Map;

/** Map based implementation of {@link XGObject}. */
public class MapBasedXGObject implements XGObject {
    /** */
    private static final long serialVersionUID = 4378979710350902592L;

    /** Key-value map. */
    private final Map<String, Double> map;

    /**
     * Constructs a new instance of map based {@link XGObject} with empty map.
     */
    public MapBasedXGObject() {
        this(new HashMap<>());
    }

    /**
     * Constructs a new instance of map based {@link XGObject} with the specified map.
     *
     * @param map Map.
     */
    public MapBasedXGObject(Map<String, Double> map) {
        this.map = map;
    }

    /** {@inheritDoc} */
    @Override public Double getFeature(String featureName) {
        return map.get(featureName);
    }

    /**
     * Puts feature value with the specified feature name.
     *
     * @param featureName Feature name.
     * @param val Feature value.
     */
    public void put(String featureName, Double val) {
        map.put(featureName, val);
    }
}

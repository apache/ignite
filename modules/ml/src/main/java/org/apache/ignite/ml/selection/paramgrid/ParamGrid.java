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

package org.apache.ignite.ml.selection.paramgrid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.math.functions.IgniteDoubleConsumer;

/**
 * Keeps the grid of parameters.
 */
public class ParamGrid implements Serializable {
    /** Parameter values by parameter index. */
    private Map<Integer, Double[]> paramValuesByParamIdx = new HashMap<>();

    /** Parameter names by parameter index. */
    private Map<Integer, IgniteDoubleConsumer> settersByParamIdx = new HashMap<>();

    /** Parameter names by parameter index. */
    private Map<Integer, String> paramNamesByParamIdx = new HashMap<>();

    /** Parameter counter. */
    private int paramCntr;

    /** Parameter search strategy. */
    private HyperParameterTuningStrategy paramSearchStgy = new BruteForceStrategy();

    /** */
    public Map<Integer, Double[]> getParamValuesByParamIdx() {
        return Collections.unmodifiableMap(paramValuesByParamIdx);
    }

    /**
     * Adds a grid for the specific hyper parameter.
     * @param paramName The parameter name.
     * @param setter The method reference to trainer or preprocessor trainer setter.
     * @param params The array of the given hyper parameter values.
     * @return The updated ParamGrid.
     */
    public ParamGrid addHyperParam(String paramName, IgniteDoubleConsumer setter, Double[] params) {
        paramValuesByParamIdx.put(paramCntr, params);
        paramNamesByParamIdx.put(paramCntr, paramName);
        settersByParamIdx.put(paramCntr, setter);
        paramCntr++;
        return this;
    }

    /**
     * Set up the hyper-parameter searching strategy.
     *
     * @param paramSearchStgy Parameter search strategy.
     */
    public ParamGrid withParameterSearchStrategy(HyperParameterTuningStrategy paramSearchStgy) {
        this.paramSearchStgy = paramSearchStgy;
        return this;
    }

    /** Returns the Hyper-parameter tuning strategy. */
    public HyperParameterTuningStrategy getHyperParameterTuningStrategy() {
        return paramSearchStgy;
    }

    /** Returns setter for parameter with the given index. */
    public IgniteDoubleConsumer getSetterByIndex(int idx) {
        return settersByParamIdx.get(idx);
    }

    /** Returns the name of hyper-parameter by the given index. */
    public String getParamNameByIndex(int idx) {
        return paramNamesByParamIdx.get(idx);
    }

    /**
     * Prepare data for hyper-parameter tuning.
     */
    public List<Double[]> getParamRawData() {
        List<Double[]> res = new ArrayList<>();
        paramValuesByParamIdx.forEach(res::add);
        return res;
    }
}

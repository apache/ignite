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

package org.apache.ignite.ml.genetic.parameter;

import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for describing the characteristics of an individual Chromosome.
 */
public class ChromosomeCriteria {
    /** List of criteria for a Chromosome */
    private List<String> criteria = new ArrayList<String>();

    /**
     * Retrieve criteria
     *
     * @return List of strings
     */
    public List<String> getCriteria() {
        return criteria;
    }

    /**
     * Set criteria
     *
     * @param criteria List of criteria to be applied for a Chromosome ;Use format "name=value", ie: "coinType=QUARTER"
     */
    public void setCriteria(List<String> criteria) {
        this.criteria = criteria;
    }

}

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

package org.apache.ignite.ml.genetic;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Represents the discrete parts of a potential solution (ie: Chromosome)
 *
 * <p>
 *
 * Gene is a container for a POJO that developer will implement. <br/>
 *
 * For the Movie Fitness example, the Movie object is the POJO contained within Gene. <br/> NOTE: Gene resides in cache:
 * 'geneCache'. This cache is replicated.
 *
 *
 * </p>
 */
public class Gene {

    private static final AtomicLong ID_GEN = new AtomicLong();

    /** Id (indexed). */
    @QuerySqlField(index = true)
    private Long id;

    /** value used to model an individual Gene. */
    private Object value;

    /**
     * object Object  parameter.
     *
     * @param object
     */
    public Gene(Object object) {
        id = ID_GEN.incrementAndGet();
        this.value = object;
    }

    /**
     * @return value for Gene
     */
    public Object getValue() {
        return value;
    }

    /**
     * Set the Gene value
     *
     * @param object Value for Gene
     */
    public void setValue(Object object) {
        this.value = object;
    }

    /**
     * @return Primary key for Gene
     */
    public Long id() {
        return id;
    }

    @Override
    public String toString() {
        return "Gene [id=" + id + ", value=" + value + "]";
    }

}

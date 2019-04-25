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

package org.apache.ignite.ml.genetic;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.util.concurrent.atomic.AtomicLong;

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
    /** primary key of Gene */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** Id (indexed). */
    @QuerySqlField(index = true)
    private Long id;

    /** value used to model an individual Gene. */
    private Object val;

    /**
     * object Object  parameter.
     *
     * @param obj
     */
    public Gene(Object obj) {
        id = ID_GEN.incrementAndGet();
        this.val = obj;
    }

    /**
     * @return Value for Gene
     */
    public Object getVal() {
        return val;
    }

    /**
     * Set the Gene value
     *
     * @param obj Value for Gene
     */
    public void setVal(Object obj) {
        this.val = obj;
    }

    /**
     * @return Primary key for Gene
     */
    public Long id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Gene [id=" + id + ", value=" + val + "]";
    }

}

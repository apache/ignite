/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
     * @return value for Gene
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

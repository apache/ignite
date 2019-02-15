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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Represents a potential solution consisting of a fixed-length collection of genes. <br/>
 *
 * <p>
 *
 * NOTE: Chromosome resides in cache: GAGridConstants.POPULATION_CACHE. This cached is partitioned.
 *
 * </p>
 */
public class Chromosome {
    /** primary key of Chromosome */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** fitness score */
    @QuerySqlField(index = true)
    private double fitnessScore = -1;

    /** Id (indexed). */
    @QuerySqlField(index = true)
    private Long id;

    /** array of gene keys. */
    private long[] genes;

    /**
     * @param genes Primary keys of Genes
     */
    public Chromosome(long[] genes) {
        id = ID_GEN.incrementAndGet();
        this.genes = genes;
    }

    /**
     * Gets the fitnessScore
     *
     * @return This chromosome's fitness score
     */
    public double getFitnessScore() {
        return fitnessScore;
    }

    /**
     * Set the fitnessScore for this chromosome
     *
     * @param fitnessScore This chromosome's new fitness score
     */
    public void setFitnessScore(double fitnessScore) {
        this.fitnessScore = fitnessScore;
    }

    /**
     * Gets the gene keys (ie: primary keys) for this chromosome
     *
     * @return This chromosome's genes
     */
    public long[] getGenes() {
        return genes;
    }

    /**
     * Set the gene keys (ie: primary keys)
     *
     * @param genes This chromosome's new genes
     */
    public void setGenes(long[] genes) {
        this.genes = genes;
    }

    /**
     * Get the id (primary key) for this chromosome
     *
     * @return This chromosome's primary key
     */
    public Long id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Chromosome [fitnessScore=" + fitnessScore + ", id=" + id + ", genes=" + Arrays.toString(genes) + "]";
    }

}

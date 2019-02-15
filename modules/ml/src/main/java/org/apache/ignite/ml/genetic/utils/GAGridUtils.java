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

package org.apache.ignite.ml.genetic.utils;

import java.util.ArrayList;
import java.util.List;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.cache.PopulationCacheConfig;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;

/**
 * GA Grid Helper routines
 */
public class GAGridUtils {
    /**
     * Retrieve chromosomes
     *
     * @param ignite Ignite
     * @param qry Sql
     * @return List of Chromosomes
     */
    public static List<Chromosome> getChromosomes(Ignite ignite, String qry) {
        List<Chromosome> chromosomes = new ArrayList<Chromosome>();

        IgniteCache<Long, Chromosome> populationCache = ignite.getOrCreateCache(PopulationCacheConfig.populationCache());

        SqlQuery sql = new SqlQuery(Chromosome.class, qry);

        try (QueryCursor<Entry<Long, Chromosome>> cursor = populationCache.query(sql)) {
            for (Entry<Long, Chromosome> e : cursor)
                chromosomes.add(e.getValue());
        }

        return chromosomes;
    }

    /**
     * Retrieve genes in order
     *
     * @param ignite Ignite
     * @param chromosome Chromosome
     * @return List of Genes
     */
    public static List<Gene> getGenesInOrderForChromosome(Ignite ignite, Chromosome chromosome) {
        List<Gene> genes = new ArrayList<Gene>();
        IgniteCache<Long, Gene> cache = ignite.cache(GAGridConstants.GENE_CACHE);

        long[] primaryKeys = chromosome.getGenes();

        for (int k = 0; k < primaryKeys.length; k++) {

            StringBuffer sbSqlClause = new StringBuffer();
            sbSqlClause.append("_key IN ");
            sbSqlClause.append("(");
            sbSqlClause.append(primaryKeys[k]);
            sbSqlClause.append(")");

            SqlQuery sql = new SqlQuery(Gene.class, sbSqlClause.toString());

            try (QueryCursor<Entry<Long, Gene>> cursor = cache.query(sql)) {
                for (Entry<Long, Gene> e : cursor)
                    genes.add(e.getValue());
            }
        }

        return genes;
    }
}

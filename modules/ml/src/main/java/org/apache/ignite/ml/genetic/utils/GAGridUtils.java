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

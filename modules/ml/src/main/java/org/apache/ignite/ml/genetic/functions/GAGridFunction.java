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

package org.apache.ignite.ml.genetic.functions;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.utils.GAGridUtils;
import org.h2.tools.SimpleResultSet;

/**
 * Responsible for providing custom SQL functions to retrieve optimization results.
 */
public class GAGridFunction {
     /** */
    public GAGridFunction() {
    }

    /**
     * Retrieve solutions in descending order based on fitness score.
     *
     * @return Result set.
     * @throws SQLException If failed.
     */
    @QuerySqlFunction
    public static SimpleResultSet getSolutionsDesc() {
        return (getChromosomes("order by fitnessScore desc"));
    }

    /**
     * Retrieve solutions in ascending order based on fitness score.
     *
     * @return Result set
     * @throws SQLException If failed.
     */
    @QuerySqlFunction
    public static SimpleResultSet getSolutionsAsc() throws SQLException {
        return (getChromosomes("order by fitnessScore asc"));
    }

    /**
     * Retrieve and individual solution by Chromosome key.
     *
     * @param key Primary key of Chromosome.
     * @return SimpleResultSet.
     * @throws SQLException If failed.
     */
    @QuerySqlFunction
    public static SimpleResultSet getSolutionById(int key) throws SQLException {
        StringBuffer sbSqlClause = new StringBuffer();
        sbSqlClause.append("_key IN");
        sbSqlClause.append("(");
        sbSqlClause.append(key);
        sbSqlClause.append(")");
        return (getChromosomes(sbSqlClause.toString()));
    }

    /**
     * Helper routine to return 'pivoted' results using the provided query param.
     *
     * @param qry Sql
     * @return Result set
     */
    private static SimpleResultSet getChromosomes(String qry) {
        Ignite ignite = Ignition.localIgnite();

        List<Chromosome> chromosomes = GAGridUtils.getChromosomes(ignite, qry);

        SimpleResultSet rs2 = new SimpleResultSet();

        Chromosome aChrom = chromosomes.get(0);
        int genesCnt = aChrom.getGenes().length;

        rs2.addColumn("Chromosome Id", Types.INTEGER, 0, 0);
        rs2.addColumn("Fitness Score", Types.DOUBLE, 0, 0);

        for (int i = 0; i < genesCnt; i++) {
            int colIdx = i + 1;
            rs2.addColumn("Gene " + colIdx, Types.VARCHAR, 0, 0);
        }

        for (Chromosome rowChrom : chromosomes) {

            Object[] row = new Object[genesCnt + 2];
            row[0] = rowChrom.id();
            row[1] = rowChrom.getFitnessScore();

            List<Gene> genes = GAGridUtils.getGenesInOrderForChromosome(ignite, rowChrom);
            int i = 2;

            for (Gene gene : genes) {
                row[i] = gene.getVal().toString();
                i = i + 1;
            }
            //Add a row for an individual Chromosome
            rs2.addRow(row);
        }

        return rs2;
    }

}

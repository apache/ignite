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

package org.apache.ignite.examples.ml.genetic.change;

import java.util.ArrayList;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;

import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.GAGrid;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.parameter.ChromosomeCriteria;
import org.apache.ignite.ml.genetic.parameter.GAConfiguration;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;

/**
 * This example demonstrates how to use the GAGrid framework. <br/>
 *
 * This example is inspired by JGAP's "Minimize Make Change" example. <br/>
 *
 * In this example, the objective is to calculate the minimum number of coins that equal user specified amount of
 * change
 *
 * ie: -DAMOUNTCHANGE
 *
 * mvn exec:java -Dexec.mainClass="org.apache.ignite.examples.ml.genetic.change.OptimizeMakeChangeGAExample"
 * -DAMOUNTCHANGE=75
 *
 * <p> Remote nodes should always be started with special configuration file which enables P2P class loading: {@code
 * 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p> <p> Alternatively you can run ExampleNodeStartup in
 * another JVM which will start node with {@code examples/config/example-ignite.xml} configuration.</p>
 */
public class OptimizeMakeChangeGAExample {
    /** Ignite instance */
    private static Ignite ignite = null;

    /** GAGrid */
    private static GAGrid gaGrid = null;

    /** GAConfiguration */
    private static GAConfiguration gaConfig = null;

    /** amount of change */
    private static String sAmountChange = null;

    /** Ignite logger */
    private static IgniteLogger logger = null;

    /**
     * Executes example.
     *
     * Specify value for -DAMOUNTCHANGE JVM system variable
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String args[]) {
        System.setProperty("IGNITE_QUIET", "false");

        sAmountChange = "75";

        StringBuffer sbErrorMessage = new StringBuffer();
        sbErrorMessage.append("AMOUNTCHANGE System property not set. Please provide a valid value between 1 and 99. ");
        sbErrorMessage.append(" ");
        sbErrorMessage.append("IE: -DAMOUNTCHANGE=75");
        sbErrorMessage.append("\n");
        sbErrorMessage.append("Using default value: 75");

        //Check if -DAMOUNTCHANGE JVM system variable is provided
        if (System.getProperty("AMOUNTCHANGE") == null) {
            System.out.println(sbErrorMessage);
        }
        else {
            sAmountChange = System.getProperty("AMOUNTCHANGE");
        }

        try {

            //Create an Ignite instance as you would in any other use case.
            ignite = Ignition.start("examples/config/example-ignite.xml");

            logger = ignite.log();

            // Create GAConfiguration
            gaConfig = new GAConfiguration();

            // set Gene Pool
            List<Gene> genes = getGenePool();

            // set selection method
            gaConfig.setSelectionMethod(GAGridConstants.SELECTION_METHOD.SELECTON_METHOD_ELETISM);
            gaConfig.setElitismCount(10);

            // set the Chromosome Length to '4' since we have 4 coins.
            gaConfig.setChromosomeLength(4);

            // set population size
            gaConfig.setPopulationSize(500);

            // initialize gene pool
            gaConfig.setGenePool(genes);

            // set Truncate Rate
            gaConfig.setTruncateRate(.10);

            // set Cross Over Rate
            gaConfig.setCrossOverRate(.50);

            // set Mutation Rate
            gaConfig.setMutationRate(.50);

            // create and set Fitness function
            OptimizeMakeChangeFitnessFunction function = new OptimizeMakeChangeFitnessFunction(new Integer(sAmountChange));
            gaConfig.setFitnessFunction(function);

            // create and set TerminateCriteria
            OptimizeMakeChangeTerminateCriteria termCriteria = new OptimizeMakeChangeTerminateCriteria(ignite);

            ChromosomeCriteria chromosomeCriteria = new ChromosomeCriteria();

            List values = new ArrayList();

            values.add("coinType=QUARTER");
            values.add("coinType=DIME");
            values.add("coinType=NICKEL");
            values.add("coinType=PENNY");

            chromosomeCriteria.setCriteria(values);

            gaConfig.setChromosomeCriteria(chromosomeCriteria);
            gaConfig.setTerminateCriteria(termCriteria);

            // initialize GAGrid
            gaGrid = new GAGrid(gaConfig, ignite);

            logger.info("##########################################################################################");

            logger.info("Calculating optimal set of coins where amount of change is " + sAmountChange);

            logger.info("##########################################################################################");

            Chromosome fittestChromosome = gaGrid.evolve();

            Ignition.stop(true);

            ignite = null;

        }
        catch (Exception e) {
            System.out.println(e);
        }

    }

    /**
     * Helper routine to initialize Gene pool
     *
     * In typical usecase genes may be stored in database.
     *
     * @return List of Genes
     */
    private static List<Gene> getGenePool() {
        List<Gene> list = new ArrayList();

        Gene quarterGene1 = new Gene(new Coin(Coin.CoinType.QUARTER, 3));
        Gene quarterGene2 = new Gene(new Coin(Coin.CoinType.QUARTER, 2));
        Gene quarterGene3 = new Gene(new Coin(Coin.CoinType.QUARTER, 1));
        Gene quarterGene4 = new Gene(new Coin(Coin.CoinType.QUARTER, 0));

        Gene dimeGene1 = new Gene(new Coin(Coin.CoinType.DIME, 2));
        Gene dimeGene2 = new Gene(new Coin(Coin.CoinType.DIME, 1));
        Gene dimeGene3 = new Gene(new Coin(Coin.CoinType.DIME, 0));

        Gene nickelGene1 = new Gene(new Coin(Coin.CoinType.NICKEL, 1));
        Gene nickelGene2 = new Gene(new Coin(Coin.CoinType.NICKEL, 0));

        Gene pennyGene1 = new Gene(new Coin(Coin.CoinType.PENNY, 4));
        Gene pennyGene2 = new Gene(new Coin(Coin.CoinType.PENNY, 3));
        Gene pennyGene3 = new Gene(new Coin(Coin.CoinType.PENNY, 2));
        Gene pennyGene4 = new Gene(new Coin(Coin.CoinType.PENNY, 1));
        Gene pennyGene5 = new Gene(new Coin(Coin.CoinType.PENNY, 0));

        list.add(quarterGene1);
        list.add(quarterGene2);
        list.add(quarterGene3);
        list.add(quarterGene4);
        list.add(dimeGene1);
        list.add(dimeGene2);
        list.add(dimeGene3);
        list.add(nickelGene1);
        list.add(nickelGene2);
        list.add(pennyGene1);
        list.add(pennyGene2);
        list.add(pennyGene3);
        list.add(pennyGene4);
        list.add(pennyGene5);

        return list;
    }

}

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

package org.apache.ignite.examples.ml.genetic.helloworld;

import java.util.ArrayList;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.GAGrid;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.parameter.GAConfiguration;

/**
 * This example demonstrates how to use the GAGrid framework.
 *
 * In this example, we want to evolve a string of 11 characters such that the word 'HELLO WORLD'.
 *
 * is found.
 *
 *
 * How To Run:
 *
 * mvn exec:java -Dexec.mainClass="org.apache.ignite.examples.ml.genetic.helloworld.HelloWorldGAExample"
 *
 * <p> Remote nodes should always be started with special configuration file which enables P2P class loading: {@code
 * 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p> <p> Alternatively you can run ExampleNodeStartup in
 * another JVM which will start node with {@code examples/config/example-ignite.xml} configuration.</p>
 */
public class HelloWorldGAExample {
    /** Ignite instance */
    private static Ignite ignite = null;
    /** GAGrid */
    private static GAGrid gaGrid = null;
    /** GAConfiguration */
    private static GAConfiguration gaConfig = null;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String args[]) {
        System.setProperty("IGNITE_QUIET", "false");

        try {

            //Create an Ignite instance as you would in any other use case.

            ignite = Ignition.start("examples/config/example-ignite.xml");

            // Create GAConfiguration
            gaConfig = new GAConfiguration();

            // set Gene Pool
            List<Gene> genes = getGenePool();

            // set the Chromosome Length to '11' since 'HELLO WORLD' contains 11 characters.
            gaConfig.setChromosomeLength(11);

            // initialize gene pool
            gaConfig.setGenePool(genes);

            // create and set Fitness function
            HelloWorldFitnessFunction function = new HelloWorldFitnessFunction();
            gaConfig.setFitnessFunction(function);

            // create and set TerminateCriteria
            HelloWorldTerminateCriteria termCriteria = new HelloWorldTerminateCriteria(ignite);
            gaConfig.setTerminateCriteria(termCriteria);

            ignite.log();

            gaGrid = new GAGrid(gaConfig, ignite);
            // evolve the population
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
     * @return List<Gene>
     */
    private static List<Gene> getGenePool() {
        List<Gene> list = new ArrayList();

        char[] chars = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
            'T', 'U', 'V', 'W', 'X', 'Y', 'Z', ' '};

        for (int i = 0; i < chars.length; i++) {
            Gene gene = new Gene(new Character(chars[i]));
            list.add(gene);
        }
        return list;
    }

}

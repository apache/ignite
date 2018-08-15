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
 * This example demonstrates how to use the {@link GAGrid} framework. In this example, we want to evolve a string
 * of 11 characters such that the word 'HELLO WORLD' is found.
 * <p>
 * Code in this example launches Ignite grid, prepares simple test data (gene pool) and configures GA grid.</p>
 * <p>
 * After that it launches the process of evolution on GA grid and outputs the progress and results.</p>
 * <p>
 * You can change the test data and parameters of GA grid used in this example and re-run it to explore
 * this functionality further.</p>
 * <p>
 * How to run from command line:</p>
 * <p>
 * {@code mvn exec:java -Dexec.mainClass="org.apache.ignite.examples.ml.genetic.helloworld.HelloWorldGAExample"}</p>
 * <p>
 *  Remote nodes should always be started with special configuration file which enables P2P class loading: {@code
 * 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p>
 * <p>
 * Alternatively you can run ExampleNodeStartup in another JVM which will start node with
 * {@code examples/config/example-ignite.xml} configuration.</p>
 */
public class HelloWorldGAExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String args[]) {
        System.out.println(">>> HelloWorld GA grid example started.");

        System.setProperty("IGNITE_QUIET", "false");

        try {
            // Create an Ignite instance as you would in any other use case.
            Ignite ignite = Ignition.start("examples/config/example-ignite.xml");

            // Create GAConfiguration.
            GAConfiguration gaCfg = new GAConfiguration();

            // Set Gene Pool.
            List<Gene> genes = getGenePool();

            // Set the Chromosome Length to '11' since 'HELLO WORLD' contains 11 characters.
            gaCfg.setChromosomeLen(11);

            // Initialize gene pool.
            gaCfg.setGenePool(genes);

            // Create and set Fitness function.
            HelloWorldFitnessFunction function = new HelloWorldFitnessFunction();
            gaCfg.setFitnessFunction(function);

            // Create and set TerminateCriteria.
            HelloWorldTerminateCriteria termCriteria = new HelloWorldTerminateCriteria(ignite);
            gaCfg.setTerminateCriteria(termCriteria);

            ignite.log();

            GAGrid gaGrid = new GAGrid(gaCfg, ignite);

            // Evolve the population.
            Chromosome chromosome = gaGrid.evolve();

            System.out.println(">>> Evolution result: " + chromosome);

            Ignition.stop(true);

            System.out.println(">>> HelloWorld GA grid example completed.");
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Helper routine to initialize Gene pool.
     *
     * In typical use case genes may be stored in database.
     *
     * @return List of Gene objects.
     */
    private static List<Gene> getGenePool() {
        List<Gene> list = new ArrayList<>();

        char[] chars = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
            'T', 'U', 'V', 'W', 'X', 'Y', 'Z', ' '};

        for (char aChar : chars) {
            Gene gene = new Gene(aChar);
            list.add(gene);
        }

        return list;
    }
}

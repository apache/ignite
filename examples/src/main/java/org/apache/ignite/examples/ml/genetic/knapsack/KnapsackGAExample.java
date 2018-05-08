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

package org.apache.ignite.examples.ml.genetic.knapsack;

import java.util.ArrayList;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import org.apache.ignite.ml.genetic.GAGrid;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.parameter.GAConfiguration;

/**
 * This example demonstrates how to use the GAGrid framework.
 *
 * Example demonstrates Knapsack Problem:  Given a set of 30 items, each with a weight and a value, pack 10 items in
 * knapsack so that the total weight is less <= 20 lbs. and the total value is maximized.
 *
 *
 * How To Run:
 *
 * mvn exec:java -Dexec.mainClass="org.apache.ignite.examples.ml.genetic.knapsack.KnapsackGAExample"
 *
 * <p> Remote nodes should always be started with special configuration file which enables P2P class loading: {@code
 * 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p> <p> Alternatively you can run ExampleNodeStartup in
 * another JVM which will start node with {@code examples/config/example-ignite.xml} configuration.</p>
 */
public class KnapsackGAExample {
    /** Ignite instance */
    private static Ignite ignite = null;
    /** GAGrid */
    private static GAGrid gaGrid = null;
    /** GAConfiguration */
    private static GAConfiguration gaConfig = null;

    /**
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

            // set the Chromosome Length to '10' since our knapsack may contain a total of 10 items.
            gaConfig.setChromosomeLength(10);

            // initialize gene pool
            gaConfig.setGenePool(genes);

            // create and set Fitness function
            KnapsackFitnessFunction function = new KnapsackFitnessFunction();
            gaConfig.setFitnessFunction(function);

            // create and set TerminateCriteria
            KnapsackTerminateCriteria termCriteria = new KnapsackTerminateCriteria(ignite);
            gaConfig.setTerminateCriteria(termCriteria);

            ignite.log();

            gaGrid = new GAGrid(gaConfig, ignite);
            // evolve the population
            gaGrid.evolve();

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
        List<Gene> list = new ArrayList<Gene>();

        Item item1 = new Item();
        item1.setName("Swiss Army Knife");
        item1.setWeight(0.08125);
        item1.setValue(15);
        Gene gene1 = new Gene(item1);

        Item item2 = new Item();
        item2.setName("Duct Tape");
        item2.setWeight(1.3);
        item2.setValue(3);
        Gene gene2 = new Gene(item2);

        Item item3 = new Item();
        item3.setName("Rope (50 feet)");
        item3.setWeight(7);
        item3.setValue(10);
        Gene gene3 = new Gene(item3);

        Item item4 = new Item();
        item4.setName("Satellite phone");
        item4.setWeight(2);
        item4.setValue(8);
        Gene gene4 = new Gene(item4);

        Item item5 = new Item();
        item5.setName("Elmer's Glue");
        item5.setWeight(0.25);
        item5.setValue(2);
        Gene gene5 = new Gene(item5);

        Item item6 = new Item();
        item6.setName("Toilet Paper Roll");
        item6.setWeight(.5);
        item6.setValue(4);
        Gene gene6 = new Gene(item6);

        Item item7 = new Item();
        item7.setName("Binoculars");
        item7.setWeight(3);
        item7.setValue(5);
        Gene gene7 = new Gene(item7);

        Item item8 = new Item();
        item8.setName("Compass");
        item8.setWeight(0.0573202);
        item8.setValue(15);
        Gene gene8 = new Gene(item8);

        Item item9 = new Item();
        item9.setName("Jug (prefilled with water)");
        item9.setWeight(4);
        item9.setValue(6);
        Gene gene9 = new Gene(item9);

        Item item10 = new Item();
        item10.setName("Flashlight");
        item10.setWeight(2);
        item10.setValue(4);
        Gene gene10 = new Gene(item10);

        Item item11 = new Item();
        item11.setName("Box of paper clips");
        item11.setWeight(.9);
        item11.setValue(2);
        Gene gene11 = new Gene(item11);

        Item item12 = new Item();
        item12.setName("Gloves (1 pair)");
        item12.setWeight(.8125);
        item12.setValue(3);
        Gene gene12 = new Gene(item12);

        Item item13 = new Item();
        item13.setName("Scissors");
        item13.setWeight(0.2);
        item13.setValue(2);
        Gene gene13 = new Gene(item13);

        Item item14 = new Item();
        item14.setName("Signal Flair (4pk)");
        item14.setWeight(4);
        item14.setValue(5);
        Gene gene14 = new Gene(item14);

        Item item15 = new Item();
        item15.setName("Water Purifying System");
        item15.setWeight(0.5125);
        item15.setValue(4);
        Gene gene15 = new Gene(item15);

        Item item16 = new Item();
        item16.setName("Whistle");
        item16.setWeight(0.075);
        item16.setValue(2);
        Gene gene16 = new Gene(item16);

        Item item17 = new Item();
        item17.setName("Sleeping Bag");
        item17.setWeight(0.38125);
        item17.setValue(4);
        Gene gene17 = new Gene(item17);

        Item item18 = new Item();
        item18.setName("Insect Repellent");
        item18.setWeight(1.15);
        item18.setValue(3);
        Gene gene18 = new Gene(item18);

        Item item19 = new Item();
        item19.setName("Trowel");
        item19.setWeight(0.31875);
        item19.setValue(3);
        Gene gene19 = new Gene(item19);

        Item item20 = new Item();
        item20.setName("Lighter");
        item20.setWeight(.2);
        item20.setValue(4);
        Gene gene20 = new Gene(item20);

        Item item21 = new Item();
        item21.setName("Safety Horn");
        item21.setWeight(.21);
        item21.setValue(3);
        Gene gene21 = new Gene(item21);

        Item item22 = new Item();
        item22.setName("Headlamp");
        item22.setWeight(.8);
        item22.setValue(4);
        Gene gene22 = new Gene(item22);

        Item item23 = new Item();
        item23.setName("Freeze Dried Food Kit");
        item23.setWeight(2);
        item23.setValue(6);
        Gene gene23 = new Gene(item23);

        Item item24 = new Item();
        item24.setName("Sunscreen");
        item24.setWeight(.5);
        item24.setValue(4);
        Gene gene24 = new Gene(item24);

        Item item25 = new Item();
        item25.setName("Trekking Pole (Adjustable)");
        item25.setWeight(1.3);
        item25.setValue(4);
        Gene gene25 = new Gene(item25);

        Item item26 = new Item();
        item26.setName("Counter Assault Bear Spray");
        item26.setWeight(.5);
        item26.setValue(4);
        Gene gene26 = new Gene(item26);

        Item item27 = new Item();
        item27.setName("Insect Spray");
        item27.setWeight(.5);
        item27.setValue(3);
        Gene gene27 = new Gene(item27);

        Item item28 = new Item();
        item28.setName("Hand sanitizer");
        item28.setWeight(.625);
        item28.setValue(3);
        Gene gene28 = new Gene(item28);

        Item item29 = new Item();
        item29.setName("Mirror");
        item29.setWeight(.5);
        item29.setValue(3);
        Gene gene29 = new Gene(item29);

        Item item30 = new Item();
        item30.setName("First Aid Kit");
        item30.setWeight(3);
        item30.setValue(6);
        Gene gene30 = new Gene(item30);

        list.add(gene1);
        list.add(gene2);
        list.add(gene3);
        list.add(gene4);
        list.add(gene5);
        list.add(gene6);
        list.add(gene7);
        list.add(gene8);
        list.add(gene9);
        list.add(gene10);
        list.add(gene11);
        list.add(gene12);
        list.add(gene13);
        list.add(gene14);
        list.add(gene15);
        list.add(gene16);
        list.add(gene17);
        list.add(gene18);
        list.add(gene19);
        list.add(gene20);
        list.add(gene21);
        list.add(gene22);
        list.add(gene23);
        list.add(gene24);
        list.add(gene25);
        list.add(gene26);
        list.add(gene27);
        list.add(gene28);
        list.add(gene29);
        list.add(gene30);

        return list;
    }

}

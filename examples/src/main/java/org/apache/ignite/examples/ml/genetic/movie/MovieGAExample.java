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

package org.apache.ignite.examples.ml.genetic.movie;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.GAGrid;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.parameter.GAConfiguration;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;

/**
 * This example demonstrates how to use the GAGrid framework.
 *
 * In this example, we utilize GA Grid to calculate an optimal set of movies based on our interests in various genres
 * (ie: Action, Comedy, and Romance)
 *
 *
 * How To Run:
 *
 * mvn exec:java -Dexec.mainClass="org.apache.ignite.examples.ml.genetic.movie.MovieGAExample" -DGENRES=Action,Comedy
 *
 * <p> Remote nodes should always be started with special configuration file which enables P2P class loading: {@code
 * 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p> <p> Alternatively you can run ExampleNodeStartup in
 * another JVM which will start node with {@code examples/config/example-ignite.xml} configuration.</p>
 */
public class MovieGAExample {
    /** Ignite instance */
    private static Ignite ignite = null;
    /** GAGrid */
    private static GAGrid gaGrid = null;
    /** GAConfiguration */
    private static GAConfiguration gaConfig = null;

    /**
     * Executes example.
     *
     * Specify value for -DGENRES JVM system variable
     *
     * @param args Command line arguments, none required.
     */

    public static void main(String args[]) {
        System.setProperty("IGNITE_QUIET", "false");

        List genres = new ArrayList();
        String sGenres = "Action,Comedy,Romance";

        StringBuffer sbErrorMessage = new StringBuffer();
        sbErrorMessage.append("GENRES System property not set. Please provide GENRES information.");
        sbErrorMessage.append(" ");
        sbErrorMessage.append("IE: -DGENRES=Action,Comedy,Romance");
        sbErrorMessage.append("\n");
        sbErrorMessage.append("Using default value: Action,Comedy,Romance");

        if (System.getProperty("GENRES") == null) {
            System.out.println(sbErrorMessage);

        }
        else {
            sGenres = System.getProperty("GENRES");
        }

        StringTokenizer st = new StringTokenizer(sGenres, ",");

        while (st.hasMoreElements()) {
            String genre = st.nextToken();
            genres.add(genre);
        }

        // Create GAConfiguration
        gaConfig = new GAConfiguration();

        // set Gene Pool
        List<Gene> genes = getGenePool();

        // Define Chromosome
        gaConfig.setChromosomeLength(3);
        gaConfig.setPopulationSize(100);
        gaConfig.setGenePool(genes);
        gaConfig.setTruncateRate(.10);
        gaConfig.setCrossOverRate(.50);
        gaConfig.setMutationRate(.50);
        gaConfig.setSelectionMethod(GAGridConstants.SELECTION_METHOD.SELECTION_METHOD_TRUNCATION);

        //Create fitness function
        MovieFitnessFunction function = new MovieFitnessFunction(genres);

        //set fitness function
        gaConfig.setFitnessFunction(function);

        try {

            //Create an Ignite instance as you would in any other use case.
            ignite = Ignition.start("examples/config/example-ignite.xml");

            MovieTerminateCriteria termCriteria = new MovieTerminateCriteria(ignite);

            gaConfig.setTerminateCriteria(termCriteria);

            gaGrid = new GAGrid(gaConfig, ignite);

            ignite.log();
            Chromosome fittestChromosome = gaGrid.evolve();

            Ignition.stop(true);
            ignite = null;

        }
        catch (Exception e) {
            System.out.println(e);
        }

    }

    private static List<Gene> getGenePool() {
        List list = new ArrayList();

        Movie movie1 = new Movie();
        movie1.setName("The Matrix");
        movie1.setImdbRating(7);
        List genre1 = new ArrayList();
        genre1.add("SciFi");
        genre1.add("Action");
        movie1.setGenre(genre1);
        movie1.setRating("PG-13");
        movie1.setYear("1999");

        Gene gene1 = new Gene(movie1);

        Movie movie2 = new Movie();
        movie2.setName("The Dark Knight");
        movie2.setImdbRating(9.6);
        List genre2 = new ArrayList();
        genre2.add("Action");
        movie2.setGenre(genre2);
        movie2.setRating("PG-13");
        movie2.setYear("2008");

        Gene gene2 = new Gene(movie2);

        Movie movie3 = new Movie();
        movie3.setName("The Avengers");
        movie3.setImdbRating(9.6);
        movie3.setYear("2012");

        List genre3 = new ArrayList();
        genre3.add("Action");
        movie3.setGenre(genre3);
        movie3.setRating("PG-13");

        Gene gene3 = new Gene(movie3);

        Movie movie4 = new Movie();
        movie4.setName("The Hangover");
        movie4.setImdbRating(7.6);
        List genre4 = new ArrayList();
        genre4.add("Comedy");
        movie4.setGenre(genre4);
        movie4.setRating("R");
        movie4.setYear("2009");

        Gene gene4 = new Gene(movie4);

        Movie movie5 = new Movie();
        movie5.setName("The Hangover 2");
        movie5.setImdbRating(9.6);
        List genre5 = new ArrayList();
        genre5.add("Comedy");
        movie5.setGenre(genre5);
        movie5.setRating("R");
        movie5.setYear("2012");

        Gene gene5 = new Gene(movie5);

        Movie movie6 = new Movie();
        movie6.setName("This Means War");
        movie6.setImdbRating(6.4);
        List genre6 = new ArrayList();
        genre6.add("Comedy");
        genre6.add("Action");
        genre6.add("Romance");
        movie6.setGenre(genre6);
        movie6.setRating("PG-13");
        movie6.setYear("2012");

        Gene gene6 = new Gene(movie6);

        Movie movie7 = new Movie();
        movie7.setName("Hitch");
        movie7.setImdbRating(10);
        List genre7 = new ArrayList();
        genre7.add("Comedy");
        genre7.add("Romance");
        movie7.setGenre(genre7);
        movie7.setRating("PG-13");
        movie7.setYear("2005");

        Gene gene7 = new Gene(movie7);

        Movie movie8 = new Movie();
        movie8.setName("21 Jump Street");
        movie8.setImdbRating(6.7);
        List genre8 = new ArrayList();
        genre8.add("Comedy");
        genre8.add("Action");
        movie8.setGenre(genre8);
        movie8.setRating("R");
        movie8.setYear("2012");

        Gene gene8 = new Gene(movie8);

        Movie movie9 = new Movie();
        movie9.setName("Killers");
        movie9.setImdbRating(5.1);
        List genre9 = new ArrayList();
        genre9.add("Comedy");
        genre9.add("Action");
        genre9.add("Romance");
        movie9.setGenre(genre9);
        movie9.setRating("PG-13");
        movie9.setYear("2010");

        Gene gene9 = new Gene(movie9);

        Movie movie10 = new Movie();
        movie10.setName("What to Expect When You're Expecting");
        movie10.setImdbRating(5.1);
        List genre10 = new ArrayList();
        genre10.add("Comedy");
        genre10.add("Romance");
        movie10.setGenre(genre10);
        movie10.setRating("PG-13");
        movie10.setYear("2012");

        Gene gene10 = new Gene(movie10);

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

        return list;

    }

}

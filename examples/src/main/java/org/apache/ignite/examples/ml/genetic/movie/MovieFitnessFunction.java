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
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.IFitnessFunction;

/**
 * This example demonstrates how to create a IFitnessFunction
 *
 * Your IFitness function will vary depending on your particular use case.
 *
 * For this fitness function, we simply want to calculate the value of
 *
 * an individual solution relative to other solutions.
 *
 *
 * To do this, we simply increase fitness score by number of times
 *
 * genre is found in list of movies.
 *
 * In addition, we increase score by fictional IMDB rating.
 *
 * If there are duplicate movies in selection, we automatically apply a '0'
 *
 * fitness score.
 */
public class MovieFitnessFunction implements IFitnessFunction {
    /** genes */
    private List<String> genres = null;

    /**
     * @param genres List of genres
     */
    public MovieFitnessFunction(List<String> genres) {
        this.genres = genres;
    }

    /**
     * Calculate fitness score
     *
     * @param genes List of Genes
     * @return Fitness score
     */
    public double evaluate(List<Gene> genes) {

        double score = 0;
        List<String> dups = new ArrayList();
        int badSolution = 1;

        for (int i = 0; i < genes.size(); i++) {
            Movie movie = (Movie)genes.get(i).getVal();
            if (dups.contains(movie.getName())) {
                badSolution = 0;
            }
            else {
                dups.add(movie.getName());
            }
            double genreScore = getGenreScore(movie);
            if (genreScore == 0) {
                badSolution = 0;
            }
            score = (score + movie.getImdbRating()) + (genreScore);
        }
        return (score * badSolution);
    }

    /**
     * helper to calculate genre score
     *
     * @param movie Move
     * @return Genre score
     */
    private double getGenreScore(Movie movie) {
        double genreScore = 0;

        for (String genre : this.genres) {
            if (movie.getGenre().contains(genre)) {
                genreScore = genreScore + 1;
            }
        }
        return genreScore;
    }
}

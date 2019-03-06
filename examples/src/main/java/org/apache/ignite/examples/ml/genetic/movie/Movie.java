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

package org.apache.ignite.examples.ml.genetic.movie;

import java.util.List;

/**
 * POJO to model a movie.
 */
public class Movie {
    /** Name of movie. */
    private String name;
    /** Genre of movie. */
    private List genre;
    /** Rating of movie. */
    private String rating;

    /** IMDB rating. */
    private double imdbRating;

    /** Year of movie. */
    private String year;

    /**
     * Get the year.
     *
     * @return Year.
     */
    public String getYear() {
        return year;
    }

    /**
     * Set the year.
     *
     * @param year Year.
     */
    public void setYear(String year) {
        this.year = year;
    }

    /**
     * Get the <a href="https://en.wikipedia.org/wiki/IMDb">IMDB rating</a>.
     *
     * @return IMDB rating.
     */
    public double getImdbRating() {
        return imdbRating;
    }

    /**
     * Set the IMDB rating.
     *
     * @param imdbRating IMDB rating.
     */
    public void setImdbRating(double imdbRating) {
        this.imdbRating = imdbRating;
    }

    /**
     * Get the name of movie.
     *
     * @return Name of movie.
     */
    public String getName() {
        return name;
    }

    /**
     * Set the name of movie.
     *
     * @param name Movie name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get movie genres.
     *
     * @return List of genres.
     */
    public List getGenre() {
        return genre;
    }

    /**
     * Set the genre.
     *
     * @param genre List of genres of movie.
     */
    public void setGenre(List genre) {
        this.genre = genre;
    }

    /**
     * Get the rating of the movie.
     *
     * @return Movie rating.
     */
    public String getRating() {
        return rating;
    }

    /**
     * Set the rating of the movie.
     *
     * @param rating Movie rating.
     */
    public void setRating(String rating) {
        this.rating = rating;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Movie [name=" + name + ", genre=" + genre + ", rating=" + rating + ", imdbRating=" + imdbRating
            + ", year=" + year + "]";
    }
}

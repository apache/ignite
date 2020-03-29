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

package org.apache.ignite.ml.util.genetic;

import org.jetbrains.annotations.NotNull;

/**
 * Represents the set of genes, known as chromosome in genetic programming.
 *
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
public class Chromosome implements Comparable {
    /** Genes. */
    private Double[] genes;

    /** Fitness. */
    private Double fitness = Double.NaN;

    /**
     * @param size Size.
     */
    public Chromosome(int size) {
        this.genes = new Double[size];
    }

    /**
     * @param doubles Doubles.
     */
    public Chromosome(Double[] doubles) {
        genes = doubles.clone();
    }

    /**
     * Returns the double array chromosome representation.
     */
    public Double[] toDoubleArray() {
        return genes;
    }

    /**
     * Returns the fitness value.
     */
    public Double getFitness() {
        return fitness;
    }

    /**
     * Sets the fitness value.
     *
     * @param fitness Fitness.
     */
    public void setFitness(Double fitness) {
        this.fitness = fitness;
    }

    /**
     * Returns the amount of genes in chromosome.
     */
    public int size() {
        return genes.length;
    }

    /**
     * Returns the gene value by index.
     *
     * @param idx Index.
     */
    public double getGene(int idx) {
        return genes[idx];
    }

    /**
     * Sets gene value by index.
     *
     * @param idx       Index.
     * @param geneValue Gene value.
     */
    public void setGene(int idx, double geneValue) {
        genes[idx] = geneValue;
    }

    /**
     * Creates chromosome copy.
     */
    public Chromosome copy() {
        Chromosome cp = new Chromosome(genes);
        cp.fitness = fitness;
        return cp;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull Object o) {
        return (int) Math.signum(getFitness() - ((Chromosome) o).getFitness());
    }
}

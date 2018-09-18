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

package org.apache.ignite.ml.genetic.parameter;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.IFitnessFunction;

/**
 * Maintains configuration parameters to be used in genetic algorithm
 *
 * <br/>
 *
 * <p>
 *
 * NOTE: Default selectionMethod is SELECTION_METHOD_TRUNCATION
 *
 * Default truncateRate is .10
 *
 * More selectionMethods will be introduced in future releases.
 *
 * </p>
 */
public class GAConfiguration {
    /** Selection method */
    private GAGridConstants.SELECTION_METHOD selectionMtd = null;

    /** Criteria used to describe a chromosome */
    private ChromosomeCriteria chromosomeCriteria = null;
    /**
     * Percentage of most fit chromosomes to be maintained and utilized to copy into new population.
     *
     * NOTE: This parameter is only considered when selectionMethod is SELECTION_METHOD_TRUNCATION
     *
     * Accepted values between 0 and 1
     */
    private double truncateRate;
    /**
     * Elitism is the concept that the strongest members of the population will be preserved from generation to
     * generation. <br/>
     *
     * No crossovers or mutations will be performed for elite chromosomes.
     *
     * NOTE: This parameter is only considered when selectionMethod is SELECTON_METHOD_ELETISM.
     */
    private int elitismCnt = 0;

    /**
     * Indicates how chromosome fitness values should be evaluated. </br> A chromosome with
     * isHigherFitnessValueFitter=true is considered fittest.
     */
    private boolean isHigherFitnessValFitter = true;
    /**
     * Population size represents the number of potential solutions (ie: chromosomes) between each generation Default
     * size is 500
     *
     * </br> NOTE: The population size remains fixed between each generation
     */
    private int populationSize = 500;

    /** Gene pool is the sum of ALL genes utilized to create chromsomes */
    private List<Gene> genePool = new ArrayList<Gene>();

    /** Number of genes within a chromosome */
    private int chromosomeLen = 0;
    /**
     * Crossover rate is the probability that two chromosomes will breed with each other. offspring with traits of each
     * of the parents.
     *
     * Accepted values are between 0 and 1
     */
    private double crossOverRate = .50;
    /**
     * Mutation rate is the probability that a chromosome will be mutated offspring with traits of each of the parents.
     *
     *
     * Accepted values are between 0 and 1
     */
    private double mutationRate = .50;
    /**
     * Call back interface used to terminate Genetic algorithm.
     *
     * Implement this interface based on particular use case.
     */
    private ITerminateCriteria terminateCriteria = null;

    /**
     * Represents a fitness function. Implement the IFitnessFunction to satisfy your particular use case.
     */
    private IFitnessFunction fitnessFunction = null;

    public GAConfiguration() {
        this.setSelectionMtd(GAGridConstants.SELECTION_METHOD.SELECTION_METHOD_TRUNCATION);
        this.setTruncateRate(.10);
    }

    /**
     * retrieve the ChromosomeCriteria
     *
     * @return Chromosome criteria
     */
    public ChromosomeCriteria getChromosomeCriteria() {
        return chromosomeCriteria;
    }

    /**
     * set value for ChromosomeCriteria
     *
     * @param chromosomeCriteria Chromosome criteria
     */

    public void setChromosomeCriteria(ChromosomeCriteria chromosomeCriteria) {
        this.chromosomeCriteria = chromosomeCriteria;
    }

    /**
     * @return Boolean value indicating how fitness values should be evaluated.
     */
    public boolean isHigherFitnessValFitter() {
        return this.isHigherFitnessValFitter;
    }

    /**
     * Retrieve the chromosome length
     *
     * @return Size of Chromosome
     */
    public int getChromosomeLen() {
        return chromosomeLen;
    }

    /**
     * Set the Chromsome length
     *
     * @param chromosomeLen Size of Chromosome
     */
    public void setChromosomeLen(int chromosomeLen) {
        this.chromosomeLen = chromosomeLen;
    }

    /**
     * Retrieve the cross over rate
     *
     * @return Cross over rate
     */
    public double getCrossOverRate() {
        return crossOverRate;
    }

    /**
     * Set the cross over rate.
     *
     * @param crossOverRate Cross over rate
     */
    public void setCrossOverRate(double crossOverRate) {
        this.crossOverRate = crossOverRate;
    }

    /**
     * Retrieve the elitism count
     *
     * @return Elitism count
     */
    public int getElitismCnt() {
        return elitismCnt;
    }

    /**
     * Set the elitism count.
     *
     * @param elitismCnt Elitism count
     */
    public void setElitismCnt(int elitismCnt) {
        this.elitismCnt = elitismCnt;
    }

    /**
     * Retrieve IFitnessFunction
     *
     * @return Fitness function
     */
    public IFitnessFunction getFitnessFunction() {
        return fitnessFunction;
    }

    /**
     * Set IFitnessFunction
     *
     * @param fitnessFunction Fitness function
     */
    public void setFitnessFunction(IFitnessFunction fitnessFunction) {
        this.fitnessFunction = fitnessFunction;
    }

    /**
     * Retrieve the gene pool
     *
     * @return List of Genes
     */
    public List<Gene> getGenePool() {
        return (this.genePool);
    }

    /**
     * Set the gene pool.
     *
     * NOTE: When Apache Ignite is started the gene pool is utilized to initialize the distributed
     * GAGridConstants.GENE_CACHE.
     *
     * @param genePool List of Genes
     */
    public void setGenePool(List<Gene> genePool) {
        this.genePool = genePool;
    }

    /**
     * Retrieve the mutation rate.
     *
     * @return Mutation Rate
     */
    public double getMutationRate() {
        return mutationRate;
    }

    /**
     * Set the mutation rate.
     *
     * @param mutationRate Mutation Rate
     */
    public void setMutationRate(double mutationRate) {
        this.mutationRate = mutationRate;
    }

    /**
     * Retrieve the population size
     *
     * @return Population size
     */

    public int getPopulationSize() {
        return populationSize;
    }

    /**
     * Set the population size
     *
     * @param populationSize Size of population
     */
    public void setPopulationSize(int populationSize) {
        this.populationSize = populationSize;
    }

    /**
     * Get the selection method
     *
     * @return Selection method
     */
    public GAGridConstants.SELECTION_METHOD getSelectionMtd() {
        return selectionMtd;
    }

    /**
     * Set the selection method
     *
     * @param selectionMtd Selection method
     */
    public void setSelectionMtd(GAGridConstants.SELECTION_METHOD selectionMtd) {
        this.selectionMtd = selectionMtd;
    }

    /**
     * Retreive the termination criteria
     *
     * @return Termination Criteria
     */
    public ITerminateCriteria getTerminateCriteria() {
        return terminateCriteria;
    }

    /**
     * Set the termination criteria.
     *
     * @param terminateCriteria Termination Criteria
     */
    public void setTerminateCriteria(ITerminateCriteria terminateCriteria) {
        this.terminateCriteria = terminateCriteria;
    }

    /**
     * Retrieve truncateRate
     *
     * @return Truncate Rate
     */
    public double getTruncateRate() {
        return truncateRate;
    }

    /**
     * Set truncatePercentage
     *
     * @param truncateRate Truncate rate
     */
    public void setTruncateRate(double truncateRate) {
        this.truncateRate = truncateRate;
    }

}

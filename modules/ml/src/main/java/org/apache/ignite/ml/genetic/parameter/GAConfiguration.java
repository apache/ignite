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
 *
 */
public class GAConfiguration {

    private GAGridConstants.SELECTION_METHOD selectionMethod = null;

    /** Criteria used to describe a chromosome */
    private ChromosomeCriteria chromosomeCritiera = null;
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
    private int elitismCount = 0;
    /**
     * Indicates how chromosome fitness values should be evaluated
     *
     * <br/> IE: A chromosome with HIGHEST_FITNESS_VALUE_IS_FITTER is considered fittest. A chromosome with
     * LOWEST_FITNESS_VALUE_IS_FITTER is considered fittest.
     */
    private GAGridConstants.FITNESS_EVALUATER_TYPE fittnessEvaluator =
        GAGridConstants.FITNESS_EVALUATER_TYPE.HIGHEST_FITNESS_VALUE_IS_FITTER;
    /**
     * Population size represents the number of potential solutions (ie: chromosomes) between each generation Default
     * size is 500
     *
     * </br> NOTE: The population size remains fixed between each generation
     */
    private int populationSize = 500;

    /** Gene pool is the sum of ALL genes utilized to create chromsomes */
    private List<Gene> genePool = new ArrayList();

    /** Number of genes within a chromosome */
    private int chromosomeLength = 0;
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
        this.setSelectionMethod(GAGridConstants.SELECTION_METHOD.SELECTION_METHOD_TRUNCATION);
        this.setTruncateRate(.10);
    }

    /**
     * retrieve the ChromosomeCriteria
     *
     * @return ChromosomeCriteria
     */
    public ChromosomeCriteria getChromosomeCritiera() {
        return chromosomeCritiera;
    }

    /**
     * set value for ChromosomeCriteria
     *
     * @param chromosomeCritiera
     */

    public void setChromosomeCritiera(ChromosomeCriteria chromosomeCritiera) {
        this.chromosomeCritiera = chromosomeCritiera;
    }

    /**
     * Retrieve the chromosome length
     *
     * @return int
     */
    public int getChromosomeLength() {
        return chromosomeLength;
    }

    /**
     * @param chromosomeLength
     */
    public void setChromosomeLength(int chromosomeLength) {
        this.chromosomeLength = chromosomeLength;
    }

    /**
     * Retrieve the cross over rate.
     *
     * @return double.
     */
    public double getCrossOverRate() {
        return crossOverRate;
    }

    /**
     * Set the cross over rate.
     *
     * @param double.
     */
    public void setCrossOverRate(double crossOverRate) {
        this.crossOverRate = crossOverRate;
    }

    /**
     * Retrieve the elitism count.
     *
     * @return int.
     */
    public int getElitismCount() {
        return elitismCount;
    }

    /**
     * Set the elitism count.
     *
     * @return int
     */
    public void setElitismCount(int elitismCount) {
        this.elitismCount = elitismCount;
    }

    /**
     * Retrieve FITNESS_EVALUATER_TYPE
     *
     * @return FITNESS_EVALUATER_TYPE
     */
    public GAGridConstants.FITNESS_EVALUATER_TYPE getFitnessEvaluator() {
        return fittnessEvaluator;
    }

    /**
     * Set value for FITNESS_EVALUATER_TYPE
     *
     * @param FITNESS_EVALUATER_TYPE
     */
    public void setFitnessEvaluator(GAGridConstants.FITNESS_EVALUATER_TYPE fittnessIndicator) {
        this.fittnessEvaluator = fittnessEvaluator;
    }

    /**
     * Retrieve IFitnessFunction
     *
     * @return IFitnessFunction
     */
    public IFitnessFunction getFitnessFunction() {
        return fitnessFunction;
    }

    /**
     * Set IFitnessFunction
     *
     * @param IFitnessFunction
     */
    public void setFitnessFunction(IFitnessFunction fitnessFunction) {
        this.fitnessFunction = fitnessFunction;
    }

    /**
     * Retrieve the gene pool.
     *
     * @return List
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
     * @param genePool
     */
    public void setGenePool(List<Gene> genePool) {
        this.genePool = genePool;
    }

    /**
     * Retrieve the mutation rate.
     *
     * @return double
     */
    public double getMutationRate() {
        return mutationRate;
    }

    /**
     * Set the mutation rate.
     *
     * @param double
     */
    public void setMutationRate(double mutationRate) {
        this.mutationRate = mutationRate;
    }

    /**
     * Retrieve the population size
     *
     * @return int
     */

    public int getPopulationSize() {
        return populationSize;
    }

    /**
     * Set the population size
     *
     * @param int
     */
    public void setPopulationSize(int populationSize) {
        this.populationSize = populationSize;
    }

    public GAGridConstants.SELECTION_METHOD getSelectionMethod() {
        return selectionMethod;
    }

    /**
     * set the selectionMethod
     *
     * @param seletionMethod
     */
    public void setSelectionMethod(GAGridConstants.SELECTION_METHOD selectionMethod) {
        this.selectionMethod = selectionMethod;
    }

    /**
     * Retreive the termination criteria.
     *
     * @return ITerminateCriteria.
     */
    public ITerminateCriteria getTerminateCriteria() {
        return terminateCriteria;
    }

    /**
     * Set the termination criteria.
     *
     * @param ITerminateCriteria.
     */
    public void setTerminateCriteria(ITerminateCriteria terminateCriteria) {
        this.terminateCriteria = terminateCriteria;
    }

    /**
     * Retrieve truncateRate
     *
     * @return double
     */
    public double getTruncateRate() {
        return truncateRate;
    }

    /**
     * Set truncatePercentage
     *
     * @param double
     */
    public void setTruncateRate(double truncateRate) {
        this.truncateRate = truncateRate;
    }

}

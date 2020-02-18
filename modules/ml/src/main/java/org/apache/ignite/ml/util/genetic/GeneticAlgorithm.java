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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.math3.util.Pair;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

/**
 * This class is an entry point to use Genetic Algorithm to solve optimization problem.
 */
public class GeneticAlgorithm {
    /** Uniform rate. */
    private static final double UNIFORM_RATE = 0.5;

    /** Population size. */
    private int populationSize;

    /** Amount of elite chromosomes. */
    private int amountOfEliteChromosomes = 2;

    /** Amount of generations. */
    private int amountOfGenerations = 10;

    /** Seed. */
    private long seed = 123L;

    /** Crossingover probability. */
    private double crossingoverProbability = 0.9;

    /** Mutation probability. */
    private double mutationProbability = 0.1;

    /** Random generator. */
    private Random rnd = new Random(seed);

    /** Population. */
    private Population population;

    /** Fitness function. */
    private Function<Chromosome, Double> fitnessFunction;

    /** Mutation operator. */
    private BiFunction<Integer, Double, Double> mutationOperator;

    /** Crossover strategy. */
    private CrossoverStrategy crossoverStgy = CrossoverStrategy.UNIFORM;

    /** Selection strategy. */
    private SelectionStrategy selectionStgy = SelectionStrategy.ROULETTE_WHEEL;

    /**
     * @param paramSet The list of sets of parameter's values.
     */
    public GeneticAlgorithm(List<Double[]> paramSet) {
        initializePopulation(paramSet);
    }

    /**
     * Forms the initial population.
     *
     * @param rawDataForPopulationFormation Rnd parameter sets.
     */
    private Population initializePopulation(List<Double[]> rawDataForPopulationFormation) {
        // validate that population size should be even
        // elite chromosome should be even too or we should handle odd case especially
        populationSize = rawDataForPopulationFormation.size();
        population = new Population(populationSize);
        for (int i = 0; i < populationSize; i++)
            population.setChromosome(i, new Chromosome(rawDataForPopulationFormation.get(i)));

        return population;
    }

    /**
     * The main method for genetic algorithm.
     */
    public void run() {
        if (population != null) {
            population.calculateFitnessForAll(fitnessFunction);
            int i = 0;
            while (stopCriteriaIsReached(i)) {
                Population newPopulation = new Population(populationSize);

                newPopulation = selectEliteChromosomes(newPopulation);

                Population parents = selectionParents();

                newPopulation = crossingover(parents, newPopulation);

                newPopulation = mutate(newPopulation);

                // update fitness for new population
                for (int j = amountOfEliteChromosomes; j < populationSize; j++)
                    newPopulation.calculateFitnessForChromosome(j, fitnessFunction);

                population = newPopulation;

                i++;
            }
        }
    }

    /**
     * The main method for genetic algorithm.
     *
     * @param environment The environment.
     */
    public void runParallel(LearningEnvironment environment) {
        if (population != null) {
            population.calculateFitnessForAll(fitnessFunction); // TODO: parallelize this
            int i = 0;
            while (stopCriteriaIsReached(i)) {
                Population newPopulation = new Population(populationSize);

                newPopulation = selectEliteChromosomes(newPopulation);

                Population parents = selectionParents();

                newPopulation = crossingover(parents, newPopulation);

                newPopulation = mutate(newPopulation);

                // update fitness for new population
                List<IgniteSupplier<Pair<Integer, Double>>> tasks = new ArrayList<>();
                for (int j = amountOfEliteChromosomes; j < populationSize; j++) {
                    int finalJ = j;
                    Population finalNewPopulation1 = newPopulation;
                    IgniteSupplier<Pair<Integer, Double>> task = () -> new Pair<>(finalJ, fitnessFunction.apply(finalNewPopulation1.getChromosome(finalJ)));
                    tasks.add(task);
                }

                List<Pair<Integer, Double>> taskResults = environment.parallelismStrategy().submit(tasks).stream()
                    .map(Promise::unsafeGet)
                    .collect(Collectors.toList());

                Population finalNewPopulation = newPopulation;
                taskResults.forEach(p -> finalNewPopulation.setFitness(p.getKey(), p.getValue()));

                population = newPopulation;

                i++;
            }
        }
    }

    /**
     * The common method of parent population building with different selection strategies.
     */
    private Population selectionParents() {
        switch (selectionStgy) {
            case ROULETTE_WHEEL:
                return selectParentsByRouletteWheel();
            default:
                throw new UnsupportedOperationException("This strategy "
                    + selectionStgy.name() + " is not supported yet.");
        }
    }

    /**
     * Form the parent population via wheel-roulette algorithm.
     * For more information, please have a look http://www.edc.ncl.ac.uk/highlight/rhjanuary2007g02.php/.
     */
    private Population selectParentsByRouletteWheel() {
        double totalFitness = population.getTotalFitness();
        double[] sectors = new double[population.size()];

        for (int i = 0; i < population.size(); i++)
            sectors[i] = population.getChromosome(i).getFitness() / totalFitness;

        Population parentPopulation = new Population(population.size());

        for (int i = 0; i < parentPopulation.size(); i++) {
            double rouletteVal = rnd.nextDouble();
            double accumulatedSectorLen = 0.0;
            int selectedChromosomeIdx = Integer.MIN_VALUE;
            int sectorIdx = 0;

            while (selectedChromosomeIdx == Integer.MIN_VALUE && sectorIdx < sectors.length) {
                accumulatedSectorLen += sectors[sectorIdx];
                if (rouletteVal < accumulatedSectorLen)
                    selectedChromosomeIdx = sectorIdx;
                sectorIdx++;
            }

            if (selectedChromosomeIdx == Integer.MIN_VALUE)
                selectedChromosomeIdx = rnd.nextInt(population.size()); // or get last

            parentPopulation.setChromosome(i, population.getChromosome(selectedChromosomeIdx));
        }

        return parentPopulation;
    }

    /**
     * Simple stop criteria condition based on max amount of generations.
     *
     * @param iterationNum Iteration number.
     */
    private boolean stopCriteriaIsReached(int iterationNum) {
        return iterationNum < amountOfGenerations;
    }

    /**
     * Selects and injects the best chromosomes to form the elite of new population.
     *
     * @param newPopulation New population.
     */
    private Population selectEliteChromosomes(Population newPopulation) {
        boolean elitism = amountOfEliteChromosomes > 0;

        if (elitism) {
            Chromosome[] elite = population.selectBestKChromosome(amountOfEliteChromosomes);
            for (int i = 0; i < elite.length; i++)
                newPopulation.setChromosome(i, elite[i]);
        }
        return newPopulation;
    }

    /**
     * Forms the new population based on parent population with crossingover operator.
     *
     * @param parents       Parents.
     * @param newPopulation New population.
     */
    private Population crossingover(Population parents, Population newPopulation) {
        // because parent population is less than new population on amount of elite chromosome
        for (int j = 0; j < populationSize - amountOfEliteChromosomes; j += 2) {
            Chromosome ch1 = parents.getChromosome(j);
            Chromosome ch2 = parents.getChromosome(j + 1);

            if (rnd.nextDouble() < crossingoverProbability) {
                List<Chromosome> twoChildren = crossover(ch1, ch2);
                newPopulation.setChromosome(amountOfEliteChromosomes + j, twoChildren.get(0));
                newPopulation.setChromosome(amountOfEliteChromosomes + j + 1, twoChildren.get(1));
            } else {
                newPopulation.setChromosome(amountOfEliteChromosomes + j, ch1);
                newPopulation.setChromosome(amountOfEliteChromosomes + j + 1, ch2);
            }
        }
        return newPopulation;
    }

    /**
     * Applies mutation operator to each chromosome in population with mutation probability.
     *
     * @param newPopulation New population.
     */
    private Population mutate(Population newPopulation) {
        for (int j = amountOfEliteChromosomes; j < populationSize; j++) {
            Chromosome possibleMutant = newPopulation.getChromosome(j);
            for (int geneIdx = 0; geneIdx < possibleMutant.size(); geneIdx++) {
                if (rnd.nextDouble() < mutationProbability) {
                    Double gene = possibleMutant.getGene(geneIdx);
                    Double newGeneVal = mutationOperator.apply(geneIdx, gene);
                    possibleMutant.setGene(geneIdx, newGeneVal);
                }
            }
        }
        return newPopulation;
    }

    /**
     * Produce the two child chromsome from two parent chromosome according crossover strategy.
     *
     * @param firstParent  First parent.
     * @param secondParent Second parent.
     */
    private List<Chromosome> crossover(Chromosome firstParent, Chromosome secondParent) {
        if (firstParent.size() != secondParent.size())
            throw new RuntimeException("Different length of hyper-parameter vectors!");
        switch (crossoverStgy) {
            case UNIFORM:
                return uniformStrategy(firstParent, secondParent);
            case ONE_POINT:
                return onePointStrategy(firstParent, secondParent);
            default:
                throw new UnsupportedOperationException("This strategy "
                    + crossoverStgy.name() + " is not supported yet.");

        }

    }

    /**
     * Produce the two child chromsome from two parent chromosome according one-point strategy.
     *
     * @param firstParent  First parent.
     * @param secondParent Second parent.
     */
    private List<Chromosome> onePointStrategy(Chromosome firstParent, Chromosome secondParent) {
        int size = firstParent.size();

        Chromosome child1 = new Chromosome(size);
        Chromosome child2 = new Chromosome(size);
        int locusPnt = rnd.nextInt(size);

        for (int i = 0; i < locusPnt; i++) {
            child1.setGene(i, firstParent.getGene(i));
            child2.setGene(i, secondParent.getGene(i));
        }
        
        for (int i = locusPnt; i < size; i++) {
            child1.setGene(i, secondParent.getGene(i));
            child2.setGene(i, firstParent.getGene(i));
        }

        List<Chromosome> res = new ArrayList<>();
        res.add(child1);
        res.add(child2);

        return res;
    }

    /**
     * Produce the two child chromsome from two parent chromosome according uniform strategy.
     *
     * @param firstParent  First parent.
     * @param secondParent Second parent.
     */
    private List<Chromosome> uniformStrategy(Chromosome firstParent, Chromosome secondParent) {
        int size = firstParent.size();

        Chromosome child1 = new Chromosome(size);
        Chromosome child2 = new Chromosome(size);

        for (int i = 0; i < firstParent.size(); i++) {
            if (rnd.nextDouble() < UNIFORM_RATE) {
                child1.setGene(i, firstParent.getGene(i));
                child2.setGene(i, secondParent.getGene(i));
            } else {
                child1.setGene(i, secondParent.getGene(i));
                child2.setGene(i, firstParent.getGene(i));
            }
        }

        List<Chromosome> res = new ArrayList<>();
        res.add(child1);
        res.add(child2);

        return res;
    }

    /**
     * Sets the custom fitness function.
     *
     * @param fitnessFunction Fitness function.
     */
    public GeneticAlgorithm withFitnessFunction(Function<Chromosome, Double> fitnessFunction) {
        this.fitnessFunction = fitnessFunction;
        return this;
    }

    /**
     * Returns the best chromosome's genes presented as double array.
     */
    public double[] getTheBestSolution() {
        Double[] boxed = population.selectBestKChromosome(1)[0].toDoubleArray();
        return Stream.of(boxed).mapToDouble(Double::doubleValue).toArray();
    }

    /**
     * @param amountOfEliteChromosomes Amount of elite chromosomes.
     */
    public GeneticAlgorithm withAmountOfEliteChromosomes(int amountOfEliteChromosomes) {
        this.amountOfEliteChromosomes = amountOfEliteChromosomes;
        return this;
    }

    /**
     * @param amountOfGenerations Amount of generations.
     */
    public GeneticAlgorithm withAmountOfGenerations(int amountOfGenerations) {
        this.amountOfGenerations = amountOfGenerations;
        return this;
    }

    /**
     * @param mutationOperator Mutation operator.
     */
    public GeneticAlgorithm withMutationOperator(
        BiFunction<Integer, Double, Double> mutationOperator) {
        this.mutationOperator = mutationOperator;
        return this;
    }

    /**
     * @param populationSize Population size.
     */
    public GeneticAlgorithm withPopulationSize(int populationSize) {
        this.populationSize = populationSize;
        return this;
    }

    /**
     * @param crossingoverProbability Crossingover probability.
     */
    public GeneticAlgorithm withCrossingoverProbability(double crossingoverProbability) {
        this.crossingoverProbability = crossingoverProbability;
        return this;
    }

    /**
     * @param mutationProbability Mutation probability.
     */
    public GeneticAlgorithm withMutationProbability(double mutationProbability) {
        this.mutationProbability = mutationProbability;
        return this;
    }

    /**
     * @param crossoverStgy Crossover strategy.
     */
    public GeneticAlgorithm withCrossoverStgy(CrossoverStrategy crossoverStgy) {
        this.crossoverStgy = crossoverStgy;
        return this;
    }

    /**
     * @param selectionStgy Selection strategy.
     */
    public GeneticAlgorithm withSelectionStgy(SelectionStrategy selectionStgy) {
        this.selectionStgy = selectionStgy;
        return this;
    }
}

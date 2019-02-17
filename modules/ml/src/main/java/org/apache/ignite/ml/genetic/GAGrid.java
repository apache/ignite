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

package org.apache.ignite.ml.genetic;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;

import org.apache.ignite.ml.genetic.cache.GeneCacheConfig;
import org.apache.ignite.ml.genetic.cache.PopulationCacheConfig;
import org.apache.ignite.ml.genetic.parameter.GAConfiguration;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;

/**
 * Central class responsible for orchestrating distributive Genetic Algorithm.
 *
 * This class accepts a GAConfigriation and Ignite instance.
 */
public class GAGrid {
    /** Ignite logger */
    IgniteLogger igniteLogger = null;
    /** GAConfiguraton */
    private GAConfiguration config = null;
    /** Ignite instance */
    private Ignite ignite = null;
    /** Population cache */
    private IgniteCache<Long, Chromosome> populationCache = null;
    /** Gene cache */
    private IgniteCache<Long, Gene> geneCache = null;
    /** population keys */
    private List<Long> populationKeys = new ArrayList<Long>();

    /**
     * @param config GAConfiguration
     * @param ignite Ignite
     */
    public GAGrid(GAConfiguration config, Ignite ignite) {
        this.ignite = ignite;
        this.config = config;
        this.ignite = ignite;
        this.igniteLogger = ignite.log();

        // Get/Create cache
        populationCache = this.ignite.getOrCreateCache(PopulationCacheConfig.populationCache());
        populationCache.clear();

        // Get/Create cache
        geneCache = this.ignite.getOrCreateCache(GeneCacheConfig.geneCache());
        geneCache.clear();
    }

    /**
     * Calculate average fitness value
     *
     * @return Average fitness score
     */
    private Double calculateAverageFitness() {

        double avgFitnessScore = 0;

        IgniteCache<Long, Gene> cache = ignite.cache(GAGridConstants.POPULATION_CACHE);

        // Execute query to get names of all employees.
        SqlFieldsQuery sql = new SqlFieldsQuery("select AVG(FITNESSSCORE) from Chromosome");

        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = cache.query(sql)) {
            for (List<?> row : cursor)
                avgFitnessScore = (Double)row.get(0);
        }

        return avgFitnessScore;
    }

    /**
     * Calculate fitness each Chromosome in population
     *
     * @param chromosomeKeys List of chromosome primary keys
     */
    private void calculateFitness(List<Long> chromosomeKeys) {
       this.ignite.compute().execute(new FitnessTask(this.config), chromosomeKeys);
    }

    /**
     * @param fittestKeys List of chromosome keys that will be copied from
     * @param selectedKeys List of chromosome keys that will be overwritten evenly by fittestKeys
     * @return Boolean value
     */
    private Boolean copyFitterChromosomesToPopulation(List<Long> fittestKeys, List<Long> selectedKeys) {
        double truncatePercentage = this.config.getTruncateRate();

        int totalSize = this.populationKeys.size();

        int truncateCount = (int)(truncatePercentage * totalSize);

        int numberOfCopies = selectedKeys.size() / truncateCount;

        Boolean boolValue = this.ignite.compute()
            .execute(new TruncateSelectionTask(fittestKeys, numberOfCopies), selectedKeys);

        return boolValue;

    }

    /**
     * create a Chromsome
     *
     * @param numberOfGenes Number of Genes in resepective Chromosome
     * @return Chromosome
     */
    private Chromosome createChromosome(int numberOfGenes) {
        long[] genes = new long[numberOfGenes];
        List<Long> keys = new ArrayList<Long>();
        int k = 0;
        while (k < numberOfGenes) {
            long key = selectGene(k);

            if (!(keys.contains(key))) {
                genes[k] = key;
                keys.add(key);
                k = k + 1;
            }
        }
        Chromosome aChromsome = new Chromosome(genes);
        return aChromsome;
    }

    /**
     * Perform crossover
     *
     * @param leastFitKeys List of primary keys for Chromsomes that are considered 'least fit'
     */
    private void crossover(List<Long> leastFitKeys) {
        this.ignite.compute().execute(new CrossOverTask(this.config), leastFitKeys);
    }

    /**
     * Evolve the population
     *
     * @return Fittest Chromosome
     */
    public Chromosome evolve() {
        // keep track of current generation
        int generationCount = 1;

        Chromosome fittestChomosome = null;

        initializeGenePopulation();

        intializePopulation();

        // Calculate Fitness
        calculateFitness(this.populationKeys);

        // Retrieve chromosomes in order by fitness value
        List<Long> keys = getChromosomesByFittest();

        // Calculate average fitness value of population
        double averageFitnessScore = calculateAverageFitness();

        fittestChomosome = populationCache.get(keys.get(0));

        // while NOT terminateCondition met
        while (!(config.getTerminateCriteria().isTerminationConditionMet(fittestChomosome, averageFitnessScore,
            generationCount))) {
            generationCount = generationCount + 1;

            // We will crossover/mutate over chromosomes based on selection method

            List<Long> selectedKeysforCrossMutaton = selection(keys);

            // Cross Over
            crossover(selectedKeysforCrossMutaton);

            // Mutate
            mutation(selectedKeysforCrossMutaton);

            // Calculate Fitness
            calculateFitness(selectedKeysforCrossMutaton);

            // Retrieve chromosomes in order by fitness value
            keys = getChromosomesByFittest();

            // Retreive the first chromosome from the list
            fittestChomosome = populationCache.get(keys.get(0));

            // Calculate average fitness value of population
            averageFitnessScore = calculateAverageFitness();

            // End Loop

        }
        return fittestChomosome;
    }

    /**
     * helper routine to retrieve Chromosome keys in order of fittest
     *
     * @return List of primary keys for chromosomes.
     */
    private List<Long> getChromosomesByFittest() {
        List<Long> orderChromKeysByFittest = new ArrayList<Long>();
        String orderDirection = "desc";

        if (!config.isHigherFitnessValueFitter())
            orderDirection = "asc";

        String fittestSQL = "select _key from Chromosome order by fitnessScore " + orderDirection;

        // Execute query to retrieve keys for ALL Chromosomes by fittnessScore
        QueryCursor<List<?>> cursor = populationCache.query(new SqlFieldsQuery(fittestSQL));

        List<List<?>> res = cursor.getAll();

        for (List row : res) {
            Long key = (Long)row.get(0);
            orderChromKeysByFittest.add(key);
        }

        return orderChromKeysByFittest;
    }

    /**
     * @param keys List of primary keys for respective Chromosomes
     * @return List of keys for respective Chromosomes
     */
    private List<Long> getFittestKeysForTruncation(List<Long> keys) {
        double truncatePercentage = this.config.getTruncateRate();

        int truncateCount = (int)(truncatePercentage * keys.size());

        List<Long> selectedKeysToRetain = keys.subList(0, truncateCount);

        return selectedKeysToRetain;
    }

    /**
     * initialize the Gene pool
     */
    void initializeGenePopulation() {
        geneCache.clear();

        List<Gene> genePool = config.getGenePool();

        for (Gene gene : genePool) {
            geneCache.put(gene.id(), gene);
        }
    }

    /**
     * Initialize the population of Chromosomes
     */
    void initializePopulation() {
        int populationSize = config.getPopulationSize();
        populationCache.clear();

        for (int j = 0; j < populationSize; j++) {
            Chromosome chromosome = createChromosome(config.getChromosomeLength());
            populationCache.put(chromosome.id(), chromosome);
            populationKeys.add(chromosome.id());
        }

    }

    /**
     * initialize the population of Chromosomes based on GAConfiguration
     */
    void intializePopulation() {
        int populationSize = config.getPopulationSize();
        populationCache.clear();

        for (int j = 0; j < populationSize; j++) {
            Chromosome chromosome = createChromosome(config.getChromosomeLength());
            populationCache.put(chromosome.id(), chromosome);
            populationKeys.add(chromosome.id());
        }

    }

    /**
     * Perform mutation
     *
     * @param leastFitKeys List of primary keys for Chromosomes that are considered 'least fit'.
     */
    private void mutation(List<Long> leastFitKeys) {
         this.ignite.compute().execute(new MutateTask(this.config), leastFitKeys);
    }

    /**
     * select a gene from the Gene pool
     *
     * @return Primary key of respective Gene
     */
    private long selectAnyGene() {
        int idx = selectRandomIndex(config.getGenePool().size());
        Gene gene = config.getGenePool().get(idx);
        return gene.id();
    }

    /**
     * For our implementation we consider 'best fit' chromosomes, by selecting least fit chromosomes for crossover and
     * mutation
     *
     * As result, we are interested in least fit chromosomes.
     *
     * @param keys List of primary keys for respective Chromsomes
     * @return List of primary Keys for respective Chromosomes that are considered least fit
     */
    private List<Long> selectByElitism(List<Long> keys) {
        int elitismCount = this.config.getElitismCount();
        List<Long> leastFitKeys = keys.subList(elitismCount, keys.size());
        return leastFitKeys;
    }

    /**
     * Truncation selection simply retains the fittest x% of the population. These fittest individuals are duplicated so
     * that the population size is maintained.
     *
     * @param keys
     * @return List of keys
     */
    private List<Long> selectByTruncation(List<Long> keys) {
        double truncatePercentage = this.config.getTruncateRate();

        int truncateCount = (int)(truncatePercentage * keys.size());

        List<Long> selectedKeysForCrossOver = keys.subList(truncateCount, keys.size());

        return selectedKeysForCrossOver;
    }

    /**
     * @param k Gene index in Chromosome.
     * @return Primary key of respective Gene chosen
     */
    private long selectGene(int k) {
        if (config.getChromosomeCriteria() == null)
            return (selectAnyGene());
        else 
            return (selectGeneByChromsomeCriteria(k));
    }

    /**
     * method assumes ChromosomeCriteria is set.
     *
     * @param k Gene index in Chromosome
     * @return Primary key of respective Gene
     */
    private long selectGeneByChromsomeCriteria(int k) {
        List<Gene> genes = new ArrayList();

        StringBuffer sbSqlClause = new StringBuffer("_val like '");
        sbSqlClause.append("%");
        sbSqlClause.append(config.getChromosomeCriteria().getCriteria().get(k));
        sbSqlClause.append("%'");

        IgniteCache<Long, Gene> cache = ignite.cache(GAGridConstants.GENE_CACHE);

        SqlQuery sql = new SqlQuery(Gene.class, sbSqlClause.toString());

        try (QueryCursor<Entry<Long, Gene>> cursor = cache.query(sql)) {
            for (Entry<Long, Gene> e : cursor)
                genes.add(e.getValue());
        }

        int idx = selectRandomIndex(genes.size());

        Gene gene = genes.get(idx);
        return gene.id();
    }

    /**
     * @param sizeOfGenePool Size of Gene pool
     * @return Index
     */
    private int selectRandomIndex(int sizeOfGenePool) {
        Random randomGenerator = new Random();
        int index = randomGenerator.nextInt(sizeOfGenePool);
        return index;
    }

    /**
     * Select chromosomes
     *
     * @param chromosomeKeys List of population primary keys for respective Chromsomes
     * @return List of primary keys for respective Chromsomes
     */
    private List<Long> selection(List<Long> chromosomeKeys) {
        List<Long> selectedKeys = new ArrayList();

        GAGridConstants.SELECTION_METHOD selectionMethod = config.getSelectionMethod();

        switch (selectionMethod) {
            case SELECTON_METHOD_ELETISM:
                selectedKeys = selectByElitism(chromosomeKeys);
                break;
            case SELECTION_METHOD_TRUNCATION:
                selectedKeys = selectByTruncation(chromosomeKeys);

                List<Long> fittestKeys = getFittestKeysForTruncation(chromosomeKeys);

                copyFitterChromosomesToPopulation(fittestKeys, selectedKeys);

                // copy more fit keys to rest of population
                break;

            default:
                break;
        }

        return selectedKeys;
    }

    /**
     * Get primary keys for Chromosomes
     *
     * @return List of Chromosome primary keys
     */
    List<Long> getPopulationKeys() {
        return populationKeys;
    }
}

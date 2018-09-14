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
    private IgniteLogger igniteLog;
    /** GAConfiguraton */
    private GAConfiguration cfg;
    /** Ignite instance */
    private Ignite ignite;
    /** Population cache */
    private IgniteCache<Long, Chromosome> populationCache;
    /** Gene cache */
    private IgniteCache<Long, Gene> geneCache;
    /** population keys */
    private List<Long> populationKeys = new ArrayList<Long>();

    /**
     * @param cfg GAConfiguration
     * @param ignite Ignite
     */
    public GAGrid(GAConfiguration cfg, Ignite ignite) {
        this.ignite = ignite;
        this.cfg = cfg;
        this.ignite = ignite;
        this.igniteLog = ignite.log();

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
       this.ignite.compute().execute(new FitnessTask(this.cfg), chromosomeKeys);
    }

    /**
     * @param fittestKeys List of chromosome keys that will be copied from
     * @param selectedKeys List of chromosome keys that will be overwritten evenly by fittestKeys
     * @return Boolean value
     */
    private Boolean copyFitterChromosomesToPopulation(List<Long> fittestKeys, List<Long> selectedKeys) {
        double truncatePercentage = this.cfg.getTruncateRate();

        int totalSize = this.populationKeys.size();

        int truncateCnt = (int)(truncatePercentage * totalSize);

        int numOfCopies = selectedKeys.size() / truncateCnt;

        return this.ignite.compute()
            .execute(new TruncateSelectionTask(fittestKeys, numOfCopies), selectedKeys);

    }

    /**
     * create a Chromsome
     *
     * @param numOfGenes Number of Genes in resepective Chromosome
     * @return Chromosome
     */
    private Chromosome createChromosome(int numOfGenes) {
        long[] genes = new long[numOfGenes];
        List<Long> keys = new ArrayList<Long>();
        int k = 0;
        while (k < numOfGenes) {
            long key = selectGene(k);

            if (!(keys.contains(key))) {
                genes[k] = key;
                keys.add(key);
                k = k + 1;
            }
        }
        return new Chromosome(genes);
    }

    /**
     * Perform crossover
     *
     * @param leastFitKeys List of primary keys for Chromsomes that are considered 'least fit'
     */
    private void crossover(List<Long> leastFitKeys) {
        this.ignite.compute().execute(new CrossOverTask(this.cfg), leastFitKeys);
    }

    /**
     * Evolve the population
     *
     * @return Fittest Chromosome
     */
    public Chromosome evolve() {
        // keep track of current generation
        int generationCnt = 1;

        Chromosome fittestChomosome;

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
        while (!(cfg.getTerminateCriteria().isTerminationConditionMet(fittestChomosome, averageFitnessScore,
            generationCnt))) {
            generationCnt = generationCnt + 1;

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

        if (!cfg.isHigherFitnessValFitter())
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
        double truncatePercentage = this.cfg.getTruncateRate();

        int truncateCnt = (int)(truncatePercentage * keys.size());

        return keys.subList(0, truncateCnt);
    }

    /**
     * initialize the Gene pool
     */
    void initializeGenePopulation() {
        geneCache.clear();

        List<Gene> genePool = cfg.getGenePool();

        for (Gene gene : genePool)
            geneCache.put(gene.id(), gene);
    }

    /**
     * Initialize the population of Chromosomes
     */
    void initializePopulation() {
        int populationSize = cfg.getPopulationSize();
        populationCache.clear();

        for (int j = 0; j < populationSize; j++) {
            Chromosome chromosome = createChromosome(cfg.getChromosomeLen());
            populationCache.put(chromosome.id(), chromosome);
            populationKeys.add(chromosome.id());
        }

    }

    /**
     * initialize the population of Chromosomes based on GAConfiguration
     */
    void intializePopulation() {
        int populationSize = cfg.getPopulationSize();
        populationCache.clear();

        for (int j = 0; j < populationSize; j++) {
            Chromosome chromosome = createChromosome(cfg.getChromosomeLen());
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
         this.ignite.compute().execute(new MutateTask(this.cfg), leastFitKeys);
    }

    /**
     * select a gene from the Gene pool
     *
     * @return Primary key of respective Gene
     */
    private long selectAnyGene() {
        int idx = selectRandomIndex(cfg.getGenePool().size());
        Gene gene = cfg.getGenePool().get(idx);
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
        int elitismCnt = this.cfg.getElitismCnt();
        return keys.subList(elitismCnt, keys.size());
    }

    /**
     * Truncation selection simply retains the fittest x% of the population. These fittest individuals are duplicated so
     * that the population size is maintained.
     *
     * @param keys
     * @return List of keys
     */
    private List<Long> selectByTruncation(List<Long> keys) {
        double truncatePercentage = this.cfg.getTruncateRate();

        int truncateCnt = (int)(truncatePercentage * keys.size());

        return keys.subList(truncateCnt, keys.size());
    }

    /**
     * @param k Gene index in Chromosome.
     * @return Primary key of respective Gene chosen
     */
    private long selectGene(int k) {
        if (cfg.getChromosomeCriteria() == null)
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
        sbSqlClause.append(cfg.getChromosomeCriteria().getCriteria().get(k));
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
        return randomGenerator.nextInt(sizeOfGenePool);
    }

    /**
     * Select chromosomes
     *
     * @param chromosomeKeys List of population primary keys for respective Chromsomes
     * @return List of primary keys for respective Chromsomes
     */
    private List<Long> selection(List<Long> chromosomeKeys) {
        List<Long> selectedKeys = new ArrayList();

        GAGridConstants.SELECTION_METHOD selectionMtd = cfg.getSelectionMtd();

        switch (selectionMtd) {
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

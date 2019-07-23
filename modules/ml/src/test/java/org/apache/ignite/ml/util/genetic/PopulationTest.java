
package org.apache.ignite.ml.util.genetic;

import java.util.function.Function;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link Population}.
 */
public class PopulationTest {
    /** Amount of the best chromosomes. */
    public static final int K = 10;

    /** Test population. */
    Population population;

    /** Precision. */
    private static final double PRECISION = 0.00000001;

    /** Fitness function. */
    Function<Chromosome, Double> fitnessFunction = (Chromosome ch) -> ch.getGene(0) + ch.getGene(1);

    /**
     *
     */
    @Before
    public void setUp() {
        population = new Population(100);
        Double[] chromosomeData = new Double[2];
        for (int i = 0; i < population.size(); i++) {
            chromosomeData[0] = (double) i;
            chromosomeData[1] = (double) i;
            population.setChromosome(i, new Chromosome(chromosomeData));
        }
    }

    /**
     *
     */
    @Test
    public void calculateFitnessForChromosome() {
        population.calculateFitnessForChromosome(0, fitnessFunction);
        Assert.assertEquals(population.getChromosome(0).getFitness(), 0, PRECISION);
    }

    /**
     *
     */
    @Test
    public void calculateFitnessForAll() {
        population.calculateFitnessForAll(fitnessFunction);
        Assert.assertEquals(population.getChromosome(0).getFitness(), 0, PRECISION);
    }

    /**
     *
     */
    @Test
    public void selectBestKChromosomeWithoutFitnessCalculation() {
        Assert.assertNull(population.selectBestKChromosome(K));
    }

    /**
     *
     */
    @Test
    public void selectBestKChromosomeWithPartiallyFitnessCalculation() {
        population.calculateFitnessForChromosome(0, fitnessFunction);
        population.calculateFitnessForChromosome(1, fitnessFunction);
        Assert.assertNull(population.selectBestKChromosome(K));
    }

    /**
     *
     */
    @Test
    public void selectBestKChromosome() {
        population.calculateFitnessForAll(fitnessFunction);
        Chromosome[] res = population.selectBestKChromosome(K);
        Assert.assertEquals(res[0].getFitness(), 180, PRECISION);
    }

    /**
     *
     */
    @Test
    public void getTotalFitness() {
        double res = population.getTotalFitness();
        Assert.assertEquals(res, Double.NaN, PRECISION);

        population.calculateFitnessForAll(fitnessFunction);
        res = population.getTotalFitness();
        Assert.assertEquals(res, 9900.0, PRECISION);
    }

    /**
     *
     */
    @Test
    public void getAverageFitness() {
        double res = population.getAverageFitness();
        Assert.assertEquals(res, Double.NaN, PRECISION);

        population.calculateFitnessForAll(fitnessFunction);
        res = population.getAverageFitness();
        Assert.assertEquals(res, 99.0, PRECISION);
    }
}
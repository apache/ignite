package org.apache.ignite.ml.util.genetic;

import org.jetbrains.annotations.NotNull;


/**
 * Representes the set of genes, known as chromosome in genetic programming.
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
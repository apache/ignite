

package org.apache.ignite.console.demo.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Country definition.
 */
public class Country implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private int id;

    /** Value for name. */
    private String name;

    /** Value for population. */
    private int population;

    /**
     * Empty constructor.
     */
    public Country() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public Country(
        int id,
        String name,
        int population
    ) {
        this.id = id;
        this.name = name;
        this.population = population;
    }

    /**
     * Gets id.
     *
     * @return Value for id.
     */
    public int getId() {
        return id;
    }

    /**
     * Sets id.
     *
     * @param id New value for id.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Gets name.
     *
     * @return Value for name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name.
     *
     * @param name New value for name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets population.
     *
     * @return Value for population.
     */
    public int getPopulation() {
        return population;
    }

    /**
     * Sets population.
     *
     * @param population New value for population.
     */
    public void setPopulation(int population) {
        this.population = population;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        
        if (!(o instanceof Country))
            return false;

        Country that = (Country)o;

        if (id != that.id)
            return false;

        if (!Objects.equals(name, that.name))
            return false;

        return population == that.population;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;

        res = 31 * res + (name != null ? name.hashCode() : 0);

        res = 31 * res + population;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Country [id=" + id +
            ", name=" + name +
            ", population=" + population +
            ']';
    }
}



package org.apache.ignite.console.demo.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Parking definition.
 */
public class Parking implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private int id;

    /** Value for name. */
    private String name;

    /** Value for capacity. */
    private int capacity;

    /**
     * Empty constructor.
     */
    public Parking() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public Parking(
        int id,
        String name,
        int capacity
    ) {
        this.id = id;
        this.name = name;
        this.capacity = capacity;
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
     * Gets capacity.
     *
     * @return Value for capacity.
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Sets capacity.
     *
     * @param capacity New value for capacity.
     */
    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        
        if (!(o instanceof Parking))
            return false;

        Parking that = (Parking)o;

        if (id != that.id)
            return false;

        if (!Objects.equals(name, that.name))
            return false;

        return capacity == that.capacity;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;

        res = 31 * res + (name != null ? name.hashCode() : 0);

        res = 31 * res + capacity;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Parking [id=" + id +
            ", name=" + name +
            ", capacity=" + capacity +
            ']';
    }
}



package org.apache.ignite.console.demo.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Department definition.
 */
public class Department implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private int id;

    /** Value for countryId. */
    private int countryId;

    /** Value for name. */
    private String name;

    /**
     * Empty constructor.
     */
    public Department() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public Department(
        int id,
        int countryId,
        String name
    ) {
        this.id = id;
        this.countryId = countryId;
        this.name = name;
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
     * Gets countryId.
     *
     * @return Value for countryId.
     */
    public int getCountryId() {
        return countryId;
    }

    /**
     * Sets countryId.
     *
     * @param countryId New value for countryId.
     */
    public void setCountryId(int countryId) {
        this.countryId = countryId;
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

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        
        if (!(o instanceof Department))
            return false;

        Department that = (Department)o;

        if (id != that.id)
            return false;

        if (countryId != that.countryId)
            return false;

        return Objects.equals(name, that.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;

        res = 31 * res + countryId;

        res = 31 * res + (name != null ? name.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Department [id=" + id +
            ", countryId=" + countryId +
            ", name=" + name +
            ']';
    }
}

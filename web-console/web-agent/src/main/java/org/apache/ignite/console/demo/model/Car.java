

package org.apache.ignite.console.demo.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Car definition.
 */
public class Car implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private int id;

    /** Value for parkingId. */
    private int parkingId;

    /** Value for name. */
    private String name;

    /**
     * Empty constructor.
     */
    public Car() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public Car(
        int id,
        int parkingId,
        String name
    ) {
        this.id = id;
        this.parkingId = parkingId;
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
     * Gets parkingId.
     *
     * @return Value for parkingId.
     */
    public int getParkingId() {
        return parkingId;
    }

    /**
     * Sets parkingId.
     *
     * @param parkingId New value for parkingId.
     */
    public void setParkingId(int parkingId) {
        this.parkingId = parkingId;
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
        
        if (!(o instanceof Car))
            return false;

        Car that = (Car)o;

        if (id != that.id)
            return false;

        if (parkingId != that.parkingId)
            return false;

        return Objects.equals(name, that.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;

        res = 31 * res + parkingId;

        res = 31 * res + (name != null ? name.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Car [id=" + id +
            ", parkingId=" + parkingId +
            ", name=" + name +
            ']';
    }
}

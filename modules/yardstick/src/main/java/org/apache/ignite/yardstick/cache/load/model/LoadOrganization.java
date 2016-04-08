package org.apache.ignite.yardstick.cache.load.model;

/**
 * Created by pyatkov-vd on 07.04.2016.
 */
public class LoadOrganization {

    /** Organization ID. */
    private int id;

    /** Organization name. */
    private String name;

    /**
     * Constructs empty organization.
     */
    public LoadOrganization() {
        // No-op.
    }

    public LoadOrganization(Integer id) {
        this.id = id;
        name = "Organization " + id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LoadOrganization that = (LoadOrganization)o;

        if (id != that.id)
            return false;
        return name != null ? name.equals(that.name) : that.name == null;

    }

    @Override public int hashCode() {
        int result = id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}

package org.apache.ignite.yardstick.cache.load.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Created by pyatkov-vd on 07.04.2016.
 */
public class LoadPersone {

    private int id;

    private String firstName;

    private String lastName;

    private double salary;

    /**
     * Constructs empty person.
     */
    public LoadPersone() {
        // No-op.
    }


    public LoadPersone(Integer id) {
        this.id = id;
        firstName = "Name " + id;
        lastName = "Last " + id;
        salary = id*8.57;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LoadPersone persone = (LoadPersone)o;

        if (id != persone.id)
            return false;
        if (Double.compare(persone.salary, salary) != 0)
            return false;
        if (firstName != null ? !firstName.equals(persone.firstName) : persone.firstName != null)
            return false;
        return lastName != null ? lastName.equals(persone.lastName) : persone.lastName == null;

    }

    @Override public int hashCode() {
        int result;
        long temp;
        result = id;
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        temp = Double.doubleToLongBits(salary);
        result = 31 * result + (int)(temp ^ (temp >>> 32));
        return result;
    }
}

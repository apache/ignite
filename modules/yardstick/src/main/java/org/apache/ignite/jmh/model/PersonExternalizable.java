/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.jmh.model;

import org.apache.ignite.cache.query.annotations.*;

import java.io.*;

/**
 * Person record used for query test.
 */
public class PersonExternalizable implements Externalizable {
    /** Person ID. */
    @QuerySqlField(index = true)
    private int id;

    /** Organization ID. */
    @QuerySqlField(index = true)
    private int orgId;

    /** First name (not-indexed). */
    @QuerySqlField
    private String firstName;

    /** Last name (not indexed). */
    @QuerySqlField
    private String lastName;

    /** Salary. */
    @QuerySqlField(index = true)
    private double salary;

    /**
     * Constructs empty person.
     */
    public PersonExternalizable() {
        // No-op.
    }

    /**
     * Constructs person record that is not linked to any organization.
     *
     * @param id Person ID.
     * @param firstName First name.
     * @param lastName Last name.
     * @param salary Salary.
     */
    public PersonExternalizable(int id, String firstName, String lastName, double salary) {
        this(id, 0, firstName, lastName, salary);
    }

    /**
     * Constructs person record.
     *
     * @param id Person ID.
     * @param orgId Organization ID.
     * @param firstName First name.
     * @param lastName Last name.
     * @param salary Salary.
     */
    public PersonExternalizable(int id, int orgId, String firstName, String lastName, double salary) {
        this.id = id;
        this.orgId = orgId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
    }

    /**
     * @return Person id.
     */
    public int getId() {
        return id;
    }

    /**
     * @param id Person id.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return Organization id.
     */
    public int getOrganizationId() {
        return orgId;
    }

    /**
     * @param orgId Organization id.
     */
    public void setOrganizationId(int orgId) {
        this.orgId = orgId;
    }

    /**
     * @return Person first name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName Person first name.
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Person last name.
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * @param lastName Person last name.
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * @return Salary.
     */
    public double getSalary() {
        return salary;
    }

    /**
     * @param salary Salary.
     */
    public void setSalary(double salary) {
        this.salary = salary;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(orgId);
        out.writeUTF(firstName);
        out.writeUTF(lastName);
        out.writeDouble(salary);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();
        orgId = in.readInt();
        firstName = in.readUTF();
        lastName = in.readUTF();
        salary = in.readDouble();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || (o instanceof PersonExternalizable) && id == ((PersonExternalizable)o).id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person [firstName=" + firstName +
            ", id=" + id +
            ", orgId=" + orgId +
            ", lastName=" + lastName +
            ", salary=" + salary +
            ']';
    }
}

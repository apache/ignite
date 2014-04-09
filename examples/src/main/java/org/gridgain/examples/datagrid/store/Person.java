/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store;

import java.io.*;

/**
 * Person class.
 */
public class Person implements Serializable {
    /** Person ID. */
    private long id;

    /** First name. */
    private String firstName;

    /** Last name. */
    private String lastName;

    /**
     *
     */
    public Person() {
        // No-op.
    }

    /**
     * Constructs person record.
     *
     * @param id Person ID.
     * @param firstName First name.
     * @param lastName Last name.
     */
    public Person(long id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * @return Person ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @param id Person ID.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * @return First name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName First name.
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Last name.
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * @param lastName Last name.
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person [id=" + id +
            ", firstName=" + firstName +
            ", lastName=" + lastName + ']';
    }
}

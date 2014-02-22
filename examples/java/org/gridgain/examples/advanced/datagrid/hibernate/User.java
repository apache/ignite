// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.hibernate;

import org.hibernate.annotations.*;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.*;
import java.util.*;

/**
 * A user entity class. Represents a user of some public service,
 * having a number of personal information fields as well as a
 * number of posts written.
 *
 * @author @java.author
 * @version @java.version
 */
@Entity
class User {
    /** ID. */
    @Id
    @GeneratedValue(strategy=GenerationType.AUTO)
    private long id;

    /** Login. */
    @NaturalId
    private String login;

    /** First name. */
    private String firstName;

    /** Last name. */
    private String lastName;

    /** Posts. */
    @OneToMany(mappedBy = "author", cascade = CascadeType.ALL)
    private Set<Post> posts = new HashSet<>();

    /**
     * Default constructor (required by Hibernate).
     */
    User() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param login Login.
     * @param firstName First name.
     * @param lastName Last name.
     */
    User(String login, String firstName, String lastName) {
        this.login = login;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * @return ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @param id New ID.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * @return Login.
     */
    public String getLogin() {
        return login;
    }

    /**
     * @param login New login.
     */
    public void setLogin(String login) {
        this.login = login;
    }

    /**
     * @return First name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName New first name.
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
     * @param lastName New last name.
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * @return Posts.
     */
    public Set<Post> getPosts() {
        return posts;
    }

    /**
     * @param posts New posts.
     */
    public void setPosts(Set<Post> posts) {
        this.posts = posts;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "User [id=" + id +
            ", login=" + login +
            ", firstName=" + firstName +
            ", lastName=" + lastName +
            ']';
    }
}

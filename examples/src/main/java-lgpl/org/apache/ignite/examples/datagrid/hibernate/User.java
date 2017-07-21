/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.datagrid.hibernate;

import java.util.HashSet;
import java.util.Set;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import org.hibernate.annotations.NaturalId;

/**
 * A user entity class. Represents a user of some public service,
 * having a number of personal information fields as well as a
 * number of posts written.
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

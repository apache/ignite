/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

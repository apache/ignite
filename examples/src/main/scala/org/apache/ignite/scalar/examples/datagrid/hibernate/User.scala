/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.datagrid.hibernate

import org.hibernate.annotations.NaturalId

import javax.persistence._
import java.util.{HashSet => JavaHashSet, Set => JavaSet}

/**
 * A user entity class. Represents a user of some public service,
 * having a number of personal information fields as well as a
 * number of posts written.
 */
@Entity private[hibernate] class User {
    /** ID. */
    @Id @GeneratedValue(strategy = GenerationType.AUTO) private var id = 0L

    /** Login. */
    @NaturalId private var login: String = null

    /** First name. */
    private var firstName: String = null

    /** Last name. */
    private var lastName: String = null

    /** Posts. */
    @OneToMany(mappedBy = "author", cascade = Array(CascadeType.ALL))
    private var posts: JavaSet[Post] = new JavaHashSet[Post]

    /**
     * Constructor.
     *
     * @param login Login.
     * @param firstName First name.
     * @param lastName Last name.
     */
    private[hibernate] def this(login: String, firstName: String, lastName: String) {
        this()
        this.login = login
        this.firstName = firstName
        this.lastName = lastName
    }

    /**
     * @return ID.
     */
    def getId: Long = {
        id
    }

    /**
     * @param id New ID.
     */
    def setId(id: Long) {
        this.id = id
    }

    /**
     * @return Login.
     */
    def getLogin: String = {
        login
    }

    /**
     * @param login New login.
     */
    def setLogin(login: String) {
        this.login = login
    }

    /**
     * @return First name.
     */
    def getFirstName: String = {
        firstName
    }

    /**
     * @param firstName New first name.
     */
    def setFirstName(firstName: String) {
        this.firstName = firstName
    }

    /**
     * @return Last name.
     */
    def getLastName: String = {
        lastName
    }

    /**
     * @param lastName New last name.
     */
    def setLastName(lastName: String) {
        this.lastName = lastName
    }

    /**
     * @return Posts.
     */
    def getPosts: JavaSet[Post] = {
        posts
    }

    /**
     * @param posts New posts.
     */
    def setPosts(posts: JavaSet[Post]) {
        this.posts = posts
    }

    override def toString: String = {
        "User [id=" + id + ", login=" + login + ", firstName=" + firstName + ", lastName=" + lastName + ']'
    }
}

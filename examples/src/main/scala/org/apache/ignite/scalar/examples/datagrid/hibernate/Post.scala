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

import javax.persistence._
import java.util.Date

/**
 * An entity class representing a post, that a
 * [[User]] has made on some public service.
 */
@Entity class Post {
    /** ID. */
    @Id @GeneratedValue(strategy = GenerationType.AUTO) private var id = 0L

    /** Author. */
    @ManyToOne private var author: User = null

    /** Text. */
    private var text: String = null

    /** Created timestamp. */
    private var created: Date = null

    /**
     * Constructor.
     *
     * @param author Author.
     * @param text Text.
     */
    private[hibernate] def this(author: User, text: String) {
        this()

        this.author = author
        this.text = text
        created = new Date
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
     * @return Author.
     */
    def getAuthor: User = {
        author
    }

    /**
     * @param author New author.
     */
    def setAuthor(author: User) {
        this.author = author
    }

    /**
     * @return Text.
     */
    def getText: String = {
        text
    }

    /**
     * @param text New text.
     */
    def setText(text: String) {
        this.text = text
    }

    /**
     * @return Created timestamp.
     */
    def getCreated: Date = {
        created.clone.asInstanceOf[Date]
    }

    /**
     * @param created New created timestamp.
     */
    def setCreated(created: Date) {
        this.created = created.clone.asInstanceOf[Date]
    }

    override def toString: String = {
        "Post [id=" + id + ", text=" + text + ", created=" + created + ']'
    }
}

// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.hibernate;

import javax.persistence.*;
import java.util.*;

/**
 * An entity class representing a post, that a
 * {@link User} has made on some public service.
 *
 * @author @java.author
 * @version @java.version
 */
@Entity
class Post {
    /** ID. */
    @Id
    @GeneratedValue(strategy=GenerationType.AUTO)
    private long id;

    /** Author. */
    @ManyToOne
    private User author;

    /** Text. */
    private String text;

    /** Created timestamp. */
    private Date created;

    /**
     * Default constructor (required by Hibernate).
     */
    Post() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param author Author.
     * @param text Text.
     */
    Post(User author, String text) {
        this.author = author;
        this.text = text;
        created = new Date();
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
     * @return Author.
     */
    public User getAuthor() {
        return author;
    }

    /**
     * @param author New author.
     */
    public void setAuthor(User author) {
        this.author = author;
    }

    /**
     * @return Text.
     */
    public String getText() {
        return text;
    }

    /**
     * @param text New text.
     */
    public void setText(String text) {
        this.text = text;
    }

    /**
     * @return Created timestamp.
     */
    public Date getCreated() {
        return (Date)created.clone();
    }

    /**
     * @param created New created timestamp.
     */
    public void setCreated(Date created) {
        this.created = (Date)created.clone();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Post [id=" + id +
            ", text=" + text +
            ", created=" + created +
            ']';
    }
}

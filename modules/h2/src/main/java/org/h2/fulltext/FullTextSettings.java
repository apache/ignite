/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.fulltext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.h2.util.SoftHashMap;

/**
 * The global settings of a full text search.
 */
final class FullTextSettings {

    /**
     * The settings of open indexes.
     */
    private static final Map<String, FullTextSettings> SETTINGS = new HashMap<>();

    /**
     * Whether this instance has been initialized.
     */
    private boolean initialized;

    /**
     * The set of words not to index (stop words).
     */
    private final Set<String> ignoreList = new HashSet<>();

    /**
     * The set of words / terms.
     */
    private final Map<String, Integer> words = new HashMap<>();

    /**
     * The set of indexes in this database.
     */
    private final Map<Integer, IndexInfo> indexes = Collections
            .synchronizedMap(new HashMap<Integer, IndexInfo>());

    /**
     * The prepared statement cache.
     */
    private final SoftHashMap<Connection,
            SoftHashMap<String, PreparedStatement>> cache =
            new SoftHashMap<>();

    /**
     * The whitespace characters.
     */
    private String whitespaceChars = " \t\n\r\f+\"*%&/()=?'!,.;:-_#@|^~`{}[]<>\\";

    /**
     * Create a new instance.
     */
    private FullTextSettings() {
        // don't allow construction
    }

    /**
     * Clear set of ignored words
     */
    public void clearIgnored() {
        synchronized (ignoreList) {
            ignoreList.clear();
        }
    }

    /**
     * Amend set of ignored words
     * @param words to add
     */
    public void addIgnored(Iterable<String> words) {
        synchronized (ignoreList) {
            for (String word : words) {
                word = normalizeWord(word);
                ignoreList.add(word);
            }
        }
    }

    /**
     * Clear set of searchable words
     */
    public void clearWordList() {
        synchronized (words) {
            words.clear();
        }
    }

    /**
     * Get id for a searchable word
     * @param word to find id for
     * @return Integer id or null if word is not found
     */
    public Integer getWordId(String word) {
        synchronized (words) {
            return words.get(word);
        }
    }

    /**
     * Register searchable word
     * @param word to register
     * @param id to register with
     */
    public void addWord(String word, Integer id) {
        synchronized (words) {
            if(!words.containsKey(word)) {
                words.put(word, id);
            }
        }
    }

    /**
     * Get the index information for the given index id.
     *
     * @param indexId the index id
     * @return the index info
     */
    protected IndexInfo getIndexInfo(int indexId) {
        return indexes.get(indexId);
    }

    /**
     * Add an index.
     *
     * @param index the index
     */
    protected void addIndexInfo(IndexInfo index) {
        indexes.put(index.id, index);
    }

    /**
     * Convert a word to uppercase. This method returns null if the word is in
     * the ignore list.
     *
     * @param word the word to convert and check
     * @return the uppercase version of the word or null
     */
    protected String convertWord(String word) {
        word = normalizeWord(word);
        synchronized (ignoreList) {
            if (ignoreList.contains(word)) {
                return null;
            }
        }
        return word;
    }

    /**
     * Get or create the fulltext settings for this database.
     *
     * @param conn the connection
     * @return the settings
     */
    protected static FullTextSettings getInstance(Connection conn)
            throws SQLException {
        String path = getIndexPath(conn);
        FullTextSettings setting;
        synchronized (SETTINGS) {
            setting = SETTINGS.get(path);
            if (setting == null) {
                setting = new FullTextSettings();
                SETTINGS.put(path, setting);
            }
        }
        return setting;
    }

    /**
     * Get the file system path.
     *
     * @param conn the connection
     * @return the file system path
     */
    private static String getIndexPath(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "CALL IFNULL(DATABASE_PATH(), 'MEM:' || DATABASE())");
        rs.next();
        String path = rs.getString(1);
        if ("MEM:UNNAMED".equals(path)) {
            throw FullText.throwException(
                    "Fulltext search for private (unnamed) " +
                    "in-memory databases is not supported.");
        }
        rs.close();
        return path;
    }

    /**
     * Prepare a statement. The statement is cached in a soft reference cache.
     *
     * @param conn the connection
     * @param sql the statement
     * @return the prepared statement
     */
    protected synchronized PreparedStatement prepare(Connection conn, String sql)
            throws SQLException {
        SoftHashMap<String, PreparedStatement> c = cache.get(conn);
        if (c == null) {
            c = new SoftHashMap<>();
            cache.put(conn, c);
        }
        PreparedStatement prep = c.get(sql);
        if (prep != null && prep.getConnection().isClosed()) {
            prep = null;
        }
        if (prep == null) {
            prep = conn.prepareStatement(sql);
            c.put(sql, prep);
        }
        return prep;
    }

    /**
     * Remove all indexes from the settings.
     */
    protected void removeAllIndexes() {
        indexes.clear();
    }

    /**
     * Remove an index from the settings.
     *
     * @param index the index to remove
     */
    protected void removeIndexInfo(IndexInfo index) {
        indexes.remove(index.id);
    }

    /**
     * Set the initialized flag.
     *
     * @param b the new value
     */
    protected void setInitialized(boolean b) {
        this.initialized = b;
    }

    /**
     * Get the initialized flag.
     *
     * @return whether this instance is initialized
     */
    protected boolean isInitialized() {
        return initialized;
    }

    /**
     * Close all fulltext settings, freeing up memory.
     */
    protected static void closeAll() {
        synchronized (SETTINGS) {
            SETTINGS.clear();
        }
    }

    protected void setWhitespaceChars(String whitespaceChars) {
        this.whitespaceChars = whitespaceChars;
    }

    protected String getWhitespaceChars() {
        return whitespaceChars;
    }

    private static String normalizeWord(String word) {
        // TODO this is locale specific, document
        return word.toUpperCase();
    }
}

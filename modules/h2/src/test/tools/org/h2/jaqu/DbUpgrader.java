/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

import org.h2.jaqu.Table.JQDatabase;

/**
 * Interface which defines a class to handle table changes based on model
 * versions. An implementation of <i>DbUpgrader</i> must be annotated with the
 * <i>JQDatabase</i> annotation, which defines the expected database version
 * number.
 */
public interface DbUpgrader {

    /**
     * Defines method interface to handle database upgrades. This method is only
     * called if your <i>DbUpgrader</i> implementation is annotated with
     * JQDatabase.
     *
     * @param db the database
     * @param fromVersion the old version
     * @param toVersion the new version
     * @return true for successful upgrade. If the upgrade is successful, the
     *         version registry is automatically updated.
     */
    boolean upgradeDatabase(Db db, int fromVersion, int toVersion);

    /**
     * Defines method interface to handle table upgrades.
     *
     * @param db the database
     * @param schema the schema
     * @param table the table
     * @param fromVersion the old version
     * @param toVersion the new version
     * @return true for successful upgrade. If the upgrade is successful, the
     *         version registry is automatically updated.
     */
    boolean upgradeTable(Db db, String schema, String table, int fromVersion,
            int toVersion);

    /**
     * The default database upgrader. It throws runtime exception instead of
     * handling upgrade requests.
     */
    @JQDatabase(version = 0)
    public static class DefaultDbUpgrader implements DbUpgrader {

        @Override
        public boolean upgradeDatabase(Db db, int fromVersion, int toVersion) {
            throw new RuntimeException(
                    "Please provide your own DbUpgrader implementation.");
        }

        @Override
        public boolean upgradeTable(Db db, String schema, String table,
                int fromVersion, int toVersion) {
            throw new RuntimeException(
                    "Please provide your own DbUpgrader implementation.");
        }

    }

}

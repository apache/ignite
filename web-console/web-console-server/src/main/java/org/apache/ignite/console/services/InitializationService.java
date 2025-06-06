

package org.apache.ignite.console.services;

import jakarta.annotation.PostConstruct;
import org.apache.ignite.console.migration.MigrationFromMongo;
import org.springframework.stereotype.Service;

/**
 * Special service for application initialization logic.
 */
@Service
public class InitializationService {
    /** */
    private MigrationFromMongo migration;

    /**
     * @param migration Service to migrate user data from MongoDB to Ignite persistence.
     */
    public InitializationService(MigrationFromMongo migration) {
        this.migration = migration;
    }

    /**
     * Initialize application.
     */
    @PostConstruct
    public void init() {
        migration.migrate();
    }
}

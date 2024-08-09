/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.services;

import javax.annotation.PostConstruct;
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
     * @param migration Service to migrate user data from MongoDB to GridGain persistence.
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

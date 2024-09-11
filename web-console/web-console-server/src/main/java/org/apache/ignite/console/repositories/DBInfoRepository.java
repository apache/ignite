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

package org.apache.ignite.console.repositories;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.DBInfoDto;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.messages.WebConsoleMessageSourceAccessor;
import org.apache.ignite.console.tx.TransactionManager;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with DBInfoDto.
 */
@Repository
public class DBInfoRepository {
    /** */
    private final TransactionManager txMgr;    

    /** Messages accessor. */
    private final WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();


    /** */
    private Table<DBInfoDto> datasourceTbl;

    /** */
    private OneToManyIndex<UUID, UUID> datasourcesIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public DBInfoRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter(() -> {
            datasourceTbl = new Table<>(ignite, "wc_datasource");
            datasourceTbl.addUniqueIndex(
            		a -> a.getJndiName().trim().toLowerCase(),
                    acc -> messages.getMessageWithArgs("err.account-with-jdniName-exists",acc.getJndiName()));
            
            datasourcesIdx = new OneToManyIndex<>(ignite, "wc_datasource_idx");
        });
    }

    /**
     * @param accId Account ID.
     * @return List of notebooks for specified account.
     */
    public Collection<DBInfoDto> list(UUID accId) {
        return txMgr.doInTransaction(() -> {
            Set<UUID> notebooksIds = datasourcesIdx.get(accId);

            return datasourceTbl.loadAll(notebooksIds);
        });
    }
    
    /**
     * @param accId Account ID.
     * @return List of notebooks for specified account.
     */
    public DBInfoDto get(UUID accId,UUID datasourceID) {
        return txMgr.doInTransaction(() -> {
            return datasourceTbl.get(datasourceID);
        });
    }

    /**
     * Save notebook.
     *
     * @param accId Account ID.
     * @param notebook Notebook to save.
     */
    public void save(UUID accId, DBInfoDto notebook) {
        txMgr.doInTransaction(() -> {
            datasourcesIdx.validateBeforeSave(accId, notebook.getId(), datasourceTbl);

            datasourceTbl.save(notebook);

            datasourcesIdx.add(accId, notebook.getId());
        });
    }

    /**
     * Delete notebook.
     *
     * @param accId Account ID.
     * @param notebookId Notebook ID to delete.
     */
    public void delete(UUID accId, UUID notebookId) {
        txMgr.doInTransaction(() -> {
            datasourcesIdx.validate(accId, notebookId);

            DBInfoDto notebook = datasourceTbl.get(notebookId);

            if (notebook != null) {
                datasourceTbl.delete(notebookId);

                datasourcesIdx.remove(accId, notebookId);
            }
        });
    }

    /**
     * Delete all notebook for specified account.
     *
     * @param accId Account ID.
     */
    public void deleteAll(UUID accId) {
        txMgr.doInTransaction(() -> {
            Set<UUID> notebooksIds = datasourcesIdx.delete(accId);

            datasourceTbl.deleteAll(notebooksIds);
        });
    }
}

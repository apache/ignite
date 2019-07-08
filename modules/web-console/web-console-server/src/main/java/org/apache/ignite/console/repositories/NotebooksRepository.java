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
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.tx.TransactionManager;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with notebooks.
 */
@Repository
public class NotebooksRepository {
    /** */
    private final TransactionManager txMgr;

    /** */
    private Table<Notebook> notebooksTbl;

    /** */
    private OneToManyIndex<UUID> notebooksIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public NotebooksRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter("notebooks", () -> {
            MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

            notebooksTbl = new Table<>(ignite, "wc_notebooks");

            notebooksIdx = new OneToManyIndex<>(
                    ignite,
                    "wc_account_notebooks_idx",
                    (key) -> messages.getMessage("err.data-access-violation")
            );
        });
    }

    /**
     * @param accId Account ID.
     * @return List of notebooks for specified account.
     */
    public Collection<Notebook> list(UUID accId) {
        return txMgr.doInTransaction(() -> {
            Set<UUID> notebooksIds = notebooksIdx.load(accId);

            return notebooksTbl.loadAll(notebooksIds);
        });
    }

    /**
     * Save notebook.
     *
     * @param accId Account ID.
     * @param notebook Notebook to save.
     */
    public void save(UUID accId, Notebook notebook) {
        txMgr.doInTransaction(() -> {
            notebooksIdx.validateBeforeSave(accId, notebook.getId(), notebooksTbl);

            notebooksTbl.save(notebook);

            notebooksIdx.add(accId, notebook.getId());
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
            notebooksIdx.validate(accId, notebookId);

            Notebook notebook = notebooksTbl.load(notebookId);

            if (notebook != null) {
                notebooksTbl.delete(notebookId);

                notebooksIdx.remove(accId, notebookId);
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
            Set<UUID> notebooksIds = notebooksIdx.delete(accId);

            notebooksTbl.deleteAll(notebooksIds);
        });
    }
}

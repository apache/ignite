

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
    private OneToManyIndex<UUID, UUID> notebooksIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public NotebooksRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter(() -> {
            MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

            notebooksTbl = new Table<>(ignite, "wc_notebooks");
            notebooksIdx = new OneToManyIndex<>(ignite, "wc_account_notebooks_idx");
        });
    }

    /**
     * @param accId Account ID.
     * @return List of notebooks for specified account.
     */
    public Collection<Notebook> list(UUID accId) {
        return txMgr.doInTransaction(() -> {
            Set<UUID> notebooksIds = notebooksIdx.get(accId);

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

            Notebook notebook = notebooksTbl.get(notebookId);

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

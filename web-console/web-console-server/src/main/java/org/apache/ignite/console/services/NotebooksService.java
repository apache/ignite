

package org.apache.ignite.console.services;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.repositories.NotebooksRepository;
import org.springframework.stereotype.Service;

/**
 * Service to handle notebooks.
 */
@Service
public class NotebooksService {
    /** */
    private final NotebooksRepository notebooksRepo;

    /**
     * @param notebooksRepo Repository to work with notebooks.
     */
    public NotebooksService(NotebooksRepository notebooksRepo) {
        this.notebooksRepo = notebooksRepo;
    }

    /**
     * Delete all notebook for specified account.
     *
     * @param accId Account ID.
     */
    void deleteByAccountId(UUID accId) {
        notebooksRepo.deleteAll(accId);
    }

    /**
     * @param accId Account ID.
     * @return List of notebooks for specified account.
     */
    public Collection<Notebook> list(UUID accId) {
        return notebooksRepo.list(accId);
    }

    /**
     * @param accId Account ID.
     * @param notebook Notebook.
     */
    public void save(UUID accId, Notebook notebook) {
        notebooksRepo.save(accId, notebook);
    }

    /**
     * @param accId Account ID.
     * @param notebookId Notebook ID.
     */
    public void delete(UUID accId, UUID notebookId) {
        notebooksRepo.delete(accId, notebookId);
    }
}

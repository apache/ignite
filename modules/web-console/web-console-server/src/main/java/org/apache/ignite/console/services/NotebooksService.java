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

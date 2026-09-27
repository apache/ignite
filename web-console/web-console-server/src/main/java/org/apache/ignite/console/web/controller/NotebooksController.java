

package org.apache.ignite.console.web.controller;

import java.util.Collection;
import java.util.UUID;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.services.NotebooksService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for notebooks API.
 */
@RestController
@RequestMapping(path = "/api/v1/notebooks")
public class NotebooksController {
    /** */
    private final NotebooksService notebooksSrv;

    /**
     * @param notebooksSrv Notebooks service.
     */
    @Autowired
    public NotebooksController(NotebooksService notebooksSrv) {
        this.notebooksSrv = notebooksSrv;
    }

    /**
     * @param acc Account.
     * @return Collection of notebooks.
     */
    @Operation(summary = "Get user's notebooks.")
    @GetMapping
    public ResponseEntity<Collection<Notebook>> list(@AuthenticationPrincipal Account acc) {
        return ResponseEntity.ok(notebooksSrv.list(acc.getId()));
    }

    /**
     * @param acc Account.
     */
    @Operation(summary = "Save user's notebook.")
    @PutMapping(consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> save(@AuthenticationPrincipal Account acc, @RequestBody Notebook notebook) {
        notebooksSrv.save(acc.getId(), notebook);

        return ResponseEntity.ok().build();
    }

    /**
     * @param acc Account.
     * @param notebookId Notebook ID.
     */
    @Operation(summary = "Delete user's notebook.")
    @DeleteMapping(path = "/{notebookId}")
    public ResponseEntity<Void> delete(
        @AuthenticationPrincipal Account acc,
        @PathVariable("notebookId") UUID notebookId
    ) {
        notebooksSrv.delete(acc.getId(), notebookId);

        return ResponseEntity.ok().build();
    }
}

package org.apache.ignite.console.web.controller;

import java.util.Collection;
import java.util.UUID;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.DBInfoDto;
import org.apache.ignite.console.repositories.DBInfoRepository;
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
 * Controller for datasources API.
 */
@RestController
@RequestMapping(path = "/api/v1/datasource")
public class DBInfoController {
    /** */
    private final DBInfoRepository datasourcesSrv;

    /**
     * @param datasourcesSrv Notebooks service.
     */
    @Autowired
    public DBInfoController(DBInfoRepository datasourcesSrv) {
        this.datasourcesSrv = datasourcesSrv;
    }
    
    /**
     * @param acc Account.
     * @param datasourceId Notebook ID.
     */
    @Operation(summary = "get user's datasource.")
    @GetMapping(path = "/{datasourceId}")
    public ResponseEntity<DBInfoDto> get(
        @AuthenticationPrincipal Account acc,
        @PathVariable("datasourceId") UUID datasourceId
    ) {
    	DBInfoDto dto = datasourcesSrv.get(acc.getId(), datasourceId);
    	
        return ResponseEntity.ok(dto);
    }

    /**
     * @param acc Account.
     * @return Collection of datasources.
     */
    @Operation(summary = "Get user's datasources.")
    @GetMapping
    public ResponseEntity<Collection<DBInfoDto>> list(@AuthenticationPrincipal Account acc) {
        return ResponseEntity.ok(datasourcesSrv.list(acc.getId()));
    }

    /**
     * @param acc Account.
     */
    @Operation(summary = "Save user's datasource.")
    @PutMapping(consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<UUID> save(@AuthenticationPrincipal Account acc, @RequestBody DBInfoDto datasource) {
        if(datasource.getId()==null) {
        	datasource.setId(UUID.randomUUID());
        	datasource.setAccId(acc.getId());
        }
        if(datasource.getAccId()==null) {        	
        	datasource.setAccId(acc.getId());
        }
    	datasourcesSrv.save(acc.getId(), datasource);

        return ResponseEntity.ok().body(datasource.getId());
    }

    /**
     * @param acc Account.
     * @param datasourceId Notebook ID.
     */
    @Operation(summary = "Delete user's datasource.")
    @DeleteMapping(path = "/{datasourceId}")
    public ResponseEntity<Void> delete(
        @AuthenticationPrincipal Account acc,
        @PathVariable("datasourceId") UUID datasourceId
    ) {
        datasourcesSrv.delete(acc.getId(), datasourceId);

        return ResponseEntity.ok().build();
    }
}

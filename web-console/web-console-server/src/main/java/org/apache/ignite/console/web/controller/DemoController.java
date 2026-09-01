

package org.apache.ignite.console.web.controller;

import io.swagger.v3.oas.annotations.Operation;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.DemoService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for demo API.
 */
@RestController
@RequestMapping(path = "/api/v1/demo")
public class DemoController {
    /** */
    private DemoService demoSrv;

    /**
     * @param demoSrv Demo service.
     */
    public DemoController(DemoService demoSrv) {
        this.demoSrv = demoSrv;
    }

    /**
     * @param acc Account.
     */
    @Operation(summary = "Reset demo configuration.")
    @PostMapping(path = "/reset")
    public ResponseEntity<Void> reset(@AuthenticationPrincipal Account acc) {
        demoSrv.reset(acc.getId());

        return ResponseEntity.ok().build();
    }
}

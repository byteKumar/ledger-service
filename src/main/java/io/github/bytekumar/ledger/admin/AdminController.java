package io.github.bytekumar.ledger.admin;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operator endpoints. Not authenticated in this project; in production this would sit
 * behind admin auth or an internal-only port.
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final ReconcileService reconcile;

    public AdminController(ReconcileService reconcile) {
        this.reconcile = reconcile;
    }

    /** 200 when the books balance, 500 when they don't, so a plain HTTP check can alert on it. */
    @GetMapping("/reconcile")
    ResponseEntity<ReconcileService.Report> reconcile() {
        ReconcileService.Report report = reconcile.reconcile();
        return ResponseEntity.status(report.consistent() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR).body(report);
    }
}

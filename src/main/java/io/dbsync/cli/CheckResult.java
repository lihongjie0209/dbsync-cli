package io.dbsync.cli;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Result of a single diagnostic check performed by the {@code test} subcommand.
 */
public final class CheckResult {

    public enum Status { PASS, WARN, FAIL }

    private final String       id;
    private final Status       status;
    private final String       message;
    private final List<String> details;

    private CheckResult(String id, Status status, String message, List<String> details) {
        this.id      = id;
        this.status  = status;
        this.message = message;
        this.details = Collections.unmodifiableList(details);
    }

    public static CheckResult pass(String id, String message, String... details) {
        return new CheckResult(id, Status.PASS, message, Arrays.asList(details));
    }

    public static CheckResult warn(String id, String message, String... details) {
        return new CheckResult(id, Status.WARN, message, Arrays.asList(details));
    }

    public static CheckResult fail(String id, String message, String... details) {
        return new CheckResult(id, Status.FAIL, message, Arrays.asList(details));
    }

    public String       getId()      { return id; }
    public Status       getStatus()  { return status; }
    public String       getMessage() { return message; }
    public List<String> getDetails() { return details; }

    public boolean isPass() { return status == Status.PASS; }
    public boolean isWarn() { return status == Status.WARN; }
    public boolean isFail() { return status == Status.FAIL; }

    @Override
    public String toString() {
        return status + " [" + id + "] " + message;
    }
}

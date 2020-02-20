package pl.kwiatek.redis;

import lombok.Value;

@Value
public class CancelledTestRunMessage {

    public static final String CHANNEL = "test-run-cancels";

    String tenant;
    long testRunId;
}

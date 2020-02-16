package pl.kwiatek.redis;

import lombok.Value;

@Value
public class CancelledTestRunMessage {
    String tenant;
    long testRunId;
}

package pl.kwiatek.redis;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.FieldNameConstants;

import static lombok.AccessLevel.PRIVATE;

@Value
@FieldNameConstants(level = PRIVATE)
public class CancelledTestRunMessage {
    @JsonProperty(Fields.tenant)
    String tenant;
    @JsonProperty(Fields.testRunId)
    long testRunId;
}

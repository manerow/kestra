package io.kestra.core.runners;

import io.kestra.core.models.HasUID;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.State;
import io.kestra.core.queues.event.DispatchEvent;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class SubflowExecutionResult implements HasUID, DispatchEvent {
    @NotNull
    private TaskRun parentTaskRun;

    @NotNull
    private String executionId;

    @NotNull
    private State.Type state;

    @Nullable
    Map<String, Object> outputs;

    @Override
    public String key() {
        return executionId;
    }

    @Override
    public String uid() {
        return executionId;
    }
}

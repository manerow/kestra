package io.kestra.core.repositories;

import io.kestra.core.models.executions.TaskOutput;

import java.util.List;
import java.util.Optional;

public interface TaskOutputRepositoryInterface {
    Optional<TaskOutput> findById(String tenantId, String taskRunId);

    TaskOutput save(TaskOutput taskOutput);

    List<TaskOutput> findByExecution(String tenantId, String executionId);
}

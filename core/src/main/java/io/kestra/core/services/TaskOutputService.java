package io.kestra.core.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.exceptions.InternalException;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.TaskOutput;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.executions.Variables;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.repositories.TaskOutputRepositoryInterface;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.InternalStorage;
import io.kestra.core.storages.NamespaceFactory;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.MapUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Singleton
public class TaskOutputService {
    private static final ObjectMapper ION_MAPPER = JacksonMapper.ofIon();

    private final TaskOutputRepositoryInterface outputRepository;
    private final StorageInterface storageInterface;
    private final NamespaceFactory namespaceFactory;
    private final int limit;

    public TaskOutputService(TaskOutputRepositoryInterface outputRepository, StorageInterface storageInterface, NamespaceFactory namespaceFactory, @Value("${kestra.task.outputs.limit:-1}") int limit) {
        this.outputRepository = outputRepository;
        this.storageInterface = storageInterface;
        this.namespaceFactory = namespaceFactory;
        this.limit = limit;
    }

    public void saveOutputs(TaskRun taskRun, Output outputs) throws InternalException {
        Map<String, Object> outputMap = Optional.ofNullable(outputs).map(o -> o.toMap()).orElse(Collections.emptyMap());
        saveOutputs(taskRun, outputMap);
    }

    public void saveOutputs(TaskRun taskRun, Map<String, Object> outputMap) throws InternalException {
        if (!MapUtils.isEmpty(outputMap)) {
            try {
                byte[] value = ION_MAPPER.writeValueAsBytes(outputMap);
                var output = shouldStoreInInternalStorage(value) ? storeToInternalStorage(taskRun, value) :
                    new TaskOutput(taskRun.getId(), taskRun.getTenantId(), taskRun.getExecutionId(), value, null);
                outputRepository.save(output);
            } catch (JsonProcessingException e) {
                throw new InternalException(e);
            }
        }
    }

    /**
     * Whether the value should be store in internal storage or not.
     * In OSS: this is only defined by the task output limit configuration if set.
     * In EE, this is also governed by the Execution data in internal storage configuration.
     */
    protected boolean shouldStoreInInternalStorage(byte[] value) {
        if (limit < 0) {
            return false;
        }
        return value.length > limit;
    }

    private TaskOutput storeToInternalStorage(TaskRun taskRun, byte[] outputBytes) throws InternalException {
        try {
            var context = StorageContext.forTask(taskRun);
            var storage = new InternalStorage(context, storageInterface, namespaceFactory);
            File file = Files.createTempFile("output-", ".ion").toFile();
            Files.write(file.toPath(), outputBytes);
            var uri = storage.putFile(file);
            return new TaskOutput(taskRun.getId(), taskRun.getTenantId(), taskRun.getExecutionId(), null, uri.toString());
        } catch (IOException e) {
            throw new InternalException(e);
        }
    }

    @VisibleForTesting
    public Map<String, Object> getOutputs(TaskRun taskRun)  throws InternalException {
        return outputRepository.findById(taskRun.getTenantId(), taskRun.getId())
            .map(throwFunction(output -> readOutput(taskRun, output)))
            .orElse(Collections.emptyMap());
    }

    public List<TaskOutput> getOutputs(Execution execution)  throws InternalException {
        return outputRepository.findByExecution(execution.getTenantId(), execution.getId());
    }

    // FIXME temporary
    public Map<String, Object> outputs(Execution execution) {
        if (execution == null || execution.getTaskRunList() == null) {
            return Collections.emptyMap();
        }

        // we pre-compute the map of taskrun by id to avoid traversing the list of all taskrun for each taskrun
        Map<String, TaskRun> byIds = execution.getTaskRunList().stream().collect(Collectors.toMap(
            taskRun -> taskRun.getId(),
            taskRun -> taskRun
        ));

        try {
            // load all outputs
            List<TaskOutput> allTaskOutputs = getOutputs(execution);

            Map<String, Object> result = new HashMap<>();
            execution.getTaskRunList().stream()
                .collect(Collectors.groupingBy(taskRun -> taskRun.getTaskId()))
                .forEach((taskId, taskRuns) -> {
                    Map<String, Object> taskOutputs = new HashMap<>();
                    for (TaskRun current : taskRuns) {
                        var outputs = allTaskOutputs.stream().filter(it -> it.taskRunId().equals(current.getId())).findAny();
                        if (outputs.isPresent()) {
                            try {
                                var outputMap = readOutput(current, outputs.get());
                                if (current.getIteration() != null) {
                                    Map<String, Object> merged = MapUtils.merge(taskOutputs, outputs(current, outputMap, byIds));
                                    // If one of two of the map is null in the merge() method, we just return the other
                                    // And if the not null map is a Variables (= read-only), we cast it back to a simple
                                    // hashmap to avoid taskOutputs becoming read-only
                                    // i.e this happens in nested loopUntil tasks
                                    if (merged instanceof Variables) {
                                        merged = new HashMap<>(merged);
                                    }
                                    taskOutputs = merged;
                                } else {
                                    taskOutputs.putAll(outputs(current, outputMap, byIds));
                                }
                            } catch (InternalException e) {
                                throw new RuntimeException(e); // FIXME building the runcontext is not supposed to fail
                            }
                        }
                    }
                    result.put(taskId, taskOutputs);
                });

            return result;
        } catch (InternalException e) {
            throw new RuntimeException(e); // FIXME building the runcontext is not supposed to fail
        }
    }

    private Map<String, Object> readOutput(TaskRun taskRun, TaskOutput taskOutput) throws InternalException {
        try {
            return taskOutput.value() != null ? readFromValue(taskOutput) : readFromInternalStorage(taskRun, taskOutput);
        } catch (IOException e) {
            throw new InternalException(e);
        }
    }

    private Map<String, Object> readFromValue(TaskOutput taskOutput) throws IOException {
        return ION_MAPPER.readValue(taskOutput.value(), JacksonMapper.MAP_TYPE_REFERENCE);
    }

    private Map<String, Object> readFromInternalStorage(TaskRun taskRun, TaskOutput taskOutput) throws IOException {
        if (taskOutput.uri() == null) {
            return null;
        }

        var context = StorageContext.forTask(taskRun);
        var storage = new InternalStorage(context, storageInterface, namespaceFactory);
        try (var is = storage.getFile(URI.create(taskOutput.uri()))) {
            return ION_MAPPER.readValue(is, JacksonMapper.MAP_TYPE_REFERENCE);
        }
    }

    private Map<String, Object> outputs(TaskRun taskRun, Map<String, Object> outputs, Map<String, TaskRun> byIds) {
        List<TaskRun> parents = findParents(taskRun, byIds)
            .stream()
            .filter(r -> r.getValue() != null)
            .toList();

        if (parents.isEmpty()) {
            if (taskRun.getValue() == null) {
                return outputs;
            } else {
                return Map.of(taskRun.getValue(), outputs);
            }
        }

        Map<String, Object> result = HashMap.newHashMap(1);
        Map<String, Object> current = result;

        for (TaskRun t : parents) {
            HashMap<String, Object> item = HashMap.newHashMap(1);
            current.put(t.getValue(), item);
            current = item;
        }

        if (taskRun.getValue() != null) {
            current.put(taskRun.getValue(), outputs);
        } else {
            current.putAll(outputs);
        }

        return result;
    }

    /**
     * Find all parents from this {@link TaskRun}. This method does the same as #Execution.findParents(TaskRun
     * taskRun) but for performance reason, as it's called a lot, we pre-compute the map of taskrun
     * by ID and use it here.
     */
    private List<TaskRun> findParents(TaskRun taskRun, Map<String, TaskRun> taskRunById) {
        if (taskRun.getParentTaskRunId() == null || taskRunById.isEmpty()) {
            return Collections.emptyList();
        }

        List<TaskRun> result = new ArrayList<>();
        boolean ended = false;
        while (!ended) {
            final TaskRun finalTaskRun = taskRun;
            TaskRun find = taskRunById.get(finalTaskRun.getParentTaskRunId());

            if (find != null) {
                result.add(find);
                taskRun = find;
            } else {
                ended = true;
            }
        }

        Collections.reverse(result);

        return result;
    }
}

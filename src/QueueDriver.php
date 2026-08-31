<?php

declare(strict_types=1);

namespace PhpModern\Queue;

/**
 * The contract Worker actually depends on — extracted from DatabaseQueue's
 * existing public methods (unchanged, this is a pure interface extraction)
 * so a second backend (RedisQueue) can stand in for it. push()'s extra
 * $batchId parameter is how Batch::dispatch() tags a group of jobs without
 * polluting the payload a job's constructor gets hydrated from — see
 * QueuedJob's own docblock for why that separation matters.
 */
interface QueueDriver
{
    /**
     * @param class-string<Job> $jobClass
     * @param array<string, mixed> $payload
     */
    public function push(string $jobClass, array $payload = [], ?string $batchId = null): int;

    public function pop(): ?QueuedJob;

    public function delete(int $id): void;

    public function markFailed(int $id, string $error, ?int $attempts = null): void;

    public function release(int $id, int $attempts, string $availableAt): void;
}

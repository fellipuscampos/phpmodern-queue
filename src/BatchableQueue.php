<?php

declare(strict_types=1);

namespace PhpModern\Queue;

/**
 * Opt-in on top of QueueDriver, the same "extra interface, not a required
 * method" shape RetryableJob uses — a driver that can track a batch's
 * completion count implements this; Worker checks for it with `instanceof`
 * before doing any batch bookkeeping, so a hypothetical future driver
 * without batch support still works for everything else.
 */
interface BatchableQueue extends QueueDriver
{
    /**
     * Registers a new batch of $total jobs, with an optional job to push
     * once every one of them has completed.
     *
     * @param array{0: class-string<Job>, 1: array<string, mixed>}|null $then
     */
    public function createBatch(string $batchId, int $total, ?array $then): void;

    /**
     * Atomically records one more job in $batchId as done and returns the
     * updated counts — atomic because two workers finishing the batch's
     * last two jobs at the same instant must not both conclude they were
     * the one that finished it.
     *
     * @return array{completed: int, total: int}
     */
    public function recordBatchCompletion(string $batchId): array;

    /** @return array{0: class-string<Job>, 1: array<string, mixed>}|null */
    public function batchThen(string $batchId): ?array;

    public function deleteBatch(string $batchId): void;
}

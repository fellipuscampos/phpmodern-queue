<?php

declare(strict_types=1);

namespace PhpModern\Queue;

/**
 * $payload is exactly what the job's constructor is unpacked from
 * (`new $jobClass(...$payload)`) — batchId is deliberately kept separate
 * from it rather than smuggled in as another payload key, so a job class
 * never has to declare (or accidentally break on) a batch-tracking
 * parameter it has no reason to know about.
 */
final class QueuedJob
{
    /** @param array<string, mixed> $payload */
    public function __construct(
        public readonly int $id,
        public readonly string $jobClass,
        public readonly array $payload,
        public readonly int $attempts = 0,
        public readonly ?string $batchId = null,
    ) {
    }
}

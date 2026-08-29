<?php

declare(strict_types=1);

namespace PhpModern\Queue;

final class QueuedJob
{
    /** @param array<string, mixed> $payload */
    public function __construct(
        public readonly int $id,
        public readonly string $jobClass,
        public readonly array $payload,
    ) {
    }
}

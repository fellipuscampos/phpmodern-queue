<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests\Fixtures;

use PhpModern\Queue\RetryableJob;
use RuntimeException;

final class RetryableFailingJob implements RetryableJob
{
    public function __construct(public readonly string $reason)
    {
    }

    public function maxAttempts(): int
    {
        return 3;
    }

    public function handle(): void
    {
        throw new RuntimeException($this->reason);
    }
}

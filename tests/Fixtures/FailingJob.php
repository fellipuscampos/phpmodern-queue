<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests\Fixtures;

use PhpModern\Queue\Job;
use RuntimeException;

final class FailingJob implements Job
{
    public function __construct(public readonly string $reason)
    {
    }

    public function handle(): void
    {
        throw new RuntimeException($this->reason);
    }
}

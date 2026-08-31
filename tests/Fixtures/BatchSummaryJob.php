<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests\Fixtures;

use PhpModern\Queue\Job;

final class BatchSummaryJob implements Job
{
    /** @var list<string> */
    public static array $ran = [];

    public function __construct(public readonly string $label)
    {
    }

    public function handle(): void
    {
        self::$ran[] = $this->label;
    }
}

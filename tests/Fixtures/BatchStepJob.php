<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests\Fixtures;

use PhpModern\Queue\Job;

final class BatchStepJob implements Job
{
    /** @var list<string> */
    public static array $handled = [];

    public function __construct(public readonly string $name)
    {
    }

    public function handle(): void
    {
        self::$handled[] = $this->name;
    }
}

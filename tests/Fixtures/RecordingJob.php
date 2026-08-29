<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests\Fixtures;

use PhpModern\Queue\Job;

final class RecordingJob implements Job
{
    /** @var list<array{greeting: string}> */
    public static array $handled = [];

    public function __construct(public readonly string $greeting)
    {
    }

    public function handle(): void
    {
        self::$handled[] = ['greeting' => $this->greeting];
    }
}

<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests\Fixtures;

use PhpModern\Queue\ChainableJob;
use PhpModern\Queue\Job;

final class ChainStepJob implements ChainableJob
{
    /** @var list<string> */
    public static array $handled = [];

    /**
     * @param class-string<Job>|null $nextClass
     * @param array<string, mixed> $nextPayload
     */
    public function __construct(
        public readonly string $name,
        public readonly ?string $nextClass = null,
        public readonly array $nextPayload = [],
    ) {
    }

    public function handle(): void
    {
        self::$handled[] = $this->name;
    }

    public function next(): ?array
    {
        return $this->nextClass !== null ? [$this->nextClass, $this->nextPayload] : null;
    }
}

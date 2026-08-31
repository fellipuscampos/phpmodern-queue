<?php

declare(strict_types=1);

namespace PhpModern\Queue;

/**
 * Opt-in on top of Job, the same shape RetryableJob already uses: a plain
 * Job's success just deletes it, exactly as before. A ChainableJob's
 * success additionally pushes whatever next() returns — so a sequence of
 * jobs runs one after another, each only starting once the previous one
 * actually succeeded (a failed link never triggers next(), the same way a
 * failed non-chained job never runs anything after it).
 */
interface ChainableJob extends Job
{
    /** @return array{0: class-string<Job>, 1: array<string, mixed>}|null */
    public function next(): ?array;
}

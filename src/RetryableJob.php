<?php

declare(strict_types=1);

namespace PhpModern\Queue;

/**
 * Opt-in on top of Job: a job that wants automatic retry with exponential
 * backoff instead of being marked failed after its first unhandled
 * exception. Worker checks for this interface specifically — a plain Job
 * still fails immediately on its first exception, exactly as it always has.
 */
interface RetryableJob extends Job
{
    /** Total attempts allowed, including the first — not "retries" on top of it. */
    public function maxAttempts(): int;
}

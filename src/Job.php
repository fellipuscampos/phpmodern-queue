<?php

declare(strict_types=1);

namespace PhpModern\Queue;

/**
 * A job is hydrated from its queued payload via named-argument unpacking
 * (`new $jobClass(...$payload)`), so its constructor parameter names must
 * match the payload keys passed to DatabaseQueue::push().
 */
interface Job
{
    public function handle(): void;
}

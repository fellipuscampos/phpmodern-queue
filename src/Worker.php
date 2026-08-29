<?php

declare(strict_types=1);

namespace PhpModern\Queue;

use RuntimeException;
use Throwable;

final class Worker
{
    public function __construct(private readonly DatabaseQueue $queue)
    {
    }

    /**
     * Processes at most one job.
     *
     * @return bool true if a job was processed (successfully or not), false if the queue was empty
     */
    public function runOnce(): bool
    {
        $queuedJob = $this->queue->pop();

        if ($queuedJob === null) {
            return false;
        }

        try {
            $job = new $queuedJob->jobClass(...$queuedJob->payload);

            if (!$job instanceof Job) {
                throw new RuntimeException(sprintf('%s must implement %s.', $queuedJob->jobClass, Job::class));
            }

            $job->handle();
            $this->queue->delete($queuedJob->id);
        } catch (Throwable $exception) {
            $this->queue->markFailed($queuedJob->id, $exception->getMessage());
        }

        return true;
    }

    /**
     * Runs forever, polling the queue. Meant to be the entire body of a
     * standalone worker process (see bin/worker.php) — never call this from
     * inside a PHP-FPM request.
     */
    public function run(int $pollIntervalMicroseconds = 200_000): void
    {
        while (true) {
            if (!$this->runOnce()) {
                usleep($pollIntervalMicroseconds);
            }
        }
    }
}

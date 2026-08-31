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

        $job = null;

        try {
            $job = new $queuedJob->jobClass(...$queuedJob->payload);

            if (!$job instanceof Job) {
                throw new RuntimeException(sprintf('%s must implement %s.', $queuedJob->jobClass, Job::class));
            }

            $job->handle();
            $this->queue->delete($queuedJob->id);
        } catch (Throwable $exception) {
            $this->handleFailure($queuedJob, $job, $exception);
        }

        return true;
    }

    /**
     * A plain Job fails permanently on its very first exception, exactly as
     * it always has. A RetryableJob instead gets released back to the queue
     * with an exponential backoff delay (2, 4, 8, 16... seconds) until its
     * own maxAttempts() is reached, at which point it fails permanently too.
     */
    private function handleFailure(QueuedJob $queuedJob, mixed $job, Throwable $exception): void
    {
        $maxAttempts = $job instanceof RetryableJob ? $job->maxAttempts() : 1;
        $attempts = $queuedJob->attempts + 1;

        if ($attempts >= $maxAttempts) {
            $this->queue->markFailed($queuedJob->id, $exception->getMessage(), $attempts);

            return;
        }

        $delaySeconds = 2 ** $attempts;
        $availableAt = date('Y-m-d H:i:s', time() + $delaySeconds);

        $this->queue->release($queuedJob->id, $attempts, $availableAt);
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

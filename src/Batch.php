<?php

declare(strict_types=1);

namespace PhpModern\Queue;

/**
 * Dispatches a group of jobs together and, once a worker has settled every
 * one of them (succeeded or permanently failed — see Worker::handleFailure()),
 * pushes $then. The "N jobs, then one more once they're all done" shape
 * behind a bulk import notification or a report generated from several
 * parallel pieces of work.
 */
final class Batch
{
    /**
     * @param list<array{0: class-string<Job>, 1: array<string, mixed>}> $jobs
     * @param array{0: class-string<Job>, 1: array<string, mixed>}|null $then
     */
    public static function dispatch(BatchableQueue $queue, array $jobs, ?array $then = null): string
    {
        $batchId = bin2hex(random_bytes(8));

        if ($jobs === []) {
            if ($then !== null) {
                $queue->push($then[0], $then[1]);
            }

            return $batchId;
        }

        $queue->createBatch($batchId, count($jobs), $then);

        foreach ($jobs as [$jobClass, $payload]) {
            $queue->push($jobClass, $payload, $batchId);
        }

        return $batchId;
    }
}

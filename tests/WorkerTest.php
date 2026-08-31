<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests;

use PhpModern\Orm\Connection;
use PhpModern\Queue\DatabaseQueue;
use PhpModern\Queue\Tests\Fixtures\FailingJob;
use PhpModern\Queue\Tests\Fixtures\RecordingJob;
use PhpModern\Queue\Tests\Fixtures\RetryableFailingJob;
use PhpModern\Queue\Worker;
use PHPUnit\Framework\TestCase;

final class WorkerTest extends TestCase
{
    protected function setUp(): void
    {
        RecordingJob::$handled = [];
    }

    public function test_runonce_returns_false_when_the_queue_is_empty(): void
    {
        $worker = new Worker(new DatabaseQueue(Connection::sqlite(':memory:')));

        self::assertFalse($worker->runOnce());
    }

    public function test_processes_a_job_and_deletes_it_on_success(): void
    {
        $connection = Connection::sqlite(':memory:');
        $queue = new DatabaseQueue($connection);
        $queue->push(RecordingJob::class, ['greeting' => 'hi']);

        $processed = (new Worker($queue))->runOnce();

        self::assertTrue($processed);
        self::assertSame([['greeting' => 'hi']], RecordingJob::$handled);

        $count = $connection->pdo()->query('SELECT COUNT(*) FROM phpmodern_jobs')->fetchColumn();
        self::assertSame('0', (string) $count);
    }

    public function test_marks_the_job_failed_when_handle_throws(): void
    {
        $connection = Connection::sqlite(':memory:');
        $queue = new DatabaseQueue($connection);
        $queue->push(FailingJob::class, ['reason' => 'boom']);

        (new Worker($queue))->runOnce();

        $statement = $connection->pdo()->query('SELECT status, error FROM phpmodern_jobs');
        $row = $statement->fetch();

        self::assertSame('failed', $row['status']);
        self::assertSame('boom', $row['error']);
    }

    public function test_a_retryable_job_is_released_with_backoff_instead_of_failed_immediately(): void
    {
        $connection = Connection::sqlite(':memory:');
        $queue = new DatabaseQueue($connection);
        $queue->push(RetryableFailingJob::class, ['reason' => 'boom']);

        (new Worker($queue))->runOnce();

        $row = $connection->pdo()->query('SELECT status, attempts, available_at FROM phpmodern_jobs')->fetch();

        self::assertSame('pending', $row['status']);
        self::assertSame(1, (int) $row['attempts']);
        self::assertNotNull($row['available_at']);
        self::assertGreaterThan(date('Y-m-d H:i:s'), $row['available_at']);
    }

    public function test_a_retryable_job_is_not_reprocessed_until_its_backoff_passes(): void
    {
        $connection = Connection::sqlite(':memory:');
        $queue = new DatabaseQueue($connection);
        $queue->push(RetryableFailingJob::class, ['reason' => 'boom']);
        $worker = new Worker($queue);

        $worker->runOnce();

        self::assertFalse($worker->runOnce(), 'a job still waiting out its backoff must not be picked up');
    }

    public function test_a_retryable_job_fails_permanently_after_exhausting_max_attempts(): void
    {
        $connection = Connection::sqlite(':memory:');
        $queue = new DatabaseQueue($connection);
        $queue->push(RetryableFailingJob::class, ['reason' => 'boom']); // maxAttempts() === 3
        $worker = new Worker($queue);

        for ($i = 0; $i < 3; $i++) {
            $worker->runOnce();
            // Simulate the backoff delay having already elapsed instead of
            // actually sleeping several real seconds in a test.
            $connection->pdo()->exec("UPDATE phpmodern_jobs SET available_at = '2000-01-01 00:00:00'");
        }

        $row = $connection->pdo()->query('SELECT status, attempts, error FROM phpmodern_jobs')->fetch();

        self::assertSame('failed', $row['status']);
        self::assertSame(3, (int) $row['attempts']);
        self::assertSame('boom', $row['error']);
    }
}

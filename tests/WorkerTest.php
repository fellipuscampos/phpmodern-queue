<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests;

use PhpModern\Orm\Connection;
use PhpModern\Queue\DatabaseQueue;
use PhpModern\Queue\Tests\Fixtures\FailingJob;
use PhpModern\Queue\Tests\Fixtures\RecordingJob;
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
}

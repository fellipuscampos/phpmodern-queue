<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests;

use PhpModern\Orm\Connection;
use PhpModern\Queue\DatabaseQueue;
use PHPUnit\Framework\TestCase;

final class DatabaseQueueTest extends TestCase
{
    public function test_push_then_pop_returns_the_job_with_its_payload(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));

        $id = $queue->push('App\\Jobs\\SendWelcomeEmail', ['userId' => 42]);
        $job = $queue->pop();

        self::assertNotNull($job);
        self::assertSame($id, $job->id);
        self::assertSame('App\\Jobs\\SendWelcomeEmail', $job->jobClass);
        self::assertSame(['userId' => 42], $job->payload);
    }

    public function test_pop_returns_null_when_the_queue_is_empty(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));

        self::assertNull($queue->pop());
    }

    public function test_a_reserved_job_is_not_delivered_twice(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));
        $queue->push('App\\Jobs\\SendWelcomeEmail');

        self::assertNotNull($queue->pop());
        self::assertNull($queue->pop());
    }

    public function test_delete_removes_the_job_row(): void
    {
        $connection = Connection::sqlite(':memory:');
        $queue = new DatabaseQueue($connection);
        $id = $queue->push('App\\Jobs\\SendWelcomeEmail');

        $queue->delete($id);

        $count = $connection->pdo()->query('SELECT COUNT(*) FROM phpmodern_jobs')->fetchColumn();
        self::assertSame('0', (string) $count);
    }

    public function test_mark_failed_records_the_error_and_status(): void
    {
        $connection = Connection::sqlite(':memory:');
        $queue = new DatabaseQueue($connection);
        $id = $queue->push('App\\Jobs\\SendWelcomeEmail');

        $queue->markFailed($id, 'SMTP timeout');

        $statement = $connection->pdo()->prepare('SELECT status, error FROM phpmodern_jobs WHERE id = :id');
        $statement->execute(['id' => $id]);
        $row = $statement->fetch();

        self::assertSame('failed', $row['status']);
        self::assertSame('SMTP timeout', $row['error']);
    }
}

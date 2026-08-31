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

    public function test_mark_failed_can_also_record_the_attempts_it_took(): void
    {
        $connection = Connection::sqlite(':memory:');
        $queue = new DatabaseQueue($connection);
        $id = $queue->push('App\\Jobs\\SendWelcomeEmail');

        $queue->markFailed($id, 'SMTP timeout', 3);

        $statement = $connection->pdo()->prepare('SELECT attempts FROM phpmodern_jobs WHERE id = :id');
        $statement->execute(['id' => $id]);

        self::assertSame(3, (int) $statement->fetchColumn());
    }

    public function test_release_puts_a_job_back_to_pending_with_attempts_and_available_at(): void
    {
        $connection = Connection::sqlite(':memory:');
        $queue = new DatabaseQueue($connection);
        $id = $queue->push('App\\Jobs\\SendWelcomeEmail');
        $queue->pop(); // reserve it, as Worker would before a failure

        $futureTimestamp = date('Y-m-d H:i:s', time() + 60);
        $queue->release($id, 1, $futureTimestamp);

        $statement = $connection->pdo()->prepare('SELECT status, attempts, available_at FROM phpmodern_jobs WHERE id = :id');
        $statement->execute(['id' => $id]);
        $row = $statement->fetch();

        self::assertSame('pending', $row['status']);
        self::assertSame(1, (int) $row['attempts']);
        self::assertSame($futureTimestamp, $row['available_at']);
    }

    public function test_a_released_job_with_a_future_available_at_is_not_popped(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));
        $id = $queue->push('App\\Jobs\\SendWelcomeEmail');
        $queue->pop();
        $queue->release($id, 1, date('Y-m-d H:i:s', time() + 60));

        self::assertNull($queue->pop());
    }

    public function test_a_released_job_becomes_available_once_its_time_has_passed(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));
        $id = $queue->push('App\\Jobs\\SendWelcomeEmail');
        $queue->pop();
        $queue->release($id, 1, date('Y-m-d H:i:s', time() - 60));

        $job = $queue->pop();

        self::assertNotNull($job);
        self::assertSame($id, $job->id);
        self::assertSame(1, $job->attempts);
    }
}

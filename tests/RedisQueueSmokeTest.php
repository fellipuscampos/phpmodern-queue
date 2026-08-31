<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests;

use PhpModern\Queue\Batch;
use PhpModern\Queue\RedisQueue;
use PhpModern\Queue\Tests\Fixtures\BatchStepJob;
use PhpModern\Queue\Tests\Fixtures\BatchSummaryJob;
use PhpModern\Queue\Tests\Fixtures\FailingJob;
use PhpModern\Queue\Tests\Fixtures\RecordingJob;
use PhpModern\Queue\Tests\Fixtures\RetryableFailingJob;
use PhpModern\Queue\Worker;
use PHPUnit\Framework\TestCase;

/**
 * Exercises RedisQueue against a genuine Redis server via RespClient's real
 * TCP socket — skipped (not faked) when PHPMODERN_TEST_REDIS_HOST isn't
 * set, so it's a no-op on a machine with no Redis installed, but CI runs it
 * for real against a redis:7 service container (see .github/workflows/ci.yml),
 * the same "SQLite locally, real engines in CI" split the ORM's
 * CrossEngineSmokeTest already established for MySQL/PostgreSQL.
 */
final class RedisQueueSmokeTest extends TestCase
{
    private RedisQueue $queue;

    protected function setUp(): void
    {
        $host = getenv('PHPMODERN_TEST_REDIS_HOST');

        if ($host === false || $host === '') {
            self::markTestSkipped('PHPMODERN_TEST_REDIS_HOST is not set — no Redis server to test against.');
        }

        $port = (int) (getenv('PHPMODERN_TEST_REDIS_PORT') ?: 6379);

        // A fresh prefix per test run keeps this test isolated from any
        // other data in the same Redis instance without needing FLUSHDB.
        $prefix = 'phpmodern_queue_smoke_' . bin2hex(random_bytes(4));

        $this->queue = new RedisQueue($host, $port, $prefix);
        RecordingJob::$handled = [];
        BatchStepJob::$handled = [];
        BatchSummaryJob::$ran = [];
    }

    public function test_push_then_pop_returns_the_job_with_its_payload(): void
    {
        $id = $this->queue->push(RecordingJob::class, ['greeting' => 'hi from redis']);
        $job = $this->queue->pop();

        self::assertNotNull($job);
        self::assertSame($id, $job->id);
        self::assertSame(RecordingJob::class, $job->jobClass);
        self::assertSame(['greeting' => 'hi from redis'], $job->payload);
    }

    public function test_pop_returns_null_when_the_queue_is_empty(): void
    {
        self::assertNull($this->queue->pop());
    }

    public function test_two_pops_never_return_the_same_job(): void
    {
        $this->queue->push(RecordingJob::class, ['greeting' => 'only-one']);

        $first = $this->queue->pop();
        $second = $this->queue->pop();

        self::assertNotNull($first);
        self::assertNull($second, 'RPOPLPUSH must not let a second pop() see the same reserved job');
    }

    public function test_worker_processes_a_job_and_deletes_it_on_success(): void
    {
        $this->queue->push(RecordingJob::class, ['greeting' => 'processed']);

        $processed = (new Worker($this->queue))->runOnce();

        self::assertTrue($processed);
        self::assertSame([['greeting' => 'processed']], RecordingJob::$handled);
        self::assertNull($this->queue->pop());
    }

    public function test_worker_marks_a_plain_job_failed_on_its_first_exception(): void
    {
        $this->queue->push(FailingJob::class, ['reason' => 'boom']);

        (new Worker($this->queue))->runOnce();

        self::assertNull($this->queue->pop(), 'a permanently failed job must not still be poppable as pending');
    }

    public function test_a_retryable_job_is_released_with_backoff_instead_of_failed_immediately(): void
    {
        $this->queue->push(RetryableFailingJob::class, ['reason' => 'boom']);
        $worker = new Worker($this->queue);

        $worker->runOnce();

        self::assertFalse($worker->runOnce(), 'a job still waiting out its backoff must not be picked up yet');
    }

    public function test_a_batch_pushes_the_then_job_only_once_every_job_completes(): void
    {
        $worker = new Worker($this->queue);

        Batch::dispatch(
            $this->queue,
            [
                [BatchStepJob::class, ['name' => 'a']],
                [BatchStepJob::class, ['name' => 'b']],
            ],
            then: [BatchSummaryJob::class, ['label' => 'redis-batch-done']],
        );

        $worker->runOnce();
        self::assertSame([], BatchSummaryJob::$ran);

        $worker->runOnce();
        self::assertSame([], BatchSummaryJob::$ran, 'the then job is pushed, not run inline');

        $worker->runOnce();
        self::assertSame(['redis-batch-done'], BatchSummaryJob::$ran);
    }
}

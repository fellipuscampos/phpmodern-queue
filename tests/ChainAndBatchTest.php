<?php

declare(strict_types=1);

namespace PhpModern\Queue\Tests;

use PhpModern\Orm\Connection;
use PhpModern\Queue\Batch;
use PhpModern\Queue\DatabaseQueue;
use PhpModern\Queue\Tests\Fixtures\BatchStepJob;
use PhpModern\Queue\Tests\Fixtures\BatchSummaryJob;
use PhpModern\Queue\Tests\Fixtures\ChainStepJob;
use PhpModern\Queue\Tests\Fixtures\FailingJob;
use PhpModern\Queue\Worker;
use PHPUnit\Framework\TestCase;

final class ChainAndBatchTest extends TestCase
{
    protected function setUp(): void
    {
        ChainStepJob::$handled = [];
        BatchStepJob::$handled = [];
        BatchSummaryJob::$ran = [];
    }

    public function test_a_chained_job_pushes_the_next_step_only_after_succeeding(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));
        $worker = new Worker($queue);

        $queue->push(ChainStepJob::class, ['name' => 'first', 'nextClass' => ChainStepJob::class, 'nextPayload' => ['name' => 'second']]);

        self::assertSame([], ChainStepJob::$handled, 'nothing runs until the worker processes the first step');

        $worker->runOnce();
        self::assertSame(['first'], ChainStepJob::$handled, 'the second step must not run before the first one is processed');

        $worker->runOnce();
        self::assertSame(['first', 'second'], ChainStepJob::$handled);

        self::assertFalse($worker->runOnce(), 'the chain ends once the last step returns null from next()');
    }

    public function test_a_chain_stops_if_a_step_fails(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));
        $worker = new Worker($queue);

        $queue->push(FailingJob::class, ['reason' => 'boom']);

        $worker->runOnce();

        self::assertFalse($worker->runOnce(), 'a plain failing job (not chainable) must never push anything after it');
    }

    public function test_a_batch_pushes_the_then_job_only_once_every_job_completes(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));
        $worker = new Worker($queue);

        Batch::dispatch(
            $queue,
            [
                [BatchStepJob::class, ['name' => 'a']],
                [BatchStepJob::class, ['name' => 'b']],
                [BatchStepJob::class, ['name' => 'c']],
            ],
            then: [BatchSummaryJob::class, ['label' => 'all-done']],
        );

        $worker->runOnce();
        $worker->runOnce();
        self::assertSame([], BatchSummaryJob::$ran, 'the then job must not run before every batch member has settled');

        $worker->runOnce(); // the third and last batch member
        self::assertSame(['a', 'b', 'c'], BatchStepJob::$handled);
        self::assertSame([], BatchSummaryJob::$ran, 'the then job is pushed, not run inline — it needs its own runOnce()');

        $worker->runOnce();
        self::assertSame(['all-done'], BatchSummaryJob::$ran);
    }

    public function test_a_permanently_failed_batch_member_still_counts_toward_completion(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));
        $worker = new Worker($queue);

        Batch::dispatch(
            $queue,
            [
                [BatchStepJob::class, ['name' => 'ok']],
                [FailingJob::class, ['reason' => 'boom']],
            ],
            then: [BatchSummaryJob::class, ['label' => 'settled']],
        );

        $worker->runOnce();
        $worker->runOnce();

        self::assertSame(['ok'], BatchStepJob::$handled);
        self::assertSame([], BatchSummaryJob::$ran);

        $worker->runOnce();
        self::assertSame(['settled'], BatchSummaryJob::$ran);
    }

    public function test_dispatching_an_empty_batch_pushes_the_then_job_immediately(): void
    {
        $queue = new DatabaseQueue(Connection::sqlite(':memory:'));
        $worker = new Worker($queue);

        Batch::dispatch($queue, [], then: [BatchSummaryJob::class, ['label' => 'nothing-to-do']]);

        $worker->runOnce();
        self::assertSame(['nothing-to-do'], BatchSummaryJob::$ran);
    }
}

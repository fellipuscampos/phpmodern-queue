<?php

declare(strict_types=1);

namespace PhpModern\Queue;

use InvalidArgumentException;
use JsonException;
use RuntimeException;

/**
 * The Redis-backed twin of DatabaseQueue, implementing the exact same
 * BatchableQueue contract so Worker never has to know which one it's
 * running against. Built on RespClient (a hand-rolled RESP client, not
 * ext-redis) — see its own docblock for why.
 *
 * Reliable-queue pattern: push() does LPUSH onto a "pending" list, pop()
 * does RPOPLPUSH pending→reserved (one atomic move, so two workers can
 * never grab the same job — no compare-and-swap needed the way
 * DatabaseQueue's UPDATE...WHERE status='pending' trick is, Redis's
 * RPOPLPUSH already is one). A retry's backoff delay is a ZSET scored by
 * the unix timestamp it becomes available again — pop() sweeps due entries
 * back onto the pending list before trying to pop, mirroring what
 * DatabaseQueue's `available_at <= now` WHERE clause does in SQL.
 */
final class RedisQueue implements BatchableQueue
{
    private readonly RespClient $client;

    public function __construct(
        string $host = '127.0.0.1',
        int $port = 6379,
        private readonly string $prefix = 'phpmodern_jobs',
        ?RespClient $client = null,
    ) {
        if ($this->prefix === '') {
            throw new InvalidArgumentException('RedisQueue prefix must not be empty.');
        }

        $this->client = $client ?? new RespClient($host, $port);
    }

    public function push(string $jobClass, array $payload = [], ?string $batchId = null): int
    {
        try {
            $encodedPayload = json_encode($payload, JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw new InvalidArgumentException('Job payload must be JSON-encodable.', previous: $exception);
        }

        $id = (int) $this->client->command('INCR', "{$this->prefix}:counter");

        $this->client->command(
            'HSET',
            $this->jobKey($id),
            'job_class',
            $jobClass,
            'payload',
            $encodedPayload,
            'status',
            'pending',
            'attempts',
            '0',
            'batch_id',
            $batchId ?? '',
        );
        $this->client->command('LPUSH', $this->pendingKey(), (string) $id);

        return $id;
    }

    public function pop(): ?QueuedJob
    {
        $this->releaseDueDelayedJobs();

        $id = $this->client->command('RPOPLPUSH', $this->pendingKey(), $this->reservedKey());

        if ($id === null) {
            return null;
        }

        $id = (int) $id;
        $hash = $this->hgetall($this->jobKey($id));

        if ($hash === []) {
            return null; // job hash vanished (e.g. deleted concurrently) — nothing usable to return
        }

        /** @var array<string, mixed> $payload */
        $payload = json_decode($hash['payload'] ?? '{}', true) ?? [];
        $batchId = ($hash['batch_id'] ?? '') !== '' ? $hash['batch_id'] : null;

        return new QueuedJob($id, $hash['job_class'] ?? '', $payload, (int) ($hash['attempts'] ?? 0), $batchId);
    }

    public function delete(int $id): void
    {
        $this->client->command('LREM', $this->reservedKey(), '0', (string) $id);
        $this->client->command('DEL', $this->jobKey($id));
    }

    public function markFailed(int $id, string $error, ?int $attempts = null): void
    {
        $this->client->command('LREM', $this->reservedKey(), '0', (string) $id);

        $fields = ['status', 'failed', 'error', $error];

        if ($attempts !== null) {
            $fields[] = 'attempts';
            $fields[] = (string) $attempts;
        }

        $this->client->command('HSET', $this->jobKey($id), ...$fields);
    }

    public function release(int $id, int $attempts, string $availableAt): void
    {
        $this->client->command('LREM', $this->reservedKey(), '0', (string) $id);
        $this->client->command('HSET', $this->jobKey($id), 'status', 'pending', 'attempts', (string) $attempts);

        $timestamp = strtotime($availableAt);

        if ($timestamp === false) {
            throw new InvalidArgumentException("Invalid availableAt datetime: {$availableAt}");
        }

        $this->client->command('ZADD', $this->delayedKey(), (string) $timestamp, (string) $id);
    }

    public function createBatch(string $batchId, int $total, ?array $then): void
    {
        $fields = ['total', (string) $total, 'completed', '0'];

        if ($then !== null) {
            try {
                $thenPayload = json_encode($then[1], JSON_THROW_ON_ERROR);
            } catch (JsonException $exception) {
                throw new InvalidArgumentException('Batch "then" job payload must be JSON-encodable.', previous: $exception);
            }

            $fields[] = 'then_job_class';
            $fields[] = $then[0];
            $fields[] = 'then_payload';
            $fields[] = $thenPayload;
        }

        $this->client->command('HSET', $this->batchKey($batchId), ...$fields);
    }

    public function recordBatchCompletion(string $batchId): array
    {
        $completed = (int) $this->client->command('HINCRBY', $this->batchKey($batchId), 'completed', '1');
        $total = $this->client->command('HGET', $this->batchKey($batchId), 'total');

        if ($total === null) {
            throw new RuntimeException("Unknown batch: {$batchId}");
        }

        return ['completed' => $completed, 'total' => (int) $total];
    }

    public function batchThen(string $batchId): ?array
    {
        $hash = $this->hgetall($this->batchKey($batchId));
        $jobClass = $hash['then_job_class'] ?? null;

        if ($jobClass === null || $jobClass === '') {
            return null;
        }

        /** @var array<string, mixed> $payload */
        $payload = json_decode($hash['then_payload'] ?? '{}', true) ?? [];

        return [self::assertJobClass($jobClass), $payload];
    }

    public function deleteBatch(string $batchId): void
    {
        $this->client->command('DEL', $this->batchKey($batchId));
    }

    /**
     * A stored "then" job class name is only a plain string once it's come
     * back from Redis — this is where that boundary gets checked, the same
     * way any other value crossing in from storage would be.
     *
     * @return class-string<Job>
     */
    private static function assertJobClass(string $class): string
    {
        if (!is_a($class, Job::class, true)) {
            throw new RuntimeException("Stored batch \"then\" class does not implement Job: {$class}");
        }

        return $class;
    }

    private function releaseDueDelayedJobs(): void
    {
        $due = $this->client->command('ZRANGEBYSCORE', $this->delayedKey(), '-inf', (string) time());

        if (!is_array($due)) {
            return;
        }

        foreach ($due as $id) {
            if (!is_string($id)) {
                continue;
            }

            $this->client->command('ZREM', $this->delayedKey(), $id);
            $this->client->command('LPUSH', $this->pendingKey(), $id);
        }
    }

    /** @return array<string, string> */
    private function hgetall(string $key): array
    {
        $flat = $this->client->command('HGETALL', $key);

        if (!is_array($flat)) {
            return [];
        }

        $result = [];

        for ($i = 0; $i + 1 < count($flat); $i += 2) {
            if (is_string($flat[$i]) && is_string($flat[$i + 1])) {
                $result[$flat[$i]] = $flat[$i + 1];
            }
        }

        return $result;
    }

    private function jobKey(int $id): string
    {
        return "{$this->prefix}:job:{$id}";
    }

    private function batchKey(string $batchId): string
    {
        return "{$this->prefix}:batch:{$batchId}";
    }

    private function pendingKey(): string
    {
        return "{$this->prefix}:pending";
    }

    private function reservedKey(): string
    {
        return "{$this->prefix}:reserved";
    }

    private function delayedKey(): string
    {
        return "{$this->prefix}:delayed";
    }
}

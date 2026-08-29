<?php

declare(strict_types=1);

namespace PhpModern\Queue;

use InvalidArgumentException;
use JsonException;
use PhpModern\Orm\Connection;

/**
 * Minimal database-backed queue: push a job from any request (FPM included
 * — no persistent connection needed on the push side), a standalone worker
 * daemon (see bin/worker.php) pops and runs them. No retry/backoff policy
 * in v1 — a failed job is marked 'failed' and left for inspection, not
 * automatically requeued.
 */
final class DatabaseQueue
{
    public function __construct(
        private readonly Connection $connection,
        private readonly string $table = 'phpmodern_jobs',
    ) {
        self::assertValidIdentifier($this->table);

        $this->connection->pdo()->exec(
            "CREATE TABLE IF NOT EXISTS {$this->table} (
                id INTEGER PRIMARY KEY,
                job_class TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                created_at TEXT NOT NULL,
                reserved_at TEXT
            )",
        );
    }

    /**
     * @param class-string<Job> $jobClass
     * @param array<string, mixed> $payload
     */
    public function push(string $jobClass, array $payload = []): int
    {
        try {
            $encodedPayload = json_encode($payload, JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw new InvalidArgumentException('Job payload must be JSON-encodable.', previous: $exception);
        }

        $statement = $this->connection->pdo()->prepare(
            "INSERT INTO {$this->table} (job_class, payload, status, created_at) VALUES (:job_class, :payload, 'pending', :created_at)",
        );
        $statement->execute([
            'job_class' => $jobClass,
            'payload' => $encodedPayload,
            'created_at' => date('Y-m-d H:i:s'),
        ]);

        return (int) $this->connection->pdo()->lastInsertId();
    }

    /**
     * Reserves and returns the oldest pending job, or null if the queue is
     * empty. Uses a status-guarded UPDATE (not a row lock) to avoid two
     * workers reserving the same job — safe for SQLite's single-writer
     * model and good enough without a distributed locking story.
     */
    public function pop(): ?QueuedJob
    {
        $select = $this->connection->pdo()->prepare(
            "SELECT id, job_class, payload FROM {$this->table} WHERE status = 'pending' ORDER BY id LIMIT 1",
        );
        $select->execute();
        $row = $select->fetch();
        $select->closeCursor();

        if ($row === false) {
            return null;
        }

        $id = (int) $row['id'];

        $update = $this->connection->pdo()->prepare(
            "UPDATE {$this->table} SET status = 'reserved', reserved_at = :reserved_at WHERE id = :id AND status = 'pending'",
        );
        $update->execute(['reserved_at' => date('Y-m-d H:i:s'), 'id' => $id]);

        if ($update->rowCount() === 0) {
            return null; // another worker reserved it between our SELECT and UPDATE
        }

        /** @var array<string, mixed> $payload */
        $payload = json_decode((string) $row['payload'], true) ?? [];

        return new QueuedJob($id, (string) $row['job_class'], $payload);
    }

    public function delete(int $id): void
    {
        $statement = $this->connection->pdo()->prepare("DELETE FROM {$this->table} WHERE id = :id");
        $statement->execute(['id' => $id]);
    }

    public function markFailed(int $id, string $error): void
    {
        $statement = $this->connection->pdo()->prepare(
            "UPDATE {$this->table} SET status = 'failed', error = :error WHERE id = :id",
        );
        $statement->execute(['error' => $error, 'id' => $id]);
    }

    private static function assertValidIdentifier(string $identifier): void
    {
        if (preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $identifier) !== 1) {
            throw new InvalidArgumentException("Invalid table identifier: {$identifier}");
        }
    }
}

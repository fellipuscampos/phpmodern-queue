<?php

declare(strict_types=1);

namespace PhpModern\Queue;

use InvalidArgumentException;
use JsonException;
use PhpModern\Orm\Connection;

/**
 * Minimal database-backed queue: push a job from any request (FPM included
 * — no persistent connection needed on the push side), a standalone worker
 * daemon (see bin/worker.php) pops and runs them.
 *
 * A job whose class implements RetryableJob gets automatic retry with
 * exponential backoff instead of being marked failed on its first
 * exception — see Worker::handleFailure(). `available_at` is what makes a
 * retried job invisible to pop() until its backoff delay has passed;
 * `attempts` is how Worker knows how many tries a job has already had.
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
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at TEXT,
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
     * Reserves and returns the oldest pending, currently-available job, or
     * null if the queue is empty (or every pending job is still waiting out
     * its retry backoff). Uses a status-guarded UPDATE (not a row lock) to
     * avoid two workers reserving the same job — a plain compare-and-swap,
     * safe for any of SQLite/MySQL/Postgres, not a SQLite-specific trick.
     */
    public function pop(): ?QueuedJob
    {
        $now = date('Y-m-d H:i:s');

        $select = $this->connection->pdo()->prepare(
            "SELECT id, job_class, payload, attempts FROM {$this->table}
             WHERE status = 'pending' AND (available_at IS NULL OR available_at <= :now)
             ORDER BY id LIMIT 1",
        );
        $select->execute(['now' => $now]);
        $row = $select->fetch();
        $select->closeCursor();

        if ($row === false) {
            return null;
        }

        $id = (int) $row['id'];

        $update = $this->connection->pdo()->prepare(
            "UPDATE {$this->table} SET status = 'reserved', reserved_at = :reserved_at WHERE id = :id AND status = 'pending'",
        );
        $update->execute(['reserved_at' => $now, 'id' => $id]);

        if ($update->rowCount() === 0) {
            return null; // another worker reserved it between our SELECT and UPDATE
        }

        /** @var array<string, mixed> $payload */
        $payload = json_decode((string) $row['payload'], true) ?? [];

        return new QueuedJob($id, (string) $row['job_class'], $payload, (int) $row['attempts']);
    }

    public function delete(int $id): void
    {
        $statement = $this->connection->pdo()->prepare("DELETE FROM {$this->table} WHERE id = :id");
        $statement->execute(['id' => $id]);
    }

    /**
     * $attempts is optional (and left untouched when omitted) purely so a
     * caller marking a job failed outside the normal retry flow — an admin
     * action, a test — isn't forced to know or care how many attempts it
     * had; Worker itself always passes the real count.
     */
    public function markFailed(int $id, string $error, ?int $attempts = null): void
    {
        if ($attempts === null) {
            $statement = $this->connection->pdo()->prepare(
                "UPDATE {$this->table} SET status = 'failed', error = :error WHERE id = :id",
            );
            $statement->execute(['error' => $error, 'id' => $id]);

            return;
        }

        $statement = $this->connection->pdo()->prepare(
            "UPDATE {$this->table} SET status = 'failed', error = :error, attempts = :attempts WHERE id = :id",
        );
        $statement->execute(['error' => $error, 'attempts' => $attempts, 'id' => $id]);
    }

    /**
     * Puts a job back into 'pending', invisible to pop() until $availableAt,
     * with $attempts recorded — the retry half of a RetryableJob's failure,
     * as opposed to markFailed()'s permanent failure.
     */
    public function release(int $id, int $attempts, string $availableAt): void
    {
        $statement = $this->connection->pdo()->prepare(
            "UPDATE {$this->table} SET status = 'pending', attempts = :attempts, available_at = :available_at, reserved_at = NULL WHERE id = :id",
        );
        $statement->execute(['attempts' => $attempts, 'available_at' => $availableAt, 'id' => $id]);
    }

    private static function assertValidIdentifier(string $identifier): void
    {
        if (preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $identifier) !== 1) {
            throw new InvalidArgumentException("Invalid table identifier: {$identifier}");
        }
    }
}

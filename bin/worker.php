#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * Composer path repositories install packages as junctions/symlinks, so
 * __DIR__ here can resolve to this file's *physical* location inside the
 * phpmodern monorepo rather than the consuming project's own vendor tree —
 * an upward search from __DIR__ alone would find OUR autoloader instead of
 * the app's, silently hiding the app's own job classes. Searching from the
 * caller's cwd first (where the app actually lives) avoids that.
 */
function phpmodern_find_upwards(string $startDir, string $relative): ?string
{
    $dir = $startDir;

    for ($i = 0; $i < 10; $i++) {
        $candidate = $dir . DIRECTORY_SEPARATOR . $relative;
        if (is_file($candidate)) {
            return $candidate;
        }

        $parent = dirname($dir);
        if ($parent === $dir) {
            break;
        }

        $dir = $parent;
    }

    return null;
}

$autoload = phpmodern_find_upwards(getcwd(), 'vendor/autoload.php');

if ($autoload === null) {
    foreach ([__DIR__ . '/../../../../vendor/autoload.php', __DIR__ . '/../vendor/autoload.php'] as $candidate) {
        if (is_file($candidate)) {
            $autoload = $candidate;

            break;
        }
    }
}

if ($autoload === null) {
    fwrite(STDERR, "Could not locate vendor/autoload.php — run this from your project (or run composer install first).\n");
    exit(1);
}

require $autoload;

use PhpModern\Config\Config;
use PhpModern\Config\Env;
use PhpModern\Orm\Connection;
use PhpModern\Queue\DatabaseQueue;
use PhpModern\Queue\RedisQueue;
use PhpModern\Queue\Worker;

Env::load(getcwd() . '/.env');

$dsn = $argv[1] ?? Config::string('DATABASE_URL') ?? Config::string('QUEUE_URL');

if ($dsn === null) {
    fwrite(STDERR, "Usage: worker.php <dsn-or-redis-url>   (or set DATABASE_URL / QUEUE_URL)\n");
    exit(1);
}

// A `redis://host:port` URL selects RedisQueue; anything else is a plain
// PDO DSN for DatabaseQueue, exactly as before this option existed.
$redisUrl = parse_url($dsn);
$driver = ($redisUrl !== false && ($redisUrl['scheme'] ?? '') === 'redis')
    ? new RedisQueue($redisUrl['host'] ?? '127.0.0.1', (int) ($redisUrl['port'] ?? 6379))
    : new DatabaseQueue(new Connection($dsn));

fwrite(STDOUT, "phpmodern queue worker started against {$dsn}\n");

(new Worker($driver))->run();

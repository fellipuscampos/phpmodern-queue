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

use PhpModern\Orm\Connection;
use PhpModern\Queue\DatabaseQueue;
use PhpModern\Queue\Worker;

$dsn = $argv[1] ?? (getenv('DATABASE_URL') ?: null);

if ($dsn === null) {
    fwrite(STDERR, "Usage: worker.php <dsn>   (or set the DATABASE_URL environment variable)\n");
    exit(1);
}

fwrite(STDOUT, "phpmodern queue worker started against {$dsn}\n");

(new Worker(new DatabaseQueue(new Connection($dsn))))->run();

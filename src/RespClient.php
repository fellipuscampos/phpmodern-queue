<?php

declare(strict_types=1);

namespace PhpModern\Queue;

use RuntimeException;

/**
 * A minimal RESP (REdis Serialization Protocol, RESP2) client over a plain
 * TCP socket — no ext-redis extension required, the same "hand-build the
 * wire protocol instead of a heavy dependency" choice already made for SMTP
 * (phpmodern/mail), SSE (push-hub), and OAuth2 (phpmodern/auth). Only what
 * RedisQueue actually needs: sending a command as a RESP array of bulk
 * strings, and parsing whichever of the five RESP reply types comes back.
 * A RESP array's elements are themselves int|string|array|null — this
 * PHPStan version doesn't support the self-referential type alias that
 * would express that precisely, so nested arrays are typed `list<mixed>`
 * instead of recursing.
 */
final class RespClient
{
    /** @var resource */
    private $socket;

    public function __construct(string $host = '127.0.0.1', int $port = 6379, float $timeoutSeconds = 5.0)
    {
        $socket = @fsockopen($host, $port, $errorCode, $errorMessage, $timeoutSeconds);

        if ($socket === false) {
            throw new RuntimeException("RespClient: could not connect to {$host}:{$port} ({$errorMessage})");
        }

        $this->socket = $socket;
    }

    public function __destruct()
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }
    }

    /**
     * Sends one command and returns its parsed reply: null (RESP nil),
     * int, string, or a list of those (a RESP array).
     *
     * @return int|string|list<mixed>|null
     */
    public function command(string ...$parts): int|string|array|null
    {
        $encoded = '*' . count($parts) . "\r\n";

        foreach ($parts as $part) {
            $encoded .= '$' . strlen($part) . "\r\n{$part}\r\n";
        }

        $this->write($encoded);

        return $this->readReply();
    }

    private function write(string $data): void
    {
        if (fwrite($this->socket, $data) === false) {
            throw new RuntimeException('RespClient: failed writing to the connection.');
        }
    }

    private function readLine(): string
    {
        $line = fgets($this->socket);

        if ($line === false) {
            throw new RuntimeException('RespClient: connection closed while reading a reply.');
        }

        return rtrim($line, "\r\n");
    }

    /** @return int|string|list<mixed>|null */
    private function readReply(): int|string|array|null
    {
        $line = $this->readLine();
        $type = $line[0] ?? '';
        $rest = substr($line, 1);

        return match ($type) {
            '+' => $rest, // simple string
            '-' => throw new RuntimeException("RespClient: Redis error: {$rest}"),
            ':' => (int) $rest,
            '$' => $this->readBulkString((int) $rest),
            '*' => $this->readArray((int) $rest),
            default => throw new RuntimeException("RespClient: unrecognized reply type byte: " . var_export($type, true)),
        };
    }

    private function readBulkString(int $length): ?string
    {
        if ($length === -1) {
            return null;
        }

        $data = '';

        while (strlen($data) < $length) {
            $chunk = fread($this->socket, max(1, $length - strlen($data)));

            if ($chunk === false || $chunk === '') {
                throw new RuntimeException('RespClient: connection closed while reading a bulk string.');
            }

            $data .= $chunk;
        }

        fread($this->socket, 2); // trailing \r\n

        return $data;
    }

    /** @return list<mixed>|null */
    private function readArray(int $count): ?array
    {
        if ($count === -1) {
            return null;
        }

        $items = [];

        for ($i = 0; $i < $count; $i++) {
            $items[] = $this->readReply();
        }

        return $items;
    }
}

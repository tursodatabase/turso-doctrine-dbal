<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL;

/**
 * Blob transport codec.
 *
 * The libsql_php extension has no typed blob parameter binding: every string
 * value binds as TEXT, and strings which are invalid UTF-8 arrive as NULL.
 * Raw bytes therefore never round-trip through parameter binding. This codec
 * transports binary payloads as an explicit marker-prefixed, double-base64
 * text cell so reads can decode them unambiguously.
 *
 * Wire format:
 *
 *     libsqlblob:<base64(base64(rawBytes))>
 *
 * Written cells written by earlier releases used bare base64(base64(bytes))
 * with no marker; those are still decoded by the legacy heuristic below
 * (with the same Faker-style false-positive protection the Laravel adapter
 * gained in 1.7.0).
 */
final class BlobCodec
{
    public const MARKER = 'libsqlblob:';

    /** Wrap raw bytes for safe storage through the TEXT parameter path. */
    public static function encode(string $bytes): string
    {
        return self::MARKER . base64_encode(base64_encode($bytes));
    }

    /** Decode every row in place (assoc or positional). */
    public static function decodeRows(array $rows): array
    {
        foreach ($rows as $i => $row) {
            if (is_array($row)) {
                $rows[$i] = self::decodeRow($row);
            }
        }

        return $rows;
    }

    /** @return array<string, mixed>|list<mixed> */
    private static function decodeRow(array $row): array
    {
        foreach ($row as $key => $value) {
            if (is_string($value)) {
                $row[$key] = self::decodeCell($value);
            }
        }

        return $row;
    }

    /** @return mixed */
    private static function decodeCell(string $value): mixed
    {
        // Marker transport — explicit and unambiguous. The inner payload is
        // base64(base64(bytes)); bytes may be arbitrary binary, so decode
        // unconditionally (validating printability would reject real blobs).
        if (str_starts_with($value, self::MARKER)) {
            $raw = substr($value, strlen(self::MARKER));
            $inner = base64_decode($raw, true);

            if ($inner !== false) {
                $bytes = base64_decode($inner, true);

                return $bytes !== false ? $bytes : $inner;
            }

            return $value; // malformed marker cell — untouched
        }

        // Plain alphabetic words (e.g. Faker names) are never transported blobs.
        if (preg_match('/^[A-Za-z]{1,24}$/', $value) === 1) {
            return $value;
        }

        // Timestamps / numeric strings stay strings.
        if (self::isValidDateOrTimestamp($value)) {
            return $value;
        }

        // Legacy double-base64 transport (pre-1.3.x data).
        if (self::isBase64($value)) {
            $inner = base64_decode($value, true);

            if ($inner !== false) {
                $decoded = base64_decode($inner, true);

                if ($decoded !== false) {
                    return $decoded;
                }
            }
        }

        // JSON containers stored as text keep their semantic type.
        if (preg_match('/^\s*[\[{]/', $value) === 1) {
            $decoded = json_decode($value, true);

            if (is_array($decoded)) {
                return $decoded;
            }
        }

        return $value;
    }

    private static function isBase64(string $value): bool
    {
        if (! preg_match('/^(?:[A-Za-z0-9+\/]{4})*(?:[A-Za-z0-9+\/]{2}==|[A-Za-z0-9+\/]{3}=)?$/', $value)) {
            return false;
        }

        $decoded = base64_decode($value, true);

        if ($decoded === false) {
            return false;
        }

        if (base64_encode($decoded) !== $value) {
            return false;
        }

        if (! ctype_print($decoded)) {
            return false;
        }

        return true;
    }

    private static function isValidDateOrTimestamp(string $string): bool
    {
        if (is_numeric($string) && (int) $string > 0 && (int) $string <= PHP_INT_MAX) {
            return true;
        }

        if (is_numeric($string) && strlen($string) === 4 && (int) $string >= 1000 && (int) $string <= 9999) {
            return true;
        }

        foreach (['Y-m-d H:i:s', 'Y-m-d'] as $format) {
            $dateTime = \DateTime::createFromFormat($format, $string);

            if ($dateTime && $dateTime->format($format) === $string) {
                return true;
            }
        }

        return false;
    }
}

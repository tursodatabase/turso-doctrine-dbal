<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Turso\Doctrine\DBAL\Connection;

final class QuoteTest extends TestCase
{
    public function testEscapesSingleQuote(): void
    {
        $this->assertSame("o\\'clock", Connection::escapeString("o'clock"));
    }

    public function testEscapesBackslash(): void
    {
        $this->assertSame("a\\\\b", Connection::escapeString('a\\b'));
    }
}

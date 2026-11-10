<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL\Tests\Feature;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Regression guard for src/libsql_php_extension.stubs.php: fails when the
 * installed libsql_php extension exposes any public method the stubs don't
 * document (or vice versa). Runs only when the extension is loaded.
 */
final class StubsCoverageTest extends TestCase
{
    /** @return array<string, array{0: string}> */
    public static function provideStubClasses(): array
    {
        return [
            'LibSQL' => ['LibSQL'],
            'LibSQLStatement' => ['LibSQLStatement'],
            'LibSQLTransaction' => ['LibSQLTransaction'],
            'LibSQLResult' => ['LibSQLResult'],
            'LibSQLIterator' => ['LibSQLIterator'],
        ];
    }

    /** @return array<string, array{0: string}> */
    private static function stubSource(): string
    {
        $source = file_get_contents(__DIR__ . '/../../src/libsql_php_extension.stubs.php');
        self::assertIsString($source);

        return $source;
    }

    protected function setUp(): void
    {
        if (! extension_loaded('libsql_php')) {
            $this->markTestSkipped('libsql_php extension is not loaded');
        }
    }

    /** @dataProvider provideStubClasses */
    #[DataProvider('provideStubClasses')]
    public function testStubsCoverEveryPublicMethod(string $class): void
    {
        $stub = self::stubSource();

        foreach (get_class_methods($class) as $method) {
            $pattern = '/public\s+(?:static\s+)?function ' . preg_quote($method, '/') . '\s*\(/';
            $this->assertMatchesRegularExpression($pattern, $stub, "$class::$method() is not documented in the stubs file");
        }
    }

    /** @dataProvider provideStubClasses */
    #[DataProvider('provideStubClasses')]
    public function testStubMethodsExistAtRuntime(string $class): void
    {
        $stub = self::stubSource();

        $classPattern = '/class ' . preg_quote($class, '/') . '\s*\{(.*?)\n    \}/s';
        $this->assertSame(1, preg_match($classPattern, $stub, $block), "class $class not found as a block in the stubs file");

        preg_match_all('/public\s+(?:static\s+)?function ([A-Za-z_][A-Za-z0-9_]*)\s*\(/', $block[1], $matches);
        $declared = $matches[1];

        $this->assertNotEmpty($declared, "no public methods found for $class in the stubs file");

        $runtimeMethods = get_class_methods($class);
        foreach ($declared as $method) {
            $this->assertContains($method, $runtimeMethods, "stubs declare $class::$method() but the extension does not provide it");
        }
    }
}

<?php

declare(strict_types=1);

namespace App\Command;

use App\Entity\Book;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'proof:run', description: 'End-to-end proof of tursodatabase/turso-doctrine-dbal inside a real Symfony application.')]
final class ProofCommand extends Command
{
    public function __construct(
        private readonly EntityManagerInterface $entityManager,
        private readonly Connection $dbalConnection,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('mode', null, InputOption::VALUE_REQUIRED, 'Connection mode (informational)', 'local');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $checks = [];
        $ok = static function (string $name, bool $cond) use (&$checks): void {
            $checks[$name] = $cond;
        };

        // 1) Raw dbal round-trip
        $ok('dbal_select_1', $this->dbalConnection->executeQuery('SELECT 1')->fetchOne() === 1);

        // 2) ORM persist / flush / find
        $book = new Book();
        $book->title = 'The Pragmatic Programmer';
        $book->price = 42;
        $this->entityManager->persist($book);
        $this->entityManager->flush();
        $found = $this->entityManager->getRepository(Book::class)->find((int) $book->id);
        $ok('orm_persist_flush_find', $found instanceof Book && $found->title === 'The Pragmatic Programmer' && $found->price === 42);

        // 3) ORM DQL
        $dqlCount = (int) $this->entityManager->createQuery('SELECT COUNT(b.id) FROM App\Entity\Book b')->getSingleScalarResult();
        $ok('orm_dql_count', $dqlCount >= 1);

        // 4) Migrations produced the books table — insert via dbal
        $n = $this->dbalConnection->executeStatement("INSERT INTO books (title, price) VALUES ('dbal-raw', 5)");
        $ok('dbal_raw_insert', $n === 1);

        // 5) Transaction rollback
        $this->dbalConnection->beginTransaction();
        $this->dbalConnection->executeStatement("INSERT INTO books (title, price) VALUES ('rollback-probe', 1)");
        $inTx = (int) $this->dbalConnection->executeQuery("SELECT count(*) FROM books WHERE title = 'rollback-probe'")->fetchOne();
        $this->dbalConnection->rollBack();
        $after = (int) $this->dbalConnection->executeQuery("SELECT count(*) FROM books WHERE title = 'rollback-probe'")->fetchOne();
        $ok('tx_rollback', $inTx === 1 && $after === 0);

        // 6) Transaction commit
        $this->dbalConnection->beginTransaction();
        $this->dbalConnection->executeStatement("INSERT INTO books (title, price) VALUES ('commit-probe', 2)");
        $this->dbalConnection->commit();
        $ok('tx_commit', (int) $this->dbalConnection->executeQuery("SELECT count(*) FROM books WHERE title = 'commit-probe'")->fetchOne() === 1);

        // 7) Streaming fetch (never-advancing iterator regression)
        $stmt = $this->dbalConnection->executeQuery('SELECT title FROM books ORDER BY id');
        $titles = [];
        while ($row = $stmt->fetchAssociative()) {
            $titles[] = $row['title'];
        }
        $ok('streaming_fetch_completes', in_array('The Pragmatic Programmer', $titles, true) && count($titles) >= 3);

        // 8) Named + positional params
        $n = $this->dbalConnection->executeStatement('INSERT INTO books (title, price) VALUES (:t, :p)', ['t' => 'named-param', 'p' => 7]);
        $row = $this->dbalConnection->executeQuery('SELECT price FROM books WHERE title = ?', ['named-param'])->fetchOne();
        $ok('named_and_positional_params', $n === 1 && (int) $row === 7);

        // 9) lastInsertId numeric
        $id = (string) $this->dbalConnection->lastInsertId();
        $ok('last_insert_id_numeric', ctype_digit($id) && (int) $id > 0);

        // 10) Server version string
        $ok('server_version', $this->dbalConnection->getServerVersion() !== '');

        $pass = count(array_filter($checks));
        $fail = count($checks) - $pass;
        $output->writeln('PROOF-SYMFONY: ' . json_encode(['ok' => $pass, 'fail' => $fail, 'all_ok' => $fail === 0, 'checks' => $checks]));

        return $fail === 0 ? Command::SUCCESS : Command::FAILURE;
    }
}

<?php

declare(strict_types=1);

namespace App\Migrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20261111000000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Create the books table used by the turso-doctrine-dbal proof.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('CREATE TABLE books (id INTEGER PRIMARY KEY AUTOINCREMENT, title VARCHAR(255) NOT NULL, price INTEGER NOT NULL DEFAULT 0)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP TABLE books');
    }
}

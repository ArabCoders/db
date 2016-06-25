# PDO Extender

This package extends PDO to implement extra method to ease the use of prepared statements,
and add shortcut methods for update/delete/insert/select.

## Install

Via Composer

```bash
$ composer require arabcoders/db
```

## Usage Example.

```php
<?php

require __DIR__ . '/../../autoload.php';

$pdo = new PDO( 'mysql:host=localhost;dbname=dbName', 'dbUser', 'dbPassword' );
$db  = new \arabcoders\db\Db( $pdo );

$insert = $db->insert( 'tableName',[
    'id'    => 1,
    'name'  => 'foo'
]);

//-- get last insert id.
$id = $db->id();

// -- update row.
$update = $db->update( 'tableName',
    [
        'name'  => 'bar'
    ],[
        'id'    => $id
    ]
);

$delete = $db->delete( 'tableName', [
    'id' => $id
]);
```

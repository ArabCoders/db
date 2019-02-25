<?php
/**
 * This file is part of {@see arabcoders\db} package.
 *
 * (c) 2015-2018 Abdulmohsen B. A. A.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace arabcoders\db\Interfaces;

use arabcoders\db\Exceptions\DBException;
use PDO;
use PDOStatement;
use RuntimeException;

/**
 * DB Interface.
 *
 * @author Abdul.Mohsen B. A. A. <admin@arabcoders.org>
 */
interface Db
{
    /**
     * DB Constructor
     *
     * @param PDO   $pdo
     * @param array $options
     */
    public function __construct( PDO $pdo, array $options = [] );

    /**
     * Prepared Query.
     *
     * @param  string $sql     sql string
     * @param  array  $bind    bind parameters.
     * @param  array  $options options.
     *
     * @throws DBException
     *
     * @return PDOStatement
     */
    public function query( string $sql, array $bind = [], array $options = [] ) : PDOStatement;

    /**
     * Turn of Auto commit to start transaction.
     *
     * @return boolean
     * @throws DBException
     */
    public function start() : bool;

    /**
     * Commit transaction.
     *
     * @return boolean
     * @throws DBException
     */
    public function commit() : bool;

    /**
     * Rollback transaction.
     *
     * @throws DBException
     * @return boolean
     */
    public function rollBack() : bool;

    /**
     * Check whether we are in transaction or not.
     *
     * @return bool
     */
    public function inTransaction() : bool;

    /**
     * Delete Rows
     *
     * @param string $table
     * @param array  $conditions
     * @param array  $options
     *
     * @throws RuntimeException  if {@see $conditions} is empty.
     * @throws DBException
     *
     * @return PDOStatement
     */
    public function delete( string $table, array $conditions, array $options = [] ) : PDOStatement;

    /**
     * Select Rows
     *
     * @param string $table
     * @param array  $cols
     * @param array  $conditions
     *  <code>
     *  [
     *  'fieldName1'    => 'val',
     *  'fieldName2     => 'val',
     *  ....            => ....
     *  ]</code>
     * @param array  $options
     *
     * <code>
     *  [
     *  'count'     => (bool) adds "SQL_CALC_FOUND_ROWS" to query string
     *  'groupby'   => (array)[ 'fieldName1','fieldName2', ... ],
     *  'orderby    => (array)[ 'fieldName1' => 'DESC|ASC', 'fieldName2' => 'DESC|ASC', .... ]
     *  'start'     => (int) offset
     *  'limit'     => (int) limit
     *  ]
     *  </code>
     *
     * @throws DBException
     *
     * @return PDOStatement
     */
    public function select( string $table, array $cols = [], array $conditions = [], array $options = [] ) : PDOStatement;

    /**
     * Update Rows
     *
     * @param  string $table      Table.
     * @param  array  $changes    changes
     * @param  array  $conditions conditions
     * @param  array  $options    Options
     *
     * @throws DBException       When query fails.
     * @throws RuntimeException  When {@see $changes} or {@see $conditions} is empty.
     *
     * @return PDOStatement
     */
    public function update( string $table, array $changes, array $conditions, array $options = [] ) : PDOStatement;

    /**
     * Insert Row
     *
     * @param  string $table      Table.
     * @param  array  $conditions Bind parameters.
     * @param  array  $options    Options.
     *
     * @throws RuntimeException  When {@see $conditions} is empty.
     *
     * @return PDOStatement
     */
    public function insert( string $table, array $conditions, array $options = [] ) : PDOStatement;

    /**
     * Upsert Row.
     *
     * @param  string $table      Table.
     * @param  array  $conditions Bind parameters.
     * @param  array  $options    Options.
     *
     * @throws RuntimeException  When {@see $conditions} is empty.
     *
     * @return PDOStatement
     */
    public function upsert( string $table, array $conditions, array $options = [] ) : PDOStatement;

    /**
     * Execute Query Builder.
     *
     * @param QueryBuilder $queryBuilder
     *
     * @return PDOStatement
     */
    public function queryBuilder( QueryBuilder $queryBuilder ) : PDOStatement;

    /**
     * Quote String
     *
     * @param mixed $text parameter to be escaped.
     * @param int   $type parameter type
     *
     * @return string
     */
    public function quote( $text, int $type = \PDO::PARAM_STR ) : string;

    /**
     * Escape String.
     *
     * @param string $text
     *
     * @return string
     */
    public function escape( string $text ) : string;

    /**
     * Set foreign Key Check.
     *
     * @param bool $bool
     *
     * @return Db
     */
    public function setForeignKeyCheck( bool $bool = true ) : Db;

    /**
     * Get Last Query String.
     *
     * @return string
     */
    public function getQueryString() : string;

    /**
     * Get last query bind parameters
     *
     * @return array
     */
    public function getQueryBind() : array;

    /**
     * Get the last Insert id.
     *
     * @param string|null $name Field name for applicable DBs.
     *
     * @return string|int
     */
    public function id( $name = null );

    /**
     * Get SQL_CALC_FOUND_ROWS results.
     *
     * @return int
     */
    public function totalRows() : int;

    /**
     * Close PDO Connection
     *
     * @return boolean
     */
    public function close() : bool;

    /**
     * Get the {@see PDO} connection instance.
     *
     * @return PDO return the PDO Instance.
     */
    public function getPdo() : PDO;

    /**
     * Set Class Variable.
     *
     * @param string $key
     * @param mixed  $value
     *
     * @return Db
     */
    public function setVariable( string $key, $value ) : Db;

    /**
     * get Class Variable.
     *
     * @param string $key
     *
     * @return mixed
     */
    public function getVariable( string $key );

    /**
     * Set {@see PDO} Attribute.
     *
     * @param mixed $key
     * @param mixed $value
     *
     * @return Db
     */
    public function setAttribute( $key, $value ) : Db;

    /**
     * If method does not exists locally, route it to {@see \PDO}.
     *
     * @param string $method
     * @param mixed  $parameters
     *
     * @throws RuntimeException if both {@see $Db} & {@see PDO} does not have the specified method.
     *
     * @return mixed
     */
    public function __call( $method, $parameters );
}
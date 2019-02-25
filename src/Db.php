<?php
/**
 * This file is part of {@see arabcoders\db} package.
 *
 * (c) 2015-2018 Abdulmohsen B. A. A.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace arabcoders\db;

use arabcoders\db\Exceptions\DBException;
use arabcoders\db\Interfaces\Db as DBInterface;
use arabcoders\db\Interfaces\QueryBuilder;
use PDO;
use PDOException;
use PDOStatement;

/**
 * Extends PDO to add extra methods.
 *
 * @author Abdul.Mohsen B. A. A. <admin@arabcoders.org>
 */
class Db implements DBInterface
{
    /**
     * @var PDO
     */
    protected $pdo;

    /**
     * @var string holds last query string.
     */
    protected $queryString;

    /**
     * @var array holds array bind.
     */
    protected $bind = [];

    /**
     * @var string Mysql date
     */
    public const DATE = 'Y-m-d H:i:s';

    /**
     * @var int
     */
    private $deadlockRetries = 3;

    /**
     * @var int dead lock counter.
     */
    private $deadlockTries = 0;

    /**
     * Db constructor.
     *
     * @param PDO   $pdo
     * @param array $options
     */
    public function __construct( PDO $pdo, array $options = [] )
    {
        $this->pdo = $pdo;

        if ( array_key_exists( 'charset', $options ) )
        {
            $this->pdo->exec( 'SET NAMES ' . $options['charset'] );
        }

        if ( !array_key_exists( 'noDefaults', $options ) )
        {
            $this->setDefaultAttributes();
        }

        if ( array_key_exists( 'setAttributes', $options ) && is_array( $options['setAttributes'] ) )
        {
            foreach ( $options['setAttributes'] as $key => $val )
            {
                $this->setAttribute( $key, $val );
            }
        }
    }

    public function query( string $sql, array $bind = [], array $options = [] ) : PDOStatement
    {
        try
        {
            $this->bind = &$bind;

            $this->queryString = $sql;

            $stmt = $this->pdo->prepare( $this->queryString );
            $stmt->execute( $this->bind );
        }
        catch ( PDOException $e )
        {
            if ( $this->deadlockTries < $this->deadlockRetries && '40001' === (string) $e->getCode() )
            {
                $this->deadlockTries++;

                usleep( "{$this->deadlockTries}000000" );

                return $this->query( $this->queryString, $this->bind, $options );
            }

            /** @noinspection PhpUnhandledExceptionInspection */
            throw ( new DBException( $e->getMessage() ) )
                ->setInfo( $this->queryString, $this->bind, $e->errorInfo, $e->getCode() )
                ->setFile( $e->getTrace()[1]['file'] ?? $e->getFile() )
                ->setLine( $e->getTrace()[1]['line'] ?? $e->getLine() )
                ->setOptions( $options );
        }

        return $stmt;
    }

    public function setForeignKeyCheck( bool $bool = true ) : DBInterface
    {
        $this->pdo->exec( 'SET foreign_key_checks = ' . (int) $bool );

        return $this;
    }

    public function start() : bool
    {
        if ( $this->pdo->inTransaction() )
        {
            return false;
        }

        return $this->pdo->beginTransaction();
    }

    public function commit() : bool
    {
        return $this->pdo->commit();
    }

    public function rollBack() : bool
    {
        return $this->pdo->rollBack();
    }

    public function inTransaction() : bool
    {
        return $this->pdo->inTransaction();
    }

    public function delete( string $table, array $conditions, array $options = [] ) : PDOStatement
    {
        if ( empty( $conditions ) )
        {
            throw new \RuntimeException( 'Conditions Parameter is empty, Expecting associative array.' );
        }

        $queryString = 'DELETE FROM ' . $this->escapeIdentifier( $table, true ) . ' WHERE ';

        $keys = [];

        foreach ( $conditions as $i => $v )
        {
            $i = trim( $i );

            $keys[] = sprintf( '%s = :%s', $this->escapeIdentifier( $i, true ), $this->escapeIdentifier( $i ) );
        }

        $queryString .= implode( ' AND ', $keys );

        if ( array_key_exists( 'limit', $options ) )
        {
            $queryString .= ' LIMIT ';
            $queryString .= ' :__acLimit ';

            $conditions['__acLimit'] = $options['limit'];
        }

        $queryString = trim( $queryString );

        return $this->query( $queryString, $conditions, $options );
    }

    public function select( string $table, array $cols = [], array $conditions = [], array $options = [] ) : PDOStatement
    {
        if ( !empty( $cols ) )
        {
            $cols = array_map( function ( $text ) {
                return $this->escapeIdentifier( $text, true );
            }, $cols );

            $col = implode( ', ', $cols );
        }
        else
        {
            $col = '*';
        }

        $count = ( array_key_exists( 'count', $options ) && $options['count'] ) ? 'SQL_CALC_FOUND_ROWS ' : '';

        $queryString = "SELECT {$count}{$col} FROM " . $this->escapeIdentifier( $table, true ) . '  ';

        if ( !empty( $conditions ) )
        {
            $keys = [];

            foreach ( $conditions as $i => $v )
            {
                if ( 0 === strpos( $i, '__' ) )
                {
                    continue;
                }

                $keys[] = sprintf( '%s = :%s', $this->escapeIdentifier( $i, true ), $this->escapeIdentifier( $i ) );
            }

            if ( !empty( $keys ) )
            {
                $queryString .= ' WHERE ' . implode( ' AND ', $keys );
            }
        }

        if ( array_key_exists( 'groupby', $options ) && is_array( $options['groupby'] ) )
        {
            $options['group']['by'] = array_map( function ( $val ) {
                return $this->escapeIdentifier( $val, true );
            }, $options['group']['by'] );

            $queryString .= ' GROUP BY ' . implode( ', ', $options['group'] ) . ' ';
        }

        if ( array_key_exists( 'orderby', $options ) && is_array( $options['orderby'] ) )
        {
            $_cols = [];

            foreach ( $options['orderby'] as $_colName => $_colSort )
            {
                $_colSort = ( 'DESC' === strtoupper( $_colSort ) ) ? ' DESC ' : ' ASC ';

                $_cols[] = $this->escapeIdentifier( $_colName, true ) . ' ' . $_colSort;
            }

            $queryString .= ' ORDER BY ' . implode( ', ', $_cols ) . ' ';
        }

        if ( array_key_exists( 'limit', $options ) )
        {
            if ( array_key_exists( 'start', $options ) )
            {
                $queryString .= ' LIMIT :__acStart, :__acLimit ';

                $conditions['__acStart'] = $options['start'];
                $conditions['__acLimit'] = $options['limit'];
            }
            else
            {
                $queryString .= ' LIMIT :__acLimit ';

                $conditions['__acLimit'] = $options['limit'];
            }
        }

        /**
         * @deprecated will be removed in later 1.x.0 version.
         */
        if ( !empty( $conditions['__orderBy'] ) )
        {
            $queryString .= ' ORDER BY ' . $this->escapeIdentifier( $conditions['__orderBy'], true );

            unset( $conditions['__orderBy'] );

            if ( !empty( $conditions['__orderBySort'] ) )
            {
                $queryString .= ( 'DESC' === strtoupper( $conditions['__orderBySort'] ) ) ? ' DESC ' : ' ASC ';
                unset( $conditions['__orderBySort'] );
            }
        }

        /**
         * @deprecated will be removed in later 1.x.0 version.
         */
        if ( array_key_exists( '__start', $conditions ) && array_key_exists( '__perpage', $conditions ) )
        {
            $queryString .= ' LIMIT :__start, :__perpage ';
        }

        $queryString = trim( $queryString );

        return $this->query( $queryString, $conditions, $options );
    }

    public function update( string $table, array $changes, array $conditions, array $options = [] ) : PDOStatement
    {
        if ( empty( $changes ) )
        {
            throw new \RuntimeException( 'Changes Parameter is empty, expecting associative array.' );
        }

        if ( empty( $conditions ) )
        {
            throw new \RuntimeException( 'Conditions Parameter is empty, Expecting associative array.' );
        }

        $params = [];

        $queryString = 'UPDATE ' . $this->escapeIdentifier( $table, true ) . ' SET ';

        $pre = [];

        foreach ( $changes as $i => $v )
        {
            $params['u_' . $i] = $v;

            $pre[] = sprintf( '%s = :%s', $this->escapeIdentifier( $i, true ), $this->escapeIdentifier( 'u_' . $i ) );
        }

        $queryString .= implode( ', ', $pre );
        $queryString .= ' WHERE ';

        $post = [];

        foreach ( $conditions as $i => $v )
        {
            $params['c_' . $i] = $v;

            $post[] = sprintf( '%s = :%s', $this->escapeIdentifier( $i, true ), $this->escapeIdentifier( 'c_' . $i ) );
        }

        $queryString .= implode( ' AND ', $post );

        if ( array_key_exists( 'limit', $options ) )
        {
            $queryString .= ' LIMIT ';
            $queryString .= ' :__acLimit ';

            $conditions['__acLimit'] = $options['limit'];
        }

        $queryString = trim( $queryString );

        return $this->query( $queryString, $params, $options );
    }

    public function insert( string $table, array $conditions, array $options = [] ) : PDOStatement
    {
        if ( empty( $conditions ) )
        {
            throw new \RuntimeException( 'Conditions Parameter is empty, Expecting associative array.' );
        }

        $queryString = 'INSERT INTO ' . $this->escapeIdentifier( $table, true ) . ' SET ';

        $keys = [];

        foreach ( array_keys( $conditions ) as $i => $v )
        {
            $keys[] = sprintf( '%s = :%s', $this->escapeIdentifier( $v, true ), $this->escapeIdentifier( $v, false ) );
        }

        $queryString .= implode( ', ', $keys );

        $queryString = trim( $queryString );

        return $this->query( $queryString, $conditions, $options );
    }

    public function upsert( string $table, array $conditions, array $options = [] ) : PDOStatement
    {
        if ( empty( $conditions ) )
        {
            throw new \RuntimeException( 'Conditions Parameter is empty, Expecting associative array.' );
        }

        $queryString = 'INSERT INTO  %s 
                            (%s) 
                        VALUES 
                            (%s) 
                        ON DUPLICATE KEY UPDATE 
                            %s
        ';

        $i          = 0;
        $updatePart = $column = $columnBind = $cond = [];

        foreach ( $conditions as $columnName => $columnValue )
        {
            $i++;

            $bind = '__bf0k_' . $i;
            $key  = $this->escapeIdentifier( $columnName, true );

            $cond[$bind] = $columnValue;

            $column[]     = $key;
            $columnBind[] = ':' . $bind;

            $updatePart[] = sprintf( '%s = VALUES(%s)', $key, $key );
        }

        $condition = null;

        $queryString = trim(
            sprintf(
                $queryString,
                $this->escapeIdentifier( $table, true ),
                implode( ', ', $column ),
                implode( ', ', $columnBind ),
                implode( ', ', $updatePart )
            )
        );

        return $this->query( $queryString, $cond, $options );
    }

    public function queryBuilder( QueryBuilder $queryBuilder ) : PDOStatement
    {
        return $this->query( $queryBuilder->getStatement(), $queryBuilder->getPlaceholderValues() ?? [] );
    }

    public function quote( $text, int $type = \PDO::PARAM_STR ) : string
    {
        return $this->pdo->quote( $text, $type );
    }

    public function escape( string $text ) : string
    {
        return mb_substr( $this->pdo->quote( $text ), 1, -1, 'UTF-8' );
    }

    public function getQueryString() : string
    {
        return $this->queryString;
    }

    public function getQueryBind() : array
    {
        return $this->bind;
    }

    public function id( $name = null )
    {
        return $this->pdo->lastInsertId( $name );
    }

    public function totalRows() : int
    {
        return (int) $this->pdo->query( 'SELECT FOUND_ROWS();' )->fetch( \PDO::FETCH_COLUMN );
    }

    public function close() : bool
    {
        $this->pdo = null;

        return true;
    }

    public function getPdo() : PDO
    {
        return $this->pdo;
    }

    public function setVariable( string $key, $value ) : DBInterface
    {
        $this->{$key} = $value;

        return $this;
    }

    public function getVariable( string $key )
    {
        return property_exists( $this, $key ) ? $this->{$key} : null;
    }

    public function setAttribute( $key, $value ) : DBInterface
    {
        $this->pdo->setAttribute( $key, $value );

        return $this;
    }

    public function __call( $method, $parameters )
    {
        $isCallable = [ $this->pdo, $method ];

        if ( method_exists( $this->pdo, $method ) )
        {
            return call_user_func_array( $isCallable, $parameters );
        }

        throw new \RuntimeException( sprintf( '%s->%s() does not exists.', __CLASS__, $method ) );
    }

    /**
     * set Default Attributes.
     *
     * @return DBInterface
     */
    private function setDefaultAttributes() : DBInterface
    {
        $this->pdo->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
        $this->pdo->setAttribute( \PDO::ATTR_DEFAULT_FETCH_MODE, \PDO::FETCH_ASSOC );
        $this->pdo->setAttribute( \PDO::ATTR_EMULATE_PREPARES, false );
        $this->pdo->setAttribute( \PDO::ATTR_STRINGIFY_FETCHES, false );

        return $this;
    }

    /**
     * Create Binds for Array Elements.
     *
     * @param array $cols
     *
     * @throws \InvalidArgumentException
     *
     * @return array
     */
    public function inArrayBind( array $cols ) : array
    {
        if ( empty( $cols ) )
        {
            throw new \InvalidArgumentException( 'First Argument Is empty.' );
        }

        try
        {
            $token = md5( random_bytes( 16 ) );
        }
        catch ( \Exception $e )
        {
            /** @noinspection RandomApiMigrationInspection */
            $token = md5( mt_rand( PHP_INT_MIN, PHP_INT_MAX ) );
        }

        $prefix = 'b' . substr( $token, 0, -( strlen( $token ) - 4 ) ) . 'd';

        $i = 0;

        $bind = [];

        foreach ( $cols as $key => $value )
        {
            $i++;

            $bind[$prefix . $i] = $value;
        }

        return [
            'bind'        => $bind,
            'queryString' => ':' . implode( ', :', array_keys( $bind ) )
        ];
    }

    /**
     * Make sure only valid characters make it in column/table names
     *
     * @see https://stackoverflow.com/questions/10573922/what-does-the-sql-standard-say-about-usage-of-backtick
     *
     * @param string $text  table or column name
     * @param bool   $quote certain SQLs escape column names (i.e. mysql with `backticks`)
     *
     * @return string
     */
    public function escapeIdentifier( string $text, bool $quote = false ) : string
    {
        // table or column has to be valid ASCII name.
        // this is opinionated but we only allow [a-zA-Z0-9_] in column/table name.
        if ( !\preg_match( '#\w#', $text ) )
        {
            throw new \RuntimeException( sprintf( 'Invalid identifier "%s": Column/table must be valid ASCII code.', $text ) );
        }

        // The first character cannot be [0-9]:
        if ( \preg_match( '/^\d/', $text ) )
        {
            throw new \RuntimeException( sprintf( 'Invalid identifier "%s": Must begin with a letter or underscore.', $text ) );
        }

        return $quote ? '`' . $text . '`' : $text;
    }
}
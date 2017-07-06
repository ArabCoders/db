<?php
/**
 * This file is part of {@see arabcoders\db} package.
 *
 * (c) 2015-2017 Abdulmohsen B. A. A..
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace arabcoders\db;

use arabcoders\db\
{
    Exceptions\DBException, Interfaces\Db as DBInterface
};
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
    const DATE = 'Y-m-d H:i:s';

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
            $this->pdo->exec( "SET NAMES " . $options['charset'] );
        }

        if ( !array_key_exists( 'noDefaults', $options ) )
        {
            $this->setDefaultAttributes();
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
            if ( $e->getCode() == '40001' && $this->deadlockTries < $this->deadlockRetries )
            {
                $this->deadlockTries++;

                usleep( "{$this->deadlockTries}000000" );

                return $this->query( $this->queryString, $this->bind, $options );
            }

            throw ( ( new DBException( $e->getMessage() ) )
                ->setInfo( $this->queryString, $this->bind, $e->errorInfo, $e->getCode() ) )
                ->setFile( $e->getTrace()[1]['file'] ?? $e->getFile() )
                ->setLine( $e->getTrace()[1]['line'] ?? $e->getLine() )
                ->setOptions( $options );
        }

        return $stmt;
    }

    public function setForeignKeyCheck( bool $bool = true ) : DBInterface
    {
        $state = ( is_bool( $bool ) && $bool ) ? 1 : 0;

        $this->pdo->query( "SET foreign_key_checks = {$state}" );

        return $this;
    }

    public function start() : bool
    {
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

        $queryString = "DELETE FROM " . $this->escapeIdentifier( $table, true ) . " WHERE ";

        $keys = [];

        foreach ( $conditions as $i => $v )
        {
            $i = trim( $i );

            $keys[] = sprintf( "%s = :%s", $this->escapeIdentifier( $i, true ), $this->escapeIdentifier( $i ) );
        }

        $queryString .= join( ' AND ', $keys );

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
            $cols = array_map( function ( $text )
            {
                return $this->escapeIdentifier( $text, true );
            }, $cols );

            $col = join( ', ', $cols );
        }
        else
        {
            $col = '*';
        }

        $count = ( array_key_exists( 'count', $options ) && $options['count'] ) ? 'SQL_CALC_FOUND_ROWS ' : '';

        $queryString = "SELECT {$count}{$col} FROM " . $this->escapeIdentifier( $table, true ) . "  ";

        if ( !empty( $conditions ) )
        {
            $keys = [];

            foreach ( $conditions as $i => $v )
            {
                if ( stripos( $i, '__' ) === 0 )
                {
                    continue;
                }

                $keys[] = sprintf( "%s = :%s", $this->escapeIdentifier( $i, true ), $this->escapeIdentifier( $i ) );
            }

            if ( !empty( $keys ) )
            {
                $queryString .= " WHERE " . join( ' AND ', $keys );
            }
        }

        if ( array_key_exists( 'groupby', $options ) && is_array( $options['groupby'] ) )
        {
            $options['group']['by'] = array_map( function ( $val )
            {
                return $this->escapeIdentifier( $val, true );
            }, $options['group']['by'] );

            $queryString .= ' GROUP BY ' . join( ', ', $options['group'] ) . ' ';
        }

        if ( array_key_exists( 'orderby', $options ) && is_array( $options['orderby'] ) )
        {
            $_cols = [];

            foreach ( $options['orderby'] as $_colName => $_colSort )
            {
                $_colSort .= ( strtoupper( $_colSort ) == 'DESC' ) ? ' DESC ' : ' ASC ';

                $_cols[] = $this->escapeIdentifier( $_colName, true ) . $_colSort;
            }

            $queryString .= ' ORDER BY ' . join( ', ', $_cols ) . ' ';
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
                $queryString .= ( strtolower( $conditions['__orderBySort'] ) == 'desc' ) ? ' DESC ' : ' ASC ';
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

        $queryString = "UPDATE " . $this->escapeIdentifier( $table, true ) . " SET ";

        $pre = [];

        foreach ( $changes as $i => $v )
        {
            $params['u_' . $i] = $v;

            $pre[] = sprintf( "%s = :%s", $this->escapeIdentifier( $i, true ), $this->escapeIdentifier( 'u_' . $i ) );
        }

        $queryString .= join( ', ', $pre );
        $queryString .= " WHERE ";

        $post = [];

        foreach ( $conditions as $i => $v )
        {
            $params['c_' . $i] = $v;

            $post[] = sprintf( "%s = :%s", $this->escapeIdentifier( $i, true ), $this->escapeIdentifier( 'c_' . $i ) );
        }

        $queryString .= join( ' AND ', $post );

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

        $queryString = "INSERT INTO " . $this->escapeIdentifier( $table, true ) . " SET ";

        $keys = [];

        foreach ( array_keys( $conditions ) as $i => $v )
        {
            $keys[] = sprintf( "%s = :%s", $this->escapeIdentifier( $v, true ), $this->escapeIdentifier( $v, false ) );
        }

        $queryString .= join( ', ', $keys );

        $queryString = trim( $queryString );

        return $this->query( $queryString, $conditions, $options );
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

        $token  = md5( random_bytes( 16 ) );
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
            'queryString' => ':' . join( ', :', array_keys( $bind ) )
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
        $str = \preg_replace( '/[^0-9a-zA-Z_]/', '', $text );

        // The first character cannot be [0-9]:
        if ( \preg_match( '/^[0-9]/', $str ) )
        {
            throw new \RuntimeException( sprintf( "Invalid identifier \"%s\": Must begin with a letter or underscore.", $str ) );
        }

        return ( $quote ) ? '`' . $str . '`' : $str;
    }
}
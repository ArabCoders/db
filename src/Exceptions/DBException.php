<?php
/**
 * This file is part of {@see arabcoders\db} package.
 *
 * (c) 2015-2018 Abdulmohsen B. A. A.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace arabcoders\db\Exceptions;

use arabcoders\db\Interfaces\DBException as DBExceptionInterface;
use PDOException;

/**
 * DB Exception
 *
 * @author Abdul.Mohsen B. A. A. <admin@arabcoders.org>
 */
class DBException extends PDOException implements DBExceptionInterface
{
    /**
     * @var string
     */
    public $queryString = '';

    /**
     * @var array
     */
    public $errorInfo = [];

    /**
     * @var array
     */
    public $bind = [];

    /**
     * @var
     */
    public $options = [];

    /**
     * @param string     $queryString
     * @param array      $bind
     * @param array      $errorInfo
     * @param string|int $errorCode
     *
     * @return $this
     */
    public function setInfo( $queryString, array $bind = [], array $errorInfo = [], $errorCode = 0 ) : self
    {
        $this->queryString = $queryString;
        $this->bind        = $bind;
        $this->errorInfo   = $errorInfo;
        $this->code        = $errorCode;

        return $this;
    }

    public function getQueryString() : string
    {
        return $this->queryString;
    }

    public function getQueryBind() : array
    {
        return $this->bind;
    }

    public function setFile( string $file ) : DBExceptionInterface
    {
        $this->file = $file;

        return $this;
    }

    public function setLine( int $line ) : DBExceptionInterface
    {
        $this->line = $line;

        return $this;
    }

    public function setOptions( array $options ) : DBExceptionInterface
    {
        $this->options = $options;

        return $this;
    }
}
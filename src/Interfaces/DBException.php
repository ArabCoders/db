<?php
/**
 * This file is part of {@see arabcoders\db} package.
 *
 * (c) 2015-2017 Abdulmohsen B. A. A..
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace arabcoders\db\Interfaces;

/**
 * DB Exception Interface.
 *
 * @author Abdul.Mohsen B. A. A. <admin@arabcoders.org>
 */
interface DBException
{
    /**
     * Constructor
     *
     * @param string $queryString
     * @param array  $bind
     * @param array  $errorInfo
     *
     */
    public function setInfo( $queryString, array $bind = [], array $errorInfo = [] );

    /**
     * Return Query String.
     *
     * @return string
     */
    public function getQueryString();

    /**
     * Return Query Parameters.
     *
     * @return array
     */
    public function getQueryBind();

    /**
     * Set File.
     *
     * @param string $file
     *
     * @return DBException
     */
    public function setFile( string $file ) : DBException;

    /**
     * Set Line.
     *
     * @param int $line
     *
     * @return DBException
     */
    public function setLine( int $line ) : DBException;
}
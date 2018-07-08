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

interface QueryBuilder
{
    /**
     * Get Parametized Query.
     *
     * @return string
     */
    public function getStatement() : string;

    /**
     * Get Placeholders Value.
     *
     * @return array
     */
    public function getPlaceholderValues() : array;
}
<?php

/**
 * This file is part of ( alsh3r-old ) project.
 *
 * (c) 2018 ArabCoders Ltd.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace arabcoders\db;

use arabcoders\db\Exceptions\QueryBuilderException;
use \arabcoders\db\Interfaces\QueryBuilder as QueryBuilderInterface;

/**
 * A Dead Simple Query builder.
 *
 * Original Code By Justin Stayton <https://github.com/jstayton/Miner>
 *
 * @author  Abdul.Mohsen B. A. A. <admin@arabcoders.org>
 * @package arabcoders\db
 */
class QueryBuilder implements QueryBuilderInterface
{
    /**
     * INNER JOIN type.
     */
    public const INNER_JOIN = 'INNER JOIN';

    /**
     * LEFT JOIN type.
     */
    public const LEFT_JOIN = 'LEFT JOIN';

    /**
     * RIGHT JOIN type.
     */
    public const RIGHT_JOIN = 'RIGHT JOIN';

    /**
     * AND logical operator.
     */
    public const LOGICAL_AND = 'AND';

    /**
     * OR logical operator.
     */
    public const LOGICAL_OR = 'OR';

    /**
     * Equals comparison operator.
     */
    public const EQUALS = '=';

    /**
     * Not equals comparison operator.
     */
    public const NOT_EQUALS = '!=';

    /**
     * Less than comparison operator.
     */
    public const LESS_THAN = '<';

    /**
     * Less than or equal to comparison operator.
     */
    public const LESS_THAN_OR_EQUAL = '<=';

    /**
     * Greater than comparison operator.
     */
    public const GREATER_THAN = '>';

    /**
     * Greater than or equal to comparison operator.
     */
    public const GREATER_THAN_OR_EQUAL = '>=';

    /**
     * IN comparison operator.
     */
    public const IN = 'IN';

    /**
     * NOT IN comparison operator.
     */
    public const NOT_IN = 'NOT IN';

    /**
     * LIKE comparison operator.
     */
    public const LIKE = 'LIKE';

    /**
     * NOT LIKE comparison operator.
     */
    public const NOT_LIKE = 'NOT LIKE';

    /**
     * ILIKE comparison operator.
     */
    public const ILIKE = 'ILIKE';

    /**
     * REGEXP comparison operator.
     */
    public const REGEX = 'REGEXP';

    /**
     * NOT REGEXP comparison operator.
     */
    public const NOT_REGEX = 'NOT REGEXP';

    /**
     * BETWEEN comparison operator.
     */
    public const BETWEEN = 'BETWEEN';

    /**
     * NOT BETWEEN comparison operator.
     */
    public const NOT_BETWEEN = 'NOT BETWEEN';

    /**
     * IS comparison operator.
     */
    public const IS = 'IS';

    /**
     * IS NOT comparison operator.
     */
    public const IS_NOT = 'IS NOT';

    /**
     * Ascending ORDER BY direction.
     */
    public const ORDER_BY_ASC = 'ASC';

    /**
     * Descending ORDER BY direction.
     */
    public const ORDER_BY_DESC = 'DESC';

    /**
     * Open bracket for grouping criteria.
     */
    public const BRACKET_OPEN = '(';

    /**
     * Closing bracket for grouping criteria.
     */
    public const BRACKET_CLOSE = ')';

    /**
     * Left Most of Identifer Escape.
     */
    public const SQL_QUOTE_LEFT = '`';

    /**
     * Right Most of Identifer Escape.
     */
    public const SQL_QUOTE_RIGHT = '`';

    /**
     * Execution options like DISTINCT and SQL_CALC_FOUND_ROWS.
     *
     * @var array
     */
    private $option;

    /**
     * Columns, tables, and expressions to SELECT from.
     *
     * @var array
     */
    private $select;

    /**
     * Table to INSERT into.
     *
     * @var string
     */
    private $insert;

    /**
     * Table to REPLACE into.
     *
     * @var string
     */
    private $replace;

    /**
     * Table to UPDATE.
     *
     * @var string
     */
    private $update;

    /**
     * Tables to DELETE from, or true if deleting from the FROM table.
     *
     * @var array|true
     */
    private $delete;

    /**
     * Column values to INSERT, UPDATE, or REPLACE.
     *
     * @var array
     */
    private $set;

    /**
     * Table to select FROM.
     *
     * @var array
     */
    private $from;

    /**
     * JOIN tables and ON criteria.
     *
     * @var array
     */
    private $join;

    /**
     * WHERE criteria.
     *
     * @var array
     */
    private $where;

    /**
     * Columns to GROUP BY.
     *
     * @var array
     */
    private $groupBy;

    /**
     * HAVING criteria.
     *
     * @var array
     */
    private $having;

    /**
     * Columns to ORDER BY.
     *
     * @var array
     */
    private $orderBy;

    /**
     * Number of rows to return from offset.
     *
     * @var array
     */
    private $limit;

    /**
     * SET placeholder values.
     *
     * @var array
     */
    private $setPlaceholderValues;

    /**
     * WHERE placeholder values.
     *
     * @var array
     */
    private $wherePlaceholderValues;

    /**
     * HAVING placeholder values.
     *
     * @var array
     */
    private $havingPlaceholderValues;

    /**
     * Constructor.
     */
    public function __construct()
    {
        $this->option  = [];
        $this->select  = [];
        $this->delete  = [];
        $this->set     = [];
        $this->from    = [];
        $this->join    = [];
        $this->where   = [];
        $this->groupBy = [];
        $this->having  = [];
        $this->orderBy = [];
        $this->limit   = [];

        $this->setPlaceholderValues    = [];
        $this->wherePlaceholderValues  = [];
        $this->havingPlaceholderValues = [];
    }

    /**
     * Add an execution option like DISTINCT or SQL_CALC_FOUND_ROWS.
     *
     * @param  string $option execution option to add
     *
     * @return QueryBuilder
     */
    public function option( string $option ) : QueryBuilder
    {
        $this->option[] = $option;

        return $this;
    }

    /**
     * Get the execution options portion of the statement as a string.
     *
     * @param  bool $includeTrailingSpace optional include space after options
     *
     * @return string execution options portion of the statement
     */
    public function getOptionsString( bool $includeTrailingSpace = false ) : string
    {
        $statement = '';

        if ( !$this->option )
        {
            return $statement;
        }

        $statement .= implode( ' ', $this->option );

        if ( $includeTrailingSpace )
        {
            $statement .= ' ';
        }

        return $statement;
    }

    /**
     * Merge this QueryBuilder's execution options into the given QueryBuilder.
     *
     * @param  QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeOptionsInto( QueryBuilder $builder ) : QueryBuilder
    {
        foreach ( $this->option as $option )
        {
            $builder->option( $option );
        }

        return $builder;
    }

    /**
     * Add DISTINCT execution option.
     *
     * @return QueryBuilder
     */
    public function distinct() : QueryBuilder
    {
        return $this->option( 'DISTINCT' );
    }

    /**
     * Add a SELECT column, table, or expression with optional alias.
     *
     * @param  string $column column name, table name, or expression
     * @param  string $alias  optional alias
     *
     * @return QueryBuilder
     */
    public function select( string $column, ?string $alias = null ) : QueryBuilder
    {
        $this->select[$column] = $alias;

        return $this;
    }

    /**
     * Merge this QueryBuilder's SELECT into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeSelectInto( QueryBuilder $builder ) : QueryBuilder
    {
        $this->mergeOptionsInto( $builder );

        foreach ( $this->select as $column => $alias )
        {
            $builder->select( $column, $alias );
        }

        return $builder;
    }

    /**
     * Get the SELECT portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'SELECT' text, default true
     *
     * @return string SELECT portion of the statement
     */
    public function getSelectString( bool $includeText = true ) : string
    {
        $statement = '';

        if ( !$this->select )
        {
            return $statement;
        }

        $statement .= $this->getOptionsString( true );

        foreach ( $this->select as $column => $alias )
        {
            $statement .= $this->escapeIdentifier( $column );

            if ( $alias )
            {
                $statement .= ' AS ' . $this->escapeIdentifier( $alias );
            }

            $statement .= ', ';
        }

        $statement = substr( $statement, 0, -2 );

        if ( $includeText && $statement )
        {
            $statement = 'SELECT ' . $statement;
        }

        return $statement;
    }

    /**
     * Set the INSERT table.
     *
     * @param  string $table INSERT table
     *
     * @return QueryBuilder
     */
    public function insert( string $table ) : QueryBuilder
    {
        $this->insert = $table;

        return $this;
    }

    /**
     * Merge this QueryBuilder's INSERT into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeInsertInto( QueryBuilder $builder ) : QueryBuilder
    {
        $this->mergeOptionsInto( $builder );

        if ( $this->insert )
        {
            $builder->insert( $this->getInsert() );
        }

        return $builder;
    }

    /**
     * Get the INSERT table.
     *
     * @return string INSERT table
     */
    public function getInsert() : ?string
    {
        return $this->insert;
    }

    /**
     * Get the INSERT portion of the statement as a string.
     *
     * @param bool $includeText optional include 'INSERT' text, default true
     *
     * @return string INSERT portion of the statement
     */
    public function getInsertString( bool $includeText = true ) : string
    {
        $statement = '';

        if ( !$this->insert )
        {
            return $statement;
        }

        $statement .= $this->getOptionsString( true );

        $statement .= $this->escapeIdentifier( $this->getInsert() );

        if ( $includeText && $statement )
        {
            $statement = 'INSERT ' . $statement;
        }

        return $statement;
    }

    /**
     * Set the REPLACE table.
     *
     * @param  string $table REPLACE table
     *
     * @return QueryBuilder
     */
    public function replace( string $table ) : QueryBuilder
    {
        $this->replace = $table;

        return $this;
    }

    /**
     * Merge this QueryBuilder's REPLACE into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeReplaceInto( QueryBuilder $builder ) : QueryBuilder
    {
        $this->mergeOptionsInto( $builder );

        if ( $this->replace )
        {
            $builder->replace( $this->getReplace() );
        }

        return $builder;
    }

    /**
     * Get the REPLACE table.
     *
     * @return string REPLACE table
     */
    public function getReplace() : ?string
    {
        return $this->replace;
    }

    /**
     * Get the REPLACE portion of the statement as a string.
     *
     * @param bool $includeText optional include 'REPLACE' text, default true
     *
     * @return string REPLACE portion of the statement
     */
    public function getReplaceString( bool $includeText = true ) : string
    {
        $statement = '';

        if ( !$this->replace )
        {
            return $statement;
        }

        $statement .= $this->getOptionsString( true );

        $statement .= $this->escapeIdentifier( $this->getReplace() );

        if ( $includeText && $statement )
        {
            $statement = 'REPLACE ' . $statement;
        }

        return $statement;
    }

    /**
     * Set the UPDATE table.
     *
     * @param  string $table UPDATE table
     *
     * @return QueryBuilder
     */
    public function update( string $table ) : QueryBuilder
    {
        $this->update = $table;

        return $this;
    }

    /**
     * Merge this QueryBuilder's UPDATE into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeUpdateInto( QueryBuilder $builder ) : QueryBuilder
    {
        $this->mergeOptionsInto( $builder );

        if ( $this->update )
        {
            $builder->update( $this->getUpdate() );
        }

        return $builder;
    }

    /**
     * Get the UPDATE table.
     *
     * @return string UPDATE table
     */
    public function getUpdate() : ?string
    {
        return $this->update;
    }

    /**
     * Get the UPDATE portion of the statement as a string.
     *
     * @param bool $includeText optional include 'UPDATE' text, default true
     *
     * @return string UPDATE portion of the statement
     */
    public function getUpdateString( bool $includeText = true ) : string
    {
        $statement = '';

        if ( !$this->update )
        {
            return $statement;
        }

        $statement .= $this->getOptionsString( true );

        $statement .= $this->escapeIdentifier( $this->getUpdate() );

        // Add any JOINs.
        $statement .= ' ' . $this->getJoinString();

        $statement = rtrim( $statement );

        if ( $includeText && $statement )
        {
            $statement = 'UPDATE ' . $statement;
        }

        return $statement;
    }

    /**
     * Add a table to DELETE from, or false if deleting from the FROM table.
     *
     * @param  string|bool $table optional table name, default false
     *
     * @return QueryBuilder
     */
    public function delete( $table = false ) : QueryBuilder
    {
        if ( $table === false )
        {
            $this->delete = true;
        }
        else
        {
            // Reset the array in case the class variable was previously set to a
            // boolean value.
            if ( !is_array( $this->delete ) )
            {
                $this->delete = [];
            }

            $this->delete[] = $table;
        }

        return $this;
    }

    /**
     * Merge this QueryBuilder's DELETE into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeDeleteInto( QueryBuilder $builder ) : QueryBuilder
    {
        $this->mergeOptionsInto( $builder );

        if ( $this->isDeleteTableFrom() )
        {
            $builder->delete();
        }
        else
        {
            foreach ( $this->delete as $delete )
            {
                $builder->delete( $delete );
            }
        }

        return $builder;
    }

    /**
     * Get the DELETE portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'DELETE' text, default true
     *
     * @return string DELETE portion of the statement
     */
    public function getDeleteString( bool $includeText = true ) : string
    {
        $statement = '';

        if ( !$this->delete && !$this->isDeleteTableFrom() )
        {
            return $statement;
        }

        $statement .= $this->getOptionsString( true );

        if ( is_array( $this->delete ) )
        {
            $statement .= implode( ', ', $this->delete );
        }

        if ( $includeText && ( $statement || $this->isDeleteTableFrom() ) )
        {
            $statement = 'DELETE ' . $statement;

            // Trim in case the table is specified in FROM.
            $statement = trim( $statement );
        }

        return $statement;
    }

    /**
     * Whether the FROM table is the single table to delete from.
     *
     * @return bool whether the delete table is FROM
     */
    private function isDeleteTableFrom() : bool
    {
        return $this->delete === true;
    }

    /**
     * Add one or more column values to INSERT, UPDATE, or REPLACE.
     *
     * @param string|array $column column name or array of columns => values
     * @param mixed|null   $value  optional value for single column
     *
     * @return QueryBuilder
     */
    public function set( string $column, $value = null ) : QueryBuilder
    {
        if ( is_array( $column ) )
        {
            foreach ( $column as $columnName => $columnValue )
            {
                $this->set( $columnName, $columnValue );
            }
        }
        else
        {
            $this->set[] = [
                'column' => $column,
                'value'  => $value,
            ];
        }

        return $this;
    }

    /**
     * Add an array of columns => values to INSERT, UPDATE, or REPLACE.
     *
     * @param array $values columns => values
     *
     * @return QueryBuilder
     */
    public function values( array $values ) : QueryBuilder
    {
        return $this->set( $values );
    }

    /**
     * Merge this QueryBuilder's SET into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeSetInto( QueryBuilder $builder ) : QueryBuilder
    {
        foreach ( $this->set as $set )
        {
            $builder->set( $set['column'], $set['value'] );
        }

        return $builder;
    }

    /**
     * Get the SET portion of the statement as a string.
     *
     * @param bool $includeText optional include 'SET' text, default true
     *
     * @return string SET portion of the statement
     */
    public function getSetString( bool $includeText = true ) : string
    {
        $statement = '';

        $this->setPlaceholderValues = [];

        foreach ( $this->set as $set )
        {
            $statement .= $this->escapeIdentifier( $set['column'] ) . ' ' . self::EQUALS . ' ?, ';

            $this->setPlaceholderValues[] = $set['value'];
        }

        $statement = substr( $statement, 0, -2 );

        if ( $includeText && $statement )
        {
            $statement = 'SET ' . $statement;
        }

        return $statement;
    }

    /**
     * Get the SET placeholder values.
     *
     * @return array SET placeholder values
     */
    public function getSetPlaceholderValues() : array
    {
        return $this->setPlaceholderValues ?? [];
    }

    /**
     * Set the FROM table with optional alias.
     *
     * @param  string $table table name
     * @param  string $alias optional alias
     *
     * @return QueryBuilder
     */
    public function from( string $table, ?string $alias = null ) : QueryBuilder
    {
        $this->from['table'] = $table;
        $this->from['alias'] = $alias;

        return $this;
    }

    /**
     * Merge this QueryBuilder's FROM into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeFromInto( QueryBuilder $builder ) : QueryBuilder
    {
        if ( $this->from )
        {
            $builder->from( $this->getFrom(), $this->getFromAlias() );
        }

        return $builder;
    }

    /**
     * Get the FROM table.
     *
     * @return string FROM table
     */
    public function getFrom() : ?string
    {
        return $this->from['table'];
    }

    /**
     * Get the FROM table alias.
     *
     * @return string FROM table alias
     */
    public function getFromAlias() : ?string
    {
        return $this->from['alias'];
    }

    /**
     * Whether the join table and alias is unique (hasn't already been joined).
     *
     * @param  string $table table name
     * @param  string $alias table alias
     *
     * @return bool whether the join table and alias is unique
     */
    private function isJoinUnique( string $table, ?string $alias ) : ?bool
    {
        foreach ( $this->join as $join )
        {
            if ( $join['table'] === $table && $join['alias'] === $alias )
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Add a JOIN table with optional ON criteria.
     *
     * @param  string       $table    table name
     * @param  string|array $criteria optional ON criteria
     * @param  string       $type     optional type of join, default INNER JOIN
     * @param  string       $alias    optional alias
     *
     * @return QueryBuilder
     */
    public function join( string $table, $criteria = null, string $type = self::INNER_JOIN, ?string $alias = null ) : QueryBuilder
    {
        if ( !$this->isJoinUnique( $table, $alias ) )
        {
            return $this;
        }

        if ( is_string( $criteria ) )
        {
            $criteria = [ $criteria ];
        }

        $this->join[] = [
            'table'    => $table,
            'criteria' => $criteria,
            'type'     => $type,
            'alias'    => $alias
        ];

        return $this;
    }

    /**
     * Add an INNER JOIN table with optional ON criteria.
     *
     * @param  string       $table    table name
     * @param  string|array $criteria optional ON criteria
     * @param  string       $alias    optional alias
     *
     * @return QueryBuilder
     */
    public function innerJoin( string $table, $criteria = null, ?string $alias = null ) : QueryBuilder
    {
        return $this->join( $table, $criteria, self::INNER_JOIN, $alias );
    }

    /**
     * Add a LEFT JOIN table with optional ON criteria.
     *
     * @param  string       $table    table name
     * @param  string|array $criteria optional ON criteria
     * @param  string       $alias    optional alias
     *
     * @return QueryBuilder
     */
    public function leftJoin( string $table, $criteria = null, ?string $alias = null ) : QueryBuilder
    {
        return $this->join( $table, $criteria, self::LEFT_JOIN, $alias );
    }

    /**
     * Add a RIGHT JOIN table with optional ON criteria.
     *
     * @param  string       $table    table name
     * @param  string|array $criteria optional ON criteria
     * @param  string       $alias    optional alias
     *
     * @return QueryBuilder
     */
    public function rightJoin( string $table, $criteria = null, ?string $alias = null ) : QueryBuilder
    {
        return $this->join( $table, $criteria, self::RIGHT_JOIN, $alias );
    }

    /**
     * Merge this QueryBuilder's JOINs into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeJoinInto( QueryBuilder $builder ) : QueryBuilder
    {
        foreach ( $this->join as $join )
        {
            $builder->join( $join['table'], $join['criteria'], $join['type'], $join['alias'] );
        }

        return $builder;
    }

    /**
     * Get an ON criteria string joining the specified table and column to the
     * same column of the previous JOIN or FROM table.
     *
     * @param  int    $joinIndex index of current join
     * @param  string $table     current table name
     * @param  string $column    current column name
     *
     * @return string ON join criteria
     */
    private function getJoinCriteriaUsingPreviousTable( int $joinIndex, string $table, string $column ) : string
    {
        $joinCriteria      = '';
        $previousJoinIndex = $joinIndex - 1;

        // If the previous table is from a JOIN, use that. Otherwise, use the
        // FROM table.
        if ( array_key_exists( $previousJoinIndex, $this->join ) )
        {
            $previousTable = $this->join[$previousJoinIndex]['table'];
        }
        elseif ( $this->isSelect() )
        {
            $previousTable = $this->getFrom();
        }
        elseif ( $this->isUpdate() )
        {
            $previousTable = $this->getUpdate();
        }
        else
        {
            $previousTable = false;
        }

        // In the off chance there is no previous table.
        if ( $previousTable )
        {
            $joinCriteria .= $this->escapeIdentifier( $previousTable ) . '.';
        }

        $joinCriteria .= $this->escapeIdentifier( $column ) .
            ' ' . self::EQUALS .
            ' ' . $this->escapeIdentifier( $table ) .
            '.' . $this->escapeIdentifier( $column );

        return $joinCriteria;
    }

    /**
     * Get the JOIN portion of the statement as a string.
     *
     * @return string JOIN portion of the statement
     */
    public function getJoinString() : string
    {
        $statement = '';

        foreach ( $this->join as $i => $join )
        {
            $statement .= ' ' . $join['type'] . ' ' . $this->escapeIdentifier( $join['table'] );

            if ( $join['alias'] )
            {
                $statement .= ' AS ' . $this->escapeIdentifier( $join['alias'] );
            }

            // Add ON criteria if specified.
            if ( $join['criteria'] )
            {
                $statement .= ' ON ';

                foreach ( $join['criteria'] as $x => $criterion )
                {
                    // Logically join each criterion with AND.
                    if ( 0 !== $x )
                    {
                        $statement .= ' ' . self::LOGICAL_AND . ' ';
                    }

                    // If the criterion does not include an equals sign, assume a
                    // column name and join against the same column from the previous
                    // table.
                    if ( false === strpos( $criterion, '=' ) )
                    {
                        $statement .= $this->getJoinCriteriaUsingPreviousTable( $i, $join['table'], $criterion );
                    }
                    else
                    {
                        $statement .= $criterion;
                    }
                }
            }
        }

        $statement = trim( $statement );

        return $statement;
    }

    /**
     * Get the FROM portion of the statement, including all JOINs, as a string.
     *
     * @param  bool $includeText optional include 'FROM' text, default true
     *
     * @return string FROM portion of the statement
     */
    public function getFromString( bool $includeText = true ) : string
    {
        $statement = '';

        if ( !$this->from )
        {
            return $statement;
        }

        $statement .= $this->escapeIdentifier( $this->getFrom() );

        if ( $this->getFromAlias() )
        {
            $statement .= ' AS ' . $this->escapeIdentifier( $this->getFromAlias() );
        }

        // Add any JOINs.
        $statement .= ' ' . $this->getJoinString();

        $statement = rtrim( $statement );

        if ( $includeText && $statement )
        {
            $statement = 'FROM ' . $statement;
        }

        return $statement;
    }

    /**
     * Add an open bracket for nesting conditions to the specified WHERE or
     * HAVING criteria.
     *
     * @param array  $criteria  WHERE or HAVING criteria
     * @param string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    private function openCriteria( array &$criteria, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        $criteria[] = [
            'bracket'   => self::BRACKET_OPEN,
            'connector' => $connector
        ];

        return $this;
    }

    /**
     * Add a closing bracket for nesting conditions to the specified WHERE or
     * HAVING criteria.
     *
     * @param  array $criteria WHERE or HAVING criteria
     *
     * @return QueryBuilder
     */
    private function closeCriteria( array &$criteria ) : QueryBuilder
    {
        $criteria[] = [
            'bracket'   => self::BRACKET_CLOSE,
            'connector' => null
        ];

        return $this;
    }

    /**
     * Add a condition to the specified WHERE or HAVING criteria.
     *
     * @param  array  $criteria  WHERE or HAVING criteria
     * @param  string $column    column name
     * @param  mixed  $value     value
     * @param  string $operator  optional comparison operator, default =
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    private function criteria( array &$criteria, string $column, $value, string $operator = self::EQUALS, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        $criteria[] = [
            'column'    => $column,
            'value'     => $value,
            'operator'  => $operator,
            'connector' => $connector,
        ];

        return $this;
    }

    /**
     * Add an OR condition to the specified WHERE or HAVING criteria.
     *
     * @param  array  $criteria WHERE or HAVING criteria
     * @param  string $column   column name
     * @param  mixed  $value    value
     * @param  string $operator optional comparison operator, default =
     *
     * @return QueryBuilder
     */
    private function orCriteria( array &$criteria, string $column, $value, string $operator = self::EQUALS ) : QueryBuilder
    {
        return $this->criteria( $criteria, $column, $value, $operator, self::LOGICAL_OR );
    }

    /**
     * Add an IN condition to the specified WHERE or HAVING criteria.
     *
     * @param  array  $criteria  WHERE or HAVING criteria
     * @param  string $column    column name
     * @param  array  $values    values
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    private function criteriaIn( array &$criteria, string $column, array $values, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteria( $criteria, $column, $values, self::IN, $connector );
    }

    /**
     * Add a NOT IN condition to the specified WHERE or HAVING criteria.
     *
     * @param  array  $criteria  WHERE or HAVING criteria
     * @param  string $column    column name
     * @param  array  $values    values
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    private function criteriaNotIn( array &$criteria, string $column, array $values, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteria( $criteria, $column, $values, self::NOT_IN, $connector );
    }

    /**
     * Add a BETWEEN condition to the specified WHERE or HAVING criteria.
     *
     * @param  array  $criteria  WHERE or HAVING criteria
     * @param  string $column    column name
     * @param  mixed  $min       minimum value
     * @param  mixed  $max       maximum value
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    private function criteriaBetween( array &$criteria, string $column, $min, $max, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteria( $criteria, $column, [ $min, $max ], self::BETWEEN, $connector );
    }

    /**
     * Add a NOT BETWEEN condition to the specified WHERE or HAVING criteria.
     *
     * @param  array  $criteria  WHERE or HAVING criteria
     * @param  string $column    column name
     * @param  mixed  $min       minimum value
     * @param  mixed  $max       maximum value
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    private function criteriaNotBetween( array &$criteria, string $column, $min, $max, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteria( $criteria, $column, [ $min, $max ], self::NOT_BETWEEN, $connector );
    }

    /**
     * Get the WHERE or HAVING portion of the statement as a string.
     *
     * @param array $criteria          WHERE or HAVING criteria
     * @param array $placeholderValues optional placeholder values array
     *
     * @return string WHERE or HAVING portion of the statement
     */
    private function getCriteriaString( array &$criteria, array &$placeholderValues = [] ) : string
    {
        $statement         = '';
        $placeholderValues = [];

        $useConnector = false;

        foreach ( $criteria as $i => $criterion )
        {
            if ( array_key_exists( 'bracket', $criterion ) )
            {
                // If an open bracket, include the logical connector.
                if ( 0 === strcmp( $criterion['bracket'], self::BRACKET_OPEN ) )
                {
                    if ( $useConnector )
                    {
                        $statement .= ' ' . $criterion['connector'] . ' ';
                    }

                    $useConnector = false;
                }
                else
                {
                    $useConnector = true;
                }

                $statement .= $criterion['bracket'];
            }
            else
            {
                if ( $useConnector )
                {
                    $statement .= ' ' . $criterion['connector'] . ' ';
                }

                $useConnector = true;

                switch ( $criterion['operator'] )
                {
                    case self::BETWEEN:
                    case self::NOT_BETWEEN:

                        $value = '? ' . self::LOGICAL_AND . ' ?';

                        $placeholderValues[] = $criterion['value'][0];
                        $placeholderValues[] = $criterion['value'][1];

                        break;

                    case self::IN:
                    case self::NOT_IN:

                        $value = self::BRACKET_OPEN . substr( str_repeat( '?, ', count( $criterion['value'] ) ), 0, -2 ) .
                            self::BRACKET_CLOSE;

                        $placeholderValues = array_merge( $placeholderValues, $criterion['value'] );

                        break;

                    case self::IS:
                    case self::IS_NOT:

                        $value = $criterion['value'];

                        break;

                    default:

                        $value = '?';

                        $placeholderValues[] = $criterion['value'];

                        break;
                }

                $statement .= $this->escapeIdentifier( $criterion['column'] ) . ' ' . $criterion['operator'] . ' ' . $value;
            }
        }

        return $statement;
    }

    /**
     * Add an open bracket for nesting WHERE conditions.
     *
     * @param string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function openWhere( string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->openCriteria( $this->where, $connector );
    }

    /**
     * Add a closing bracket for nesting WHERE conditions.
     *
     * @return QueryBuilder
     */
    public function closeWhere() : QueryBuilder
    {
        return $this->closeCriteria( $this->where );
    }

    /**
     * Add a WHERE condition.
     *
     * @param  string $column    column name
     * @param  mixed  $value     value
     * @param  string $operator  optional comparison operator, default =
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function where( string $column, $value, string $operator = self::EQUALS, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteria( $this->where, $column, $value, $operator, $connector );
    }

    /**
     * Add an AND WHERE condition.
     *
     * @param  string $column   colum name
     * @param  mixed  $value    value
     * @param  string $operator optional comparison operator, default =
     *
     * @return QueryBuilder
     */
    public function andWhere( string $column, $value, string $operator = self::EQUALS ) : QueryBuilder
    {
        return $this->criteria( $this->where, $column, $value, $operator, self::LOGICAL_AND );
    }

    /**
     * Add an OR WHERE condition.
     *
     * @param  string $column   colum name
     * @param  mixed  $value    value
     * @param  string $operator optional comparison operator, default =
     *
     * @return QueryBuilder
     */
    public function orWhere( string $column, $value, string $operator = self::EQUALS ) : QueryBuilder
    {
        return $this->orCriteria( $this->where, $column, $value, $operator );
    }

    /**
     * Add an IN WHERE condition.
     *
     * @param  string $column    column name
     * @param  array  $values    values
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function whereIn( string $column, array $values, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteriaIn( $this->where, $column, $values, $connector );
    }

    /**
     * Add a NOT IN WHERE condition.
     *
     * @param  string $column    column name
     * @param  array  $values    values
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function whereNotIn( string $column, array $values, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteriaNotIn( $this->where, $column, $values, $connector );
    }

    /**
     * Add a BETWEEN WHERE condition.
     *
     * @param  string $column    column name
     * @param  mixed  $min       minimum value
     * @param  mixed  $max       maximum value
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function whereBetween( string $column, $min, $max, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteriaBetween( $this->where, $column, $min, $max, $connector );
    }

    /**
     * Add a NOT BETWEEN WHERE condition.
     *
     * @param  string $column    column name
     * @param  mixed  $min       minimum value
     * @param  mixed  $max       maximum value
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function whereNotBetween( string $column, $min, $max, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteriaNotBetween( $this->where, $column, $min, $max, $connector );
    }

    /**
     * Merge this QueryBuilder's WHERE into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeWhereInto( QueryBuilder $builder ) : QueryBuilder
    {
        foreach ( $this->where as $where )
        {
            // Handle open/close brackets differently than other criteria.
            if ( array_key_exists( 'bracket', $where ) )
            {
                if ( 0 === strcmp( $where['bracket'], self::BRACKET_OPEN ) )
                {
                    $builder->openWhere( $where['connector'] );
                }
                else
                {
                    $builder->closeWhere();
                }
            }
            else
            {
                $builder->where( $where['column'], $where['value'], $where['operator'], $where['connector'] );
            }
        }

        return $builder;
    }

    /**
     * Get the WHERE portion of the statement as a string.
     *
     * @param bool $includeText optional include 'WHERE' text, default true
     *
     * @return string WHERE portion of the statement
     */
    public function getWhereString( bool $includeText = true ) : string
    {
        $statement = $this->getCriteriaString( $this->where, $this->wherePlaceholderValues );

        if ( $includeText && $statement )
        {
            $statement = 'WHERE ' . $statement;
        }

        return $statement;
    }

    /**
     * Get the WHERE placeholder values.
     *
     * @return array WHERE placeholder values
     */
    public function getWherePlaceholderValues() : array
    {
        return $this->wherePlaceholderValues;
    }

    /**
     * Add a GROUP BY column.
     *
     * @param  string      $column column name
     * @param  string|null $order  optional order direction, default none
     *
     * @return QueryBuilder
     */
    public function groupBy( string $column, ?string $order = null ) : QueryBuilder
    {
        $this->groupBy[] = [
            'column' => $column,
            'order'  => $order
        ];

        return $this;
    }

    /**
     * Merge this QueryBuilder's GROUP BY into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeGroupByInto( QueryBuilder $builder ) : QueryBuilder
    {
        foreach ( $this->groupBy as $groupBy )
        {
            $builder->groupBy( $groupBy['column'], $groupBy['order'] );
        }

        return $builder;
    }

    /**
     * Get the GROUP BY portion of the statement as a string.
     *
     * @param bool $includeText optional include 'GROUP BY' text, default true
     *
     * @return string GROUP BY portion of the statement
     */
    public function getGroupByString( bool $includeText = true ) : string
    {
        $statement = '';

        foreach ( $this->groupBy as $groupBy )
        {
            $statement .= $this->escapeIdentifier( $groupBy['column'] );

            if ( $groupBy['order'] )
            {
                $statement .= ' ' . $groupBy['order'];
            }

            $statement .= ', ';
        }

        $statement = substr( $statement, 0, -2 );

        if ( $includeText && $statement )
        {
            $statement = 'GROUP BY ' . $statement;
        }

        return $statement;
    }

    /**
     * Add an open bracket for nesting HAVING conditions.
     *
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function openHaving( string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->openCriteria( $this->having, $connector );
    }

    /**
     * Add a closing bracket for nesting HAVING conditions.
     *
     * @return QueryBuilder
     */
    public function closeHaving() : QueryBuilder
    {
        return $this->closeCriteria( $this->having );
    }

    /**
     * Add a HAVING condition.
     *
     * @param string $column    colum name
     * @param mixed  $value     value
     * @param string $operator  optional comparison operator, default =
     * @param string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function having( string $column, $value, string $operator = self::EQUALS, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteria( $this->having, $column, $value, $operator, $connector );
    }

    /**
     * Add an AND HAVING condition.
     *
     * @param  string $column   colum name
     * @param  mixed  $value    value
     * @param  string $operator optional comparison operator, default =
     *
     * @return QueryBuilder
     */
    public function andHaving( string $column, $value, string $operator = self::EQUALS ) : QueryBuilder
    {
        return $this->criteria( $this->having, $column, $value, $operator, self::LOGICAL_AND );
    }

    /**
     * Add an OR HAVING condition.
     *
     * @param  string $column   colum name
     * @param  mixed  $value    value
     * @param  string $operator optional comparison operator, default =
     *
     * @return QueryBuilder
     */
    public function orHaving( string $column, $value, string $operator = self::EQUALS ) : QueryBuilder
    {
        return $this->orCriteria( $this->having, $column, $value, $operator );
    }

    /**
     * Add an IN WHERE condition.
     *
     * @param  string $column    column name
     * @param  array  $values    values
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function havingIn( string $column, array $values, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteriaIn( $this->having, $column, $values, $connector );
    }

    /**
     * Add a NOT IN HAVING condition.
     *
     * @param  string $column    column name
     * @param  array  $values    values
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function havingNotIn( string $column, array $values, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteriaNotIn( $this->having, $column, $values, $connector );
    }

    /**
     * Add a BETWEEN HAVING condition.
     *
     * @param  string $column    column name
     * @param  mixed  $min       minimum value
     * @param  mixed  $max       maximum value
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function havingBetween( string $column, $min, $max, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteriaBetween( $this->having, $column, $min, $max, $connector );
    }

    /**
     * Add a NOT BETWEEN HAVING condition.
     *
     * @param  string $column    column name
     * @param  mixed  $min       minimum value
     * @param  mixed  $max       maximum value
     * @param  string $connector optional logical connector, default AND
     *
     * @return QueryBuilder
     */
    public function havingNotBetween( string $column, $min, $max, string $connector = self::LOGICAL_AND ) : QueryBuilder
    {
        return $this->criteriaNotBetween( $this->having, $column, $min, $max, $connector );
    }

    /**
     * Merge this QueryBuilder's HAVING into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeHavingInto( QueryBuilder $builder ) : QueryBuilder
    {
        foreach ( $this->having as $having )
        {
            // Handle open/close brackets differently than other criteria.
            if ( array_key_exists( 'bracket', $having ) )
            {
                if ( 0 === strcmp( $having['bracket'], self::BRACKET_OPEN ) )
                {
                    $builder->openHaving( $having['connector'] );
                }
                else
                {
                    $builder->closeHaving();
                }
            }
            else
            {
                $builder->having( $having['column'], $having['value'], $having['operator'], $having['connector'] );
            }
        }

        return $builder;
    }

    /**
     * Get the HAVING portion of the statement as a string.
     *
     * @param bool $includeText optional include 'HAVING' text, default true
     *
     * @return string HAVING portion of the statement
     */
    public function getHavingString( bool $includeText = true ) : string
    {
        $statement = $this->getCriteriaString( $this->having, $this->havingPlaceholderValues );

        if ( $includeText && $statement )
        {
            $statement = 'HAVING ' . $statement;
        }

        return $statement;
    }

    /**
     * Get the HAVING placeholder values.
     *
     * @return array HAVING placeholder values
     */
    public function getHavingPlaceholderValues() : array
    {
        return $this->havingPlaceholderValues;
    }

    /**
     * Add a column to ORDER BY.
     *
     * @param  string $column column name
     * @param  string $order  optional order direction, default ASC
     *
     * @return QueryBuilder
     */
    public function orderBy( string $column, string $order = self::ORDER_BY_ASC ) : QueryBuilder
    {
        $this->orderBy[] = [
            'column' => $column,
            'order'  => $order
        ];

        return $this;
    }

    /**
     * Merge this QueryBuilder's ORDER BY into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeOrderByInto( QueryBuilder $builder ) : QueryBuilder
    {
        foreach ( $this->orderBy as $orderBy )
        {
            $builder->orderBy( $orderBy['column'], $orderBy['order'] );
        }

        return $builder;
    }

    /**
     * Get the ORDER BY portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'ORDER BY' text, default true
     *
     * @return string ORDER BY portion of the statement
     */
    public function getOrderByString( bool $includeText = true ) : string
    {
        $statement = '';

        foreach ( $this->orderBy as $orderBy )
        {
            $statement .= $this->escapeIdentifier( $orderBy['column'] ) . ' ' . $orderBy['order'] . ', ';
        }

        $statement = substr( $statement, 0, -2 );

        if ( $includeText && $statement )
        {
            $statement = 'ORDER BY ' . $statement;
        }

        return $statement;
    }

    /**
     * Set the LIMIT on number of rows to return with optional offset.
     *
     * @param int $limit  number of rows to return
     * @param int $offset optional row number to start at, default 0
     *
     * @return QueryBuilder
     */
    public function limit( int $limit, int $offset = 0 ) : QueryBuilder
    {
        $this->limit['limit']  = $limit;
        $this->limit['offset'] = $offset;

        return $this;
    }

    /**
     * Merge this QueryBuilder's LIMIT into the given QueryBuilder.
     *
     * @param QueryBuilder $builder to merge into
     *
     * @return QueryBuilder
     */
    public function mergeLimitInto( QueryBuilder $builder ) : QueryBuilder
    {
        if ( $this->limit )
        {
            $builder->limit( $this->getLimit(), $this->getLimitOffset() );
        }

        return $builder;
    }

    /**
     * Get the LIMIT on number of rows to return.
     *
     * @return int|string LIMIT on number of rows to return
     */
    public function getLimit()
    {
        return $this->limit['limit'];
    }

    /**
     * Get the LIMIT row number to start at.
     *
     * @return int|string LIMIT row number to start at
     */
    public function getLimitOffset()
    {
        return $this->limit['offset'];
    }

    /**
     * Get the LIMIT portion of the statement as a string.
     *
     * @param bool $includeText optional include 'LIMIT' text, default true
     *
     * @return string LIMIT portion of the statement
     */
    public function getLimitString( bool $includeText = true ) : string
    {
        $statement = '';

        if ( !$this->limit )
        {
            return $statement;
        }

        $statement .= (int) $this->limit['limit'];

        if ( $this->limit['offset'] !== 0 )
        {
            $statement .= ' OFFSET ' . (int) $this->limit['offset'];
        }

        if ( $includeText && $statement )
        {
            $statement = 'LIMIT ' . $statement;
        }

        return $statement;
    }

    /**
     * Whether this is a SELECT statement.
     *
     * @return bool whether this is a SELECT statement
     */
    public function isSelect() : bool
    {
        return !empty( $this->select );
    }

    /**
     * Whether this is an INSERT statement.
     *
     * @return bool whether this is an INSERT statement
     */
    public function isInsert() : bool
    {
        return !empty( $this->insert );
    }

    /**
     * Whether this is a REPLACE statement.
     *
     * @return bool whether this is a REPLACE statement
     */
    public function isReplace() : bool
    {
        return !empty( $this->replace );
    }

    /**
     * Whether this is an UPDATE statement.
     *
     * @return bool whether this is an UPDATE statement
     */
    public function isUpdate() : bool
    {
        return !empty( $this->update );
    }

    /**
     * Whether this is a DELETE statement.
     *
     * @return bool whether this is a DELETE statement
     */
    public function isDelete() : bool
    {
        return !empty( $this->delete );
    }

    /**
     * Merge this QueryBuilder into the given QueryBuilder.
     *
     * @param QueryBuilder $builder       to merge into
     * @param bool         $overrideLimit optional override limit, default true
     *
     * @return QueryBuilder
     */
    public function mergeInto( QueryBuilder $builder, bool $overrideLimit = true ) : QueryBuilder
    {
        if ( $this->isSelect() )
        {
            $this->mergeSelectInto( $builder );
            $this->mergeFromInto( $builder );
            $this->mergeJoinInto( $builder );
            $this->mergeWhereInto( $builder );
            $this->mergeGroupByInto( $builder );
            $this->mergeHavingInto( $builder );
            $this->mergeOrderByInto( $builder );

            if ( $overrideLimit )
            {
                $this->mergeLimitInto( $builder );
            }
        }
        elseif ( $this->isInsert() )
        {
            $this->mergeInsertInto( $builder );
            $this->mergeSetInto( $builder );
        }
        elseif ( $this->isReplace() )
        {
            $this->mergeReplaceInto( $builder );
            $this->mergeSetInto( $builder );
        }
        elseif ( $this->isUpdate() )
        {
            $this->mergeUpdateInto( $builder );
            $this->mergeJoinInto( $builder );
            $this->mergeSetInto( $builder );
            $this->mergeWhereInto( $builder );

            // ORDER BY and LIMIT are only applicable when updating a single table.
            if ( !$this->join )
            {
                $this->mergeOrderByInto( $builder );

                if ( $overrideLimit )
                {
                    $this->mergeLimitInto( $builder );
                }
            }
        }
        elseif ( $this->isDelete() )
        {
            $this->mergeDeleteInto( $builder );
            $this->mergeFromInto( $builder );
            $this->mergeJoinInto( $builder );
            $this->mergeWhereInto( $builder );

            // ORDER BY and LIMIT are only applicable when deleting from a single
            // table.
            if ( $this->isDeleteTableFrom() )
            {
                $this->mergeOrderByInto( $builder );

                if ( $overrideLimit )
                {
                    $this->mergeLimitInto( $builder );
                }
            }
        }

        return $builder;
    }

    /**
     * Get the full SELECT statement.
     *
     * @return string full SELECT statement
     */
    private function getSelectStatement() : string
    {
        $statement = '';

        if ( !$this->isSelect() )
        {
            return $statement;
        }

        $statement .= $this->getSelectString();

        if ( $this->from )
        {
            $statement .= ' ' . $this->getFromString();
        }

        if ( $this->where )
        {
            $statement .= ' ' . $this->getWhereString();
        }

        if ( $this->groupBy )
        {
            $statement .= ' ' . $this->getGroupByString();
        }

        if ( $this->having )
        {
            $statement .= ' ' . $this->getHavingString();
        }

        if ( $this->orderBy )
        {
            $statement .= ' ' . $this->getOrderByString();
        }

        if ( $this->limit )
        {
            $statement .= ' ' . $this->getLimitString();
        }

        return $statement;
    }

    /**
     * Get the full INSERT statement.
     *
     * @return string full INSERT statement
     */
    private function getInsertStatement() : string
    {
        $statement = '';

        if ( !$this->isInsert() )
        {
            return $statement;
        }

        $statement .= $this->getInsertString();

        if ( $this->set )
        {
            $statement .= ' ' . $this->getSetString();
        }

        return $statement;
    }

    /**
     * Get the full REPLACE statement.
     *
     * @return string full REPLACE statement
     */
    private function getReplaceStatement() : string
    {
        $statement = '';

        if ( !$this->isReplace() )
        {
            return $statement;
        }

        $statement .= $this->getReplaceString();

        if ( $this->set )
        {
            $statement .= ' ' . $this->getSetString();
        }

        return $statement;
    }

    /**
     * Get the full UPDATE statement.
     *
     * @return string full UPDATE statement
     */
    private function getUpdateStatement() : string
    {
        $statement = '';

        if ( !$this->isUpdate() )
        {
            return $statement;
        }

        $statement .= $this->getUpdateString();

        if ( $this->set )
        {
            $statement .= ' ' . $this->getSetString();
        }

        if ( $this->where )
        {
            $statement .= ' ' . $this->getWhereString();
        }

        // ORDER BY and LIMIT are only applicable when updating a single table.
        if ( !$this->join )
        {
            if ( $this->orderBy )
            {
                $statement .= ' ' . $this->getOrderByString();
            }

            if ( $this->limit )
            {
                $statement .= ' ' . $this->getLimitString();
            }
        }

        return $statement;
    }

    /**
     * Get the full DELETE statement.
     *
     * @return string full DELETE statement
     */
    private function getDeleteStatement() : string
    {
        $statement = '';

        if ( !$this->isDelete() )
        {
            return $statement;
        }

        $statement .= $this->getDeleteString();

        if ( $this->from )
        {
            $statement .= ' ' . $this->getFromString();
        }

        if ( $this->where )
        {
            $statement .= ' ' . $this->getWhereString();
        }

        // ORDER BY and LIMIT are only applicable when deleting from a single
        // table.
        if ( $this->isDeleteTableFrom() )
        {
            if ( $this->orderBy )
            {
                $statement .= ' ' . $this->getOrderByString();
            }

            if ( $this->limit )
            {
                $statement .= ' ' . $this->getLimitString();
            }
        }

        return $statement;
    }

    /**
     * Get the full SQL statement.
     *
     * @return string full SQL statement
     */
    public function getStatement() : string
    {
        $statement = '';

        if ( $this->isSelect() )
        {
            $statement = $this->getSelectStatement();
        }
        elseif ( $this->isInsert() )
        {
            $statement = $this->getInsertStatement();
        }
        elseif ( $this->isReplace() )
        {
            $statement = $this->getReplaceStatement();
        }
        elseif ( $this->isUpdate() )
        {
            $statement = $this->getUpdateStatement();
        }
        elseif ( $this->isDelete() )
        {
            $statement = $this->getDeleteStatement();
        }

        return $statement;
    }

    /**
     * Get all placeholder values (SET, WHERE, and HAVING).
     *
     * @return array all placeholder values
     */
    public function getPlaceholderValues() : array
    {
        return array_merge( $this->getSetPlaceholderValues(), $this->getWherePlaceholderValues(), $this->getHavingPlaceholderValues() );
    }

    /**
     * Make sure only valid characters make it in column/table names
     *
     * @see https://stackoverflow.com/questions/10573922/what-does-the-sql-standard-say-about-usage-of-backtick
     * @see https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
     *
     * @param string $text table or column name
     *
     * @return string
     */
    public function escapeIdentifier( string $text ) : string
    {
        if ( '*' === $text )
        {
            return $text;
        }

        // safe guard aganist double quote.
        $text = str_replace( [ self::SQL_QUOTE_RIGHT, self::SQL_QUOTE_LEFT ], '', $text );

        if ( false !== strpos( $text, '.' ) )
        {
            $string = [];

            foreach ( explode( '.', $text ) as $str )
            {
                $string[] = $this->escapeIdentifier( $str );
            }

            return implode( '.', $string );
        }

        // table or column has to be valid ASCII name.
        // this is opinionated but we only allow [a-zA-Z0-9_] in column/table name.
        if ( !\preg_match( '#\w#', $text ) )
        {
            throw new QueryBuilderException( sprintf( 'Invalid identifier "%s": Column/table must be valid ASCII code.', $text ) );
        }

        // The first character cannot be [0-9]:
        if ( \preg_match( '/^\d/', $text ) )
        {
            throw new QueryBuilderException( sprintf( 'Invalid identifier "%s": Must begin with a letter or underscore.', $text ) );
        }

        return self::SQL_QUOTE_LEFT . $text . self::SQL_QUOTE_RIGHT;
    }
}
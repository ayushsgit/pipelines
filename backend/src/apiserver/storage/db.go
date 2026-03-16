// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"
)

// DB a struct wrapping plain sql library with SQL dialect, to solve any feature
// difference between MySQL, which is used in production, and Sqlite, which is used
// for unit testing.
type DB struct {
	*sql.DB
	SQLDialect
}

// NewDB creates a DB.
func NewDB(db *sql.DB, dialect SQLDialect) *DB {
	return &DB{db, dialect}
}

// SQLDialect abstracts common sql queries which vary in different dialect.
// It is used to bridge the difference between mysql (production) and sqlite
// (test).
type SQLDialect interface {
	GroupConcat(expr string, separator string) string
	Concat(exprs []string, separator string) string
	IsDuplicateError(err error) bool
	SelectForUpdate(query string) string
	Upsert(query string, key string, overwrite bool, columns ...string) string
	UpdateWithJointOrFrom(targetTable, joinTable, setClause, joinClause, whereClause string) string
}

// MySQLDialect implements SQLDialect with mysql dialect implementation.
type MySQLDialect struct{}

func (d MySQLDialect) GroupConcat(expr string, separator string) string {
	var buffer bytes.Buffer
	buffer.WriteString("GROUP_CONCAT(")
	buffer.WriteString(expr)
	if separator != "" {
		buffer.WriteString(fmt.Sprintf(" SEPARATOR \"%s\"", separator))
	}
	buffer.WriteString(")")
	return buffer.String()
}

func (d MySQLDialect) Concat(exprs []string, separator string) string {
	separatorSQL := ","
	if separator != "" {
		separatorSQL = fmt.Sprintf(`,"%s",`, separator)
	}
	return fmt.Sprintf("CONCAT(%s)", strings.Join(exprs, separatorSQL))
}

func (d MySQLDialect) IsDuplicateError(err error) bool {
	sqlError, ok := err.(*mysql.MySQLError)
	return ok && sqlError.Number == mysqlerr.ER_DUP_ENTRY
}

func (d MySQLDialect) UpdateWithJointOrFrom(targetTable, joinTable, setClause, joinClause, whereClause string) string {
	return fmt.Sprintf("UPDATE %s INNER JOIN %s ON %s SET %s WHERE %s", targetTable, joinTable, joinClause, setClause, whereClause)
}

func (d MySQLDialect) SelectForUpdate(query string) string {
	return query + " FOR UPDATE"
}

func (d MySQLDialect) Upsert(query string, key string, overwrite bool, columns ...string) string {
	return fmt.Sprintf("%v ON DUPLICATE KEY UPDATE %v", query, prepareUpdateSuffixMySQL(columns, overwrite))
}

func NewMySQLDialect() MySQLDialect {
	return MySQLDialect{}
}

func prepareUpdateSuffixMySQL(columns []string, overwrite bool) string {
	columnsExtended := make([]string, 0)
	if overwrite {
		for _, c := range columns {
			columnsExtended = append(columnsExtended, fmt.Sprintf("%[1]v=VALUES(%[1]v)", c))
		}
	} else {
		for _, c := range columns {
			columnsExtended = append(columnsExtended, fmt.Sprintf("%[1]v=%[1]v", c))
		}
	}
	return strings.Join(columnsExtended, ",")
}

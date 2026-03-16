//go:build integration
// +build integration

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
	"fmt"
	"strings"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// SQLiteDialect implements SQLDialect with sqlite dialect implementation.
type SQLiteDialect struct{}

func (d SQLiteDialect) GroupConcat(expr string, separator string) string {
	var buffer bytes.Buffer
	buffer.WriteString("GROUP_CONCAT(")
	buffer.WriteString(expr)
	if separator != "" {
		buffer.WriteString(fmt.Sprintf(", \"%s\"", separator))
	}
	buffer.WriteString(")")
	return buffer.String()
}

func (d SQLiteDialect) Concat(exprs []string, separator string) string {
	separatorSQL := "||"
	if separator != "" {
		separatorSQL = fmt.Sprintf(`||"%s"||`, separator)
	}
	return strings.Join(exprs, separatorSQL)
}

func (d SQLiteDialect) SelectForUpdate(query string) string {
	return query
}

func (d SQLiteDialect) Upsert(query string, key string, overwrite bool, columns ...string) string {
	return fmt.Sprintf("%v ON CONFLICT(%v) DO UPDATE SET %v", query, key, prepareUpdateSuffixSQLite(columns, overwrite))
}

func (d SQLiteDialect) IsDuplicateError(err error) bool {
	sqlError, ok := err.(sqlite3.Error)
	return ok && sqlError.Code == sqlite3.ErrConstraint
}

func (d SQLiteDialect) UpdateWithJointOrFrom(targetTable, joinTable, setClause, joinClause, whereClause string) string {
	return fmt.Sprintf("UPDATE %s SET %s FROM %s WHERE %s AND %s", targetTable, setClause, joinTable, joinClause, whereClause)
}

func NewSQLiteDialect() SQLiteDialect {
	return SQLiteDialect{}
}

func prepareUpdateSuffixSQLite(columns []string, overwrite bool) string {
	columnsExtended := make([]string, 0)
	if overwrite {
		for _, c := range columns {
			columnsExtended = append(columnsExtended, fmt.Sprintf("%[1]v=excluded.%[1]v", c))
		}
	} else {
		for _, c := range columns {
			columnsExtended = append(columnsExtended, fmt.Sprintf("%[1]v=%[1]v", c))
		}
	}
	return strings.Join(columnsExtended, ",")
}

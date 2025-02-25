package utils_test

import (
	"strings"
	"testing"

	sqlparser "github.com/kwilteam/kwil-db/parse/sql"
	"github.com/kwilteam/kwil-db/parse/sql/sqlanalyzer/utils"
	"github.com/kwilteam/kwil-db/parse/sql/tree"
	"github.com/stretchr/testify/require"
)

func Test_JoinSearch(t *testing.T) {
	type testcase struct {
		name   string
		stmt   string // must be a select statement
		tables []*tree.RelationTable
	}

	tests := []testcase{
		{
			name:   "simple select",
			stmt:   "SELECT * FROM users",
			tables: tbls("users"),
		},
		{
			name:   "select with joins and aliases",
			stmt:   "SELECT * FROM users AS u INNER JOIN posts AS p ON u.id = p.user_id",
			tables: tbls("users u", "posts p"),
		},
		{
			name: "select with joins and subqueries", // it should not register the subquery as a table
			stmt: `SELECT p.id, p.title
			FROM posts AS p
			INNER JOIN followers AS f ON p.user_id = f.user_id
			INNER JOIN users ON users.id = f.user_id
			INNER JOIN (
				SELECT * FROM SOME_OTHER_TABLE
			) AS l ON l.post_id = p.id
			ORDER BY p.post_date DESC NULLS LAST
			LIMIT 20 OFFSET $offset;`,
			tables: tbls("posts p", "followers f", "users", "l l"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tt.stmt)
			require.NoError(t, err)

			topSelect, ok := stmt.(*tree.SelectStmt)
			require.True(t, ok)
			require.Equal(t, len(topSelect.Stmt.SimpleSelects), 1)

			tbls, err := utils.GetUsedTables(topSelect.Stmt.SimpleSelects[0].From)
			require.NoError(t, err)

			require.EqualValues(t, tt.tables, tbls)
		})
	}
}

func tbls(tables ...string) []*tree.RelationTable {
	// should either be "tablename" OR "tablename alias"
	tbls := make([]*tree.RelationTable, len(tables))
	for i, t := range tables {
		split := strings.Split(t, " ")
		switch len(split) {
		case 1:
			tbls[i] = tbl(split[0])
		case 2:
			tbls[i] = tbl(split[0], split[1])
		default:
			panic("too many aliases")
		}
	}

	return tbls
}

// if alias is empty, the table name is used as the alias
func tbl(name string, alias ...string) *tree.RelationTable {
	if len(alias) == 0 {
		return &tree.RelationTable{
			Name: name,
		}
	}
	if len(alias) > 1 {
		panic("too many aliases")
	}

	return &tree.RelationTable{
		Name:  name,
		Alias: alias[0],
	}
}

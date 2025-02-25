package tree

import (
	"fmt"

	sqlwriter "github.com/kwilteam/kwil-db/parse/sql/tree/sql-writer"
)

// TODO: update this docs to reflect the current state of the code
/*
From the SQLite documentation:
	If the join-operator is "CROSS JOIN", "INNER JOIN", "JOIN" or a comma (",") and there is no ON or USING clause,
	then the result of the join is simply the cartesian product of the left and right-hand datasets.
	If join-operator does have ON or USING clauses, those are handled according to the following bullet points:

	- 	If there is an ON clause then the ON expression is evaluated for each row of the cartesian product as a boolean expression.
		Only rows for which the expression evaluates to true are included from the dataset.

	- 	If there is a USING clause then each of the column names specified must exist in the datasets to both the left
		and right of the join-operator. For each pair of named columns, the expression "lhs.X = rhs.X" is evaluated for
		each row of the cartesian product as a boolean expression. Only rows for which all such expressions evaluates to
		true are included from the result set. When comparing values as a result of a USING clause, the normal rules for
		handling affinities, collation sequences and NULL values in comparisons apply. The column from the dataset on
		the left-hand side of the join-operator is considered to be on the left-hand side of the comparison operator (=)
		for the purposes of collation sequence and affinity precedence.

	- 	For each pair of columns identified by a USING clause, the column from the right-hand dataset is omitted from the joined dataset.
		This is the only difference between a USING clause and its equivalent ON constraint.

	- 	If the NATURAL keyword is in the join-operator then an implicit USING clause is added to the join-constraints.
		The implicit USING clause contains each of the column names that appear in both the left and right-hand input datasets.
		If the left and right-hand input datasets feature no common column names, then the NATURAL keyword has no effect on the
		results of the join. A USING or ON clause may not be added to a join that specifies the NATURAL keyword.

	- 	If the join-operator is a "LEFT JOIN" or "LEFT OUTER JOIN", then after the ON or USING filtering clauses have been applied,
		an extra row is added to the output for each row in the original left-hand input dataset that does not match any row in the
		right-hand dataset. The added rows contain NULL values in the columns that would normally contain values copied from the right-hand input dataset.

	- 	If the join-operator is a "RIGHT JOIN" or "RIGHT OUTER JOIN", then after the ON or USING filtering clauses have been applied,
		an extra row is added to the output for each row in the original right-hand input dataset that does not match any row in the
		left-hand dataset. The added rows contain NULL values in the columns that would normally contain values copied from the left-hand input dataset.

	- 	A "FULL JOIN" or "FULL OUTER JOIN" is a combination of a "LEFT JOIN" and a "RIGHT JOIN". Extra rows of output are added
		for each row in left dataset that matches no rows in the right, and for each row in the right dataset that matches no rows in the left.
		Unmatched columns are filled in with NULL.

	When more than two tables are joined together as part of a FROM clause, the join operations are processed in order from left to right.
	In other words, the FROM clause (A join-op-1 B join-op-2 C) is computed as ((A join-op-1 B) join-op-2 C).
*/

type JoinPredicate struct {
	node

	JoinOperator *JoinOperator
	Table        Relation
	Constraint   Expression
}

func (j *JoinPredicate) Walk(w AstListener) error {
	return run(
		w.EnterJoinPredicate(j),
		walk(w, j.JoinOperator),
		walk(w, j.Table),
		walk(w, j.Constraint),
		w.ExitJoinPredicate(j),
	)
}

func (j *JoinPredicate) ToSQL() string {
	if j.Constraint == nil {
		panic("join 'ON' cannot be nil")
	}
	if j.Constraint.joinable() != joinableStatusValid {
		panic("invalid join constraint")
	}
	if j.JoinOperator == nil {
		panic("join operator cannot be nil")
	}
	if j.Table == nil {
		panic("join table cannot be nil")
	}

	stmt := sqlwriter.NewWriter()
	stmt.WriteString(j.JoinOperator.ToSQL())
	stmt.WriteString(j.Table.ToSQL())
	stmt.Token.On()
	stmt.WriteString(j.Constraint.ToSQL())

	return stmt.String()
}

func (j *JoinPredicate) Accept(v AstVisitor) any {
	return v.VisitJoinPredicate(j)
}

type JoinOperator struct {
	node

	JoinType JoinType
	Outer    bool
}

func (j *JoinOperator) Accept(v AstVisitor) any {
	return v.VisitJoinOperator(j)
}

func (j *JoinOperator) Walk(w AstListener) error {
	return run(
		w.EnterJoinOperator(j),
		w.ExitJoinOperator(j),
	)
}

type JoinType uint8

const (
	JoinTypeJoin JoinType = iota
	JoinTypeInner
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
)

func (j *JoinOperator) ToSQL() string {
	stmt := sqlwriter.NewWriter()

	switch j.JoinType {
	case JoinTypeJoin:
		// do nothing
	case JoinTypeInner:
		stmt.Token.Inner()
	case JoinTypeLeft:
		stmt.Token.Left()
	case JoinTypeRight:
		stmt.Token.Right()
	case JoinTypeFull:
		stmt.Token.Full()
	default:
		panic("invalid join type: " + fmt.Sprint(j.JoinType))
	}

	if j.Outer {
		if j.JoinType == JoinTypeInner || j.JoinType == JoinTypeJoin {
			panic("outer join cannot be used with generic join or inner join")
		}
		stmt.Token.Outer()
	}

	stmt.Token.Join()
	return stmt.String()
}

func (j *JoinOperator) Valid() error {
	if j.JoinType < JoinTypeJoin || j.JoinType > JoinTypeFull {
		return fmt.Errorf("invalid join type: %d", j.JoinType)
	}

	if j.Outer && (j.JoinType == JoinTypeJoin || j.JoinType == JoinTypeInner) {
		return fmt.Errorf("outer join cannot be used with generic join or inner join")
	}

	return nil
}

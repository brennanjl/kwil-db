package actparser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	actparser "github.com/kwilteam/kwil-db/parse/actions/parser"
	"github.com/kwilteam/kwil-db/parse/sql/tree"
)

func Test_ParseMany(t *testing.T) {
	stmt := `
	$id = action_x(2, '3');
	INSERT INTO users (id, name) VALUES ($id, 'test');
	`

	got, err := actparser.Parse(stmt)
	assert.NoError(t, err)

	assert.Len(t, got, 2)
}

func TestParseActionStmt(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect actparser.ActionStmt
	}{
		{
			name: "action_call",
			// both `-2` and `- 2` will be parsed as ExpressionUnary
			input: `action_xx(2, '3', $a, @b, -2, 1 + - 2, (1 * 2) + 3, 1 <= 2, 1 and $c, abs($c), abs(upper($c)));`,
			expect: &actparser.ActionCallStmt{
				Method: "action_xx",
				Args: []tree.Expression{
					&tree.ExpressionNumericLiteral{Value: 2},
					&tree.ExpressionTextLiteral{Value: `3`},
					&tree.ExpressionBindParameter{Parameter: "$a"},
					&tree.ExpressionBindParameter{Parameter: "@b"},
					&tree.ExpressionUnary{
						Operator: tree.UnaryOperatorMinus,
						Operand:  &tree.ExpressionNumericLiteral{Value: 2},
					},
					&tree.ExpressionArithmetic{
						Left:     &tree.ExpressionNumericLiteral{Value: 1},
						Operator: tree.ArithmeticOperatorAdd,
						Right: &tree.ExpressionUnary{
							Operator: tree.UnaryOperatorMinus,
							Operand:  &tree.ExpressionNumericLiteral{Value: 2},
						},
					},
					&tree.ExpressionArithmetic{
						Left: &tree.ExpressionArithmetic{
							Wrapped:  true,
							Left:     &tree.ExpressionNumericLiteral{Value: 1},
							Operator: tree.ArithmeticOperatorMultiply,
							Right:    &tree.ExpressionNumericLiteral{Value: 2},
						},
						Operator: tree.ArithmeticOperatorAdd,
						Right:    &tree.ExpressionNumericLiteral{Value: 3},
					},
					&tree.ExpressionBinaryComparison{
						Left:     &tree.ExpressionNumericLiteral{Value: 1},
						Operator: tree.ComparisonOperatorLessThanOrEqual,
						Right:    &tree.ExpressionNumericLiteral{Value: 2},
					},
					&tree.ExpressionBinaryComparison{
						Left:     &tree.ExpressionNumericLiteral{Value: 1},
						Operator: tree.LogicalOperatorAnd,
						Right:    &tree.ExpressionBindParameter{Parameter: "$c"},
					},
					&tree.ExpressionFunction{
						Function: "abs",
						Inputs:   []tree.Expression{&tree.ExpressionBindParameter{Parameter: "$c"}},
					},
					&tree.ExpressionFunction{
						Function: "abs",
						Inputs: []tree.Expression{
							&tree.ExpressionFunction{
								Function: "upper",
								Inputs:   []tree.Expression{&tree.ExpressionBindParameter{Parameter: "$c"}},
							},
						},
					},
				},
			},
		},
		{
			name: "extension_call",
			// both `-2` and `- 2` will be parsed as ExpressionUnary
			input: `$a, $b = erc20.transfer(2, '3', $a, @b, -2, 1 + - 2, (1 * 2) + 3, 1 <= 2, 1 and $c, abs($c), abs(upper($c)));`,
			expect: &actparser.ExtensionCallStmt{
				Extension: "erc20",
				Method:    "transfer",
				Receivers: []string{"$a", "$b"},
				Args: []tree.Expression{
					&tree.ExpressionNumericLiteral{Value: 2},
					&tree.ExpressionTextLiteral{Value: "3"},
					&tree.ExpressionBindParameter{Parameter: "$a"},
					&tree.ExpressionBindParameter{Parameter: "@b"},
					&tree.ExpressionUnary{
						Operator: tree.UnaryOperatorMinus,
						Operand:  &tree.ExpressionNumericLiteral{Value: 2},
					},
					&tree.ExpressionArithmetic{
						Left:     &tree.ExpressionNumericLiteral{Value: 1},
						Operator: tree.ArithmeticOperatorAdd,
						Right: &tree.ExpressionUnary{
							Operator: tree.UnaryOperatorMinus,
							Operand:  &tree.ExpressionNumericLiteral{Value: 2},
						},
					},
					&tree.ExpressionArithmetic{
						Left: &tree.ExpressionArithmetic{
							Wrapped:  true,
							Left:     &tree.ExpressionNumericLiteral{Value: 1},
							Operator: tree.ArithmeticOperatorMultiply,
							Right:    &tree.ExpressionNumericLiteral{Value: 2},
						},
						Operator: tree.ArithmeticOperatorAdd,
						Right:    &tree.ExpressionNumericLiteral{Value: 3},
					},
					&tree.ExpressionBinaryComparison{
						Left:     &tree.ExpressionNumericLiteral{Value: 1},
						Operator: tree.ComparisonOperatorLessThanOrEqual,
						Right:    &tree.ExpressionNumericLiteral{Value: 2},
					},
					&tree.ExpressionBinaryComparison{
						Left:     &tree.ExpressionNumericLiteral{Value: 1},
						Operator: tree.LogicalOperatorAnd,
						Right:    &tree.ExpressionBindParameter{Parameter: "$c"},
					},
					&tree.ExpressionFunction{
						Function: "abs",
						Inputs:   []tree.Expression{&tree.ExpressionBindParameter{Parameter: "$c"}},
					},
					&tree.ExpressionFunction{
						Function: "abs",
						Inputs: []tree.Expression{
							&tree.ExpressionFunction{
								Function: "upper",
								Inputs:   []tree.Expression{&tree.ExpressionBindParameter{Parameter: "$c"}},
							},
						},
					},
				},
			},
		},
		{
			name:  "action_call with sql keyword prefix",
			input: `update_xx(1);`,
			expect: &actparser.ActionCallStmt{
				Method: "update_xx",
				Args: []tree.Expression{
					&tree.ExpressionNumericLiteral{Value: 1},
				},
			},
		},
		{
			name:  "dml select",
			input: `SELECT * FROM users;`,
			expect: &actparser.DMLStmt{
				Statement: `SELECT * FROM users;`,
			},
		},
		{
			name:  "dml insert",
			input: `insert into users (id, name) values (1, "test");`,
			expect: &actparser.DMLStmt{
				Statement: `insert into users (id, name) values (1, "test");`,
			},
		},
		{
			name:  "dml update",
			input: `update users set name = "test" where id = 1;`,
			expect: &actparser.DMLStmt{
				Statement: `update users set name = "test" where id = 1;`,
			},
		},
		{
			name:  "dml delete",
			input: `delete from users where id = 1;`,
			expect: &actparser.DMLStmt{
				Statement: `delete from users where id = 1;`,
			},
		},
		{
			name:  "dml with",
			input: `with x as (select * from users) select * from x;`,
			expect: &actparser.DMLStmt{
				Statement: `with x as (select * from users) select * from x;`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAst, err := actparser.Parse(tt.input)
			if err != nil {
				t.Errorf("ParseActionStmt() error = %v", err)
				return
			}

			assert.EqualValues(t, tt.expect, gotAst[0], "ParseRawSQL() got %+v, want %+v", gotAst, tt.expect)
		})
	}
}

func TestParseActionStmt_scalar_function(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "scalar function notexist",
			input:   `a(notexist($a));`,
			wantErr: true,
		},
	}

	fns := []string{
		"format", "count", "lower", "upper", "abs", "error", "length", "sum",
	}

	// existing scalar functions
	for _, fn := range fns {
		tests = append(tests, struct {
			name    string
			input   string
			wantErr bool
		}{
			name:  "scalar function " + fn,
			input: "a(" + fn + "($a));",
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := actparser.Parse(tt.input)
			if tt.wantErr {
				assert.Error(t, err, "ParseActionStmt(%v)", tt.input)
			} else {
				assert.NoError(t, err, "ParseActionStmt(%v)", tt.input)
			}
		})
	}
}

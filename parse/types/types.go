package types

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
)

type ParseErrors []*ParseError

func (p ParseErrors) Error() string {
	return CombineParseErrors(p).Error()
}

func (p ParseErrors) Err() error {
	return CombineParseErrors(p)
}

func (p *ParseErrors) Add(errs ...*ParseError) {
	*p = append(*p, errs...)
}

// ParseError is an error that occurred during parsing.
type ParseError struct {
	ParserName string         `json:"parser_name,omitempty"`
	Type       ParseErrorType `json:"type"`
	Err        string         `json:"error"`
	Node       *Node          `json:"node,omitempty"`
}

// Error satisfies the standard library error interface.
func (p *ParseError) Error() string {
	// Add 1 to the line and column numbers to make them 1-indexed.
	return fmt.Sprintf("(%s) %s error: start %d:%d end %d:%d: %s", p.ParserName, p.Type, p.Node.StartLine+1, p.Node.StartCol+1, p.Node.EndLine+1, p.Node.EndCol+1, p.Err)
}

// CombineParseErrors formats multiple parse errors into a single error.
// If there are no errors, it will return nil.
func CombineParseErrors(errs []*ParseError) error {
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		str := strings.Builder{}
		for i, err := range errs {
			str.WriteString(fmt.Sprintf("\n%d: %s", i, err.Error()))
		}
		return fmt.Errorf("detected multiple parse errors:%s", str.String())
	}
}

type ParseErrorType string

const (
	ParseErrorTypeSyntax   ParseErrorType = "syntax"
	ParseErrorTypeType     ParseErrorType = "type"
	ParseErrorTypeSemantic ParseErrorType = "semantic"
	ParseErrorTypeUnknown  ParseErrorType = "unknown"
)

type Node struct {
	// Set is true if the position of the node has been set.
	// This is useful for testing parsers.
	IsSet     bool `json:"-"`
	StartLine int  `json:"start_line"`
	StartCol  int  `json:"start_col"`
	EndLine   int  `json:"end_line"`
	EndCol    int  `json:"end_col"`
}

// Set sets the position of the node based on the given parser rule context.
func (n *Node) Set(r antlr.ParserRuleContext) {
	n.IsSet = true
	n.StartLine = r.GetStart().GetLine() - 1
	n.StartCol = r.GetStart().GetColumn()
	n.EndLine = r.GetStop().GetLine() - 1
	n.EndCol = r.GetStop().GetColumn()
}

// SetToken sets the position of the node based on the given token.
func (n *Node) SetToken(t antlr.Token) {
	n.IsSet = true
	n.StartLine = t.GetLine() - 1
	n.StartCol = t.GetColumn()
	n.EndLine = t.GetLine() - 1
	n.EndCol = t.GetColumn()
}

// GetNode returns the node.
// It is useful if the node is embedded in another struct.
func (n Node) GetNode() *Node {
	return &n
}

// unaryNode creates a node with the same start and end position.
func unaryNode(start, end int) *Node {
	return &Node{
		StartLine: start,
		StartCol:  end,
		EndLine:   start,
		EndCol:    end,
	}
}

// MergeNodes merges two nodes into a single node.
// It starts at the left node and ends at the right node.
func MergeNodes(left, right *Node) *Node {
	return &Node{
		StartLine: left.StartLine,
		StartCol:  left.StartCol,
		EndLine:   right.EndLine,
		EndCol:    right.EndCol,
	}
}

// SchemaInfo contains information about a parsed schema
type SchemaInfo struct {
	// Blocks maps declared block names to their nodes.
	// Block names include:
	// - tables
	// - extensions
	// - actions
	// - procedures
	// - foreign procedures
	Blocks map[string]*Block `json:"blocks"`
}

type Block struct {
	Node
	// AbsStart is the absolute start position of the block in the source code.
	AbsStart int `json:"abs_start"`
	// AbsEnd is the absolute end position of the block in the source code.
	AbsEnd int `json:"abs_end"`
}

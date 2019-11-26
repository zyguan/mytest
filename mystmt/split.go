package mystmt

import (
	"bytes"
	"errors"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"
)

//go:generate make

type Command struct {
	Name string
	Args []string
}

type Stmt struct {
	Text     string
	Commands []Command
}

type Iterator interface {
	Scan() bool
	Text() string
	Stmt() Stmt
	Err() error
}

type iterator struct {
	lexer *MyStmt
	head  antlr.Token
	buf   bytes.Buffer
	cmds  []Command
}

func (it *iterator) Scan() bool {
	it.buf.Truncate(0)
	it.cmds = nil
	for {
		it.head = it.lexer.NextToken()
		tt := it.head.GetTokenType()
		if tt == antlr.TokenEOF {
			if it.buf.Len() > 0 {
				return true
			}
			return false
		} else if tt == MyStmtCOMMAND_COMMENT {
			s := it.head.GetText()
			if !strings.HasPrefix(s, "--") {
				continue
			}
			ss := strings.Fields(s)
			cmd := Command{strings.TrimLeft(ss[0], "-"), ss[1:]}
			it.cmds = append(it.cmds, cmd)
		} else if tt == MyStmtSEMI {
			if it.buf.Len() == 0 {
				continue
			}
			it.buf.WriteByte(';')
			return true
		} else if tt >= antlr.TokenMinUserTokenType && it.head.GetChannel() != antlr.TokenHiddenChannel {
			it.buf.WriteString(it.head.GetText())
		}
	}
}

func (it *iterator) Text() string {
	return it.buf.String()
}

func (it *iterator) Stmt() Stmt {
	return Stmt{Text: it.buf.String(), Commands: it.cmds}
}

func (it *iterator) Err() error {
	if it.head == nil {
		return errors.New("scan hasn't been called")
	}
	return nil
}

func SplitText(text string) Iterator {
	in := antlr.NewInputStream(text)
	return &iterator{lexer: NewMyStmt(in)}
}

func SplitFile(file string) (Iterator, error) {
	in, err := antlr.NewFileStream(file)
	if err != nil {
		return nil, err
	}
	return &iterator{lexer: NewMyStmt(in)}, nil
}

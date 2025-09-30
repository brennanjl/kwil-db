// package cache implements a basic LRU cache precompile.
// It only works for text types as of now
package cache

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

func CacheInitializer(ctx context.Context, service *common.Service, db sql.DB, alias string, metadata map[string]any) (precompiles.Precompile, error) {
	var size int64
	sizeAny, ok := metadata["size"]
	if !ok {
		size = 10
	} else {
		size = sizeAny.(int64)
		if size > 100000 {
			return precompiles.Precompile{}, fmt.Errorf("cannot have LRU size exceeding 100000, recieved %d", size)
		}
	}
	cache := newLRU(int(size))
	return precompiles.Precompile{
		Methods: []precompiles.Method{
			{
				Name:            "set",
				AccessModifiers: []precompiles.Modifier{precompiles.SYSTEM},
				Parameters: []precompiles.PrecompileValue{
					{
						Name: "key",
						Type: types.TextType.Copy(),
					},
					{
						Name: "value",
						Type: types.TextType.Copy(),
					},
				},
				Handler: func(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
					cache.Put(inputs[0].(string), inputs[1].(string))
					return nil
				},
			},
			{
				Name:            "get",
				AccessModifiers: []precompiles.Modifier{precompiles.SYSTEM},
				Parameters: []precompiles.PrecompileValue{
					{
						Name: "key",
						Type: types.TextType.Copy(),
					},
				},
				Returns: &precompiles.MethodReturn{
					Fields: []precompiles.PrecompileValue{
						{
							Name:     "value",
							Type:     types.TextType.Copy(),
							Nullable: true,
						},
					},
				},
				Handler: func(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
					v, found := cache.Get(inputs[0].(string))
					if !found {
						return resultFn([]any{nil})
					}
					return resultFn([]any{v})
				},
			},
		},
	}, nil
}

var _ precompiles.Initializer = CacheInitializer

type lruCache struct {
	values     map[string]*node
	head, tail *node
	cap        int
}

type node struct {
	left, right *node
	key, val    string
}

func newLRU(capacity int) lruCache {
	return lruCache{
		values: make(map[string]*node),
		cap:    capacity,
	}
}

func (l *lruCache) Get(key string) (string, bool) {
	n, ok := l.values[key]
	if !ok {
		return "", false // empty string instead of -1
	}

	if n == l.tail {
		l.popTail()
	}
	l.setHead(n)
	return n.val, true
}

func (l *lruCache) Put(key string, value string) {
	if len(l.values) == 0 {
		n := &node{
			key: key,
			val: value,
		}
		l.values[key] = n
		l.setHead(n)
		return
	}

	n, ok := l.values[key]
	if ok {
		n.val = value
		if n == l.tail {
			l.popTail()
		}
		l.setHead(n)
		return
	}

	if l.cap == len(l.values) {
		delete(l.values, l.tail.key)
		l.popTail()
	}

	n = &node{
		key: key,
		val: value,
	}
	l.values[key] = n
	l.setHead(n)
}

func (l *lruCache) setHead(n *node) {
	if l.head == nil {
		l.head = n
		l.tail = n
		return
	}
	if l.head == n {
		return
	}

	if n.left != nil {
		n.left.right = n.right
	}
	if n.right != nil {
		n.right.left = n.left
	}
	n.left = nil
	n.right = nil

	l.head.left = n
	n.right = l.head
	l.head = n
}

func (l *lruCache) popTail() {
	if l.tail == l.head {
		l.head = nil
		l.tail = nil
		return
	}

	l.tail = l.tail.left
	if l.tail != nil {
		l.tail.right = nil
	}
}

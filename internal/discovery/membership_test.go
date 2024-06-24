package discovery_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dunielm02/memdist/internal/discovery"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMembership(t *testing.T) {
	nodeCount := 5
	members, handlers := setUpMembership(t, nodeCount)

	time.Sleep(time.Second * 3)
	for i := range nodeCount {
		for j := range nodeCount {
			if j == i {
				continue
			}
			require.Equal(t, handlers[i].members[strconv.Itoa(j)], members[j].BindAddrs)
		}
	}

	require.NoError(t, members[0].Leave())

	time.Sleep(time.Second * 3)
	for i := 1; i < nodeCount; i++ {
		for k := range handlers[i].members {
			if k == strconv.Itoa(i) {
				continue
			}
			j, err := strconv.Atoi(k)
			require.NoError(t, err)
			require.Equal(t, handlers[i].members[k], members[j].BindAddrs)
		}
	}
}

func setUpMembership(t *testing.T, nodeCount int) (members []*discovery.Membership, handlers []*handler) {
	var ports = dynaport.Get(nodeCount)
	for i := range nodeCount {
		addrs := fmt.Sprintf("127.0.0.1:%d", ports[i])
		cfg := discovery.Config{
			NodeName:  strconv.Itoa(i),
			BindAddrs: addrs,
			Tags: map[string]string{
				"rpc_addr": addrs,
			},
		}
		if i != 0 {
			cfg.StartJoinAddrs = []string{members[0].BindAddrs}
		}
		handler := &handler{
			members: make(map[string]string),
		}
		membership, err := discovery.New(cfg, handler)
		require.NoError(t, err)

		members = append(members, membership)
		handlers = append(handlers, handler)
	}

	return members, handlers
}

type handler struct {
	members map[string]string
}

func (h *handler) Join(name string, addrs string) error {
	h.members[name] = addrs
	return nil
}

func (h *handler) Leave(name string) error {
	delete(h.members, name)
	return nil
}

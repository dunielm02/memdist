package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type SerfHandler interface {
	Join(name string, addrs string) error
	Leave(name string) error
}

type Membership struct {
	Config
	handler SerfHandler
	serf    *serf.Serf
	EventCh chan serf.Event
	logger  *zap.Logger
}

func New(cfg Config, handler SerfHandler) (*Membership, error) {
	m := &Membership{
		Config:  cfg,
		handler: handler,
		logger:  zap.L(),
	}

	err := m.setUpRaft()
	if err != nil {
		return nil, err
	}

	return m, nil
}

type Config struct {
	NodeName       string
	BindAddrs      string
	Tags           map[string]string
	StartJoinAddrs []string
}

func (m *Membership) setUpRaft() error {
	addrs, err := net.ResolveTCPAddr("tcp", m.BindAddrs)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()

	config.MemberlistConfig.BindAddr = addrs.IP.String()
	config.MemberlistConfig.BindPort = addrs.Port
	m.EventCh = make(chan serf.Event)
	config.EventCh = m.EventCh
	config.Tags = m.Tags

	config.NodeName = m.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Membership) eventHandler() {
	for {
		event := <-m.EventCh
		switch event.EventType() {
		case serf.EventMemberJoin:
			for _, member := range event.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range event.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	)
	if err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

func (m *Membership) Leave() error {
	err := m.serf.Leave()
	return err
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	// if err == raft.ErrNotLeader {
	// 	log = m.logger.Debug
	// }
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}

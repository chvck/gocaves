package svcimpls

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// RegisterOptions specifies options used for impl registration
type RegisterOptions struct {
	KvInHooks  mock.KvHookManager
	KvOutHooks mock.KvHookManager
	MgmtHooks  mock.MgmtHookManager
}

// Register registers all known hooks.
func Register(opts RegisterOptions) {
	h := &hookHelper{
		KvInHooks:  opts.KvInHooks,
		KvOutHooks: opts.KvOutHooks,
		MgmtHooks:  opts.MgmtHooks,
	}

	(&kvImplAuth{}).Register(h)
	(&kvImplCccp{}).Register(h)
	(&kvImplCrud{}).Register(h)
	(&kvImplErrMap{}).Register(h)
	(&kvImplHello{}).Register(h)
	(&kvImplPing{}).Register(h)
	(&mgmtImplConfig{}).Register(h)
	(&mgmtImplUser{}).Register(h)
}

func replyWithError(source mock.KvClient, pak *memd.Packet, err error) {
	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusInternalError,
		Value:   []byte(err.Error()),
	})
}

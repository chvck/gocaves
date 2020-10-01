package svcimpls

import (
	"encoding/binary"
	"log"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mockimpl/svcimpls/crudproc"
)

type kvImplCrud struct {
}

func (x *kvImplCrud) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdAdd, x.handleAddRequest)
	h.RegisterKvHandler(memd.CmdSet, x.handleSetRequest)
	h.RegisterKvHandler(memd.CmdReplace, x.handleReplaceRequest)
	h.RegisterKvHandler(memd.CmdGet, x.handleGetRequest)
	h.RegisterKvHandler(memd.CmdDelete, x.handleDeleteRequest)
	h.RegisterKvHandler(memd.CmdIncrement, x.handleIncrementRequest)
	h.RegisterKvHandler(memd.CmdDecrement, x.handleDecrementRequest)
	h.RegisterKvHandler(memd.CmdAppend, x.handleAppendRequest)
	h.RegisterKvHandler(memd.CmdPrepend, x.handlePrependRequest)
	h.RegisterKvHandler(memd.CmdTouch, x.handleTouchRequest)
	h.RegisterKvHandler(memd.CmdGAT, x.handleGATRequest)
	h.RegisterKvHandler(memd.CmdGetLocked, x.handleGetLockedRequest)
	h.RegisterKvHandler(memd.CmdUnlockKey, x.handleUnlockRequest)
	h.RegisterKvHandler(memd.CmdSubDocMultiLookup, x.handleMultiLookupRequest)
	h.RegisterKvHandler(memd.CmdSubDocMultiMutation, x.handleMultiMutateRequest)
}

func (x *kvImplCrud) writeStatusReply(source mock.KvClient, pak *memd.Packet, status memd.StatusCode) {
	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  status,
	})
}

// makeProc either writes a reply to the network, or returns a non-nil Engine to use.
func (x *kvImplCrud) makeProc(source mock.KvClient, pak *memd.Packet) *crudproc.Engine {
	selectedBucket := source.SelectedBucket()
	if selectedBucket == nil {
		x.writeStatusReply(source, pak, memd.StatusNoBucket)
		return nil
	}

	sourceNode := source.Source().Node()
	vbOwnership := selectedBucket.VbucketOwnership(sourceNode)

	return crudproc.New(selectedBucket.Store(), vbOwnership)
}

func (x *kvImplCrud) translateProcErr(err error) memd.StatusCode {
	// TODO(brett19): Implement special handling for various errors on specific versions.

	switch err {
	case nil:
		return memd.StatusSuccess
	case crudproc.ErrNotSupported:
		return memd.StatusNotSupported
	case crudproc.ErrNotMyVbucket:
		return memd.StatusNotMyVBucket
	case crudproc.ErrInternal:
		return memd.StatusInternalError
	case crudproc.ErrDocExists:
		return memd.StatusKeyExists
	case crudproc.ErrDocNotFound:
		return memd.StatusKeyNotFound
	case crudproc.ErrCasMismatch:
		return memd.StatusTmpFail
	case crudproc.ErrLocked:
		return memd.StatusLocked
	case crudproc.ErrNotLocked:
		return memd.StatusTmpFail
	case crudproc.ErrSdToManyTries:
		// TODO(brett19): Confirm too many sd retries is TMPFAIL.
		return memd.StatusTmpFail
	case crudproc.ErrSdNotJSON:
		return memd.StatusSubDocNotJSON
	case crudproc.ErrSdPathInvalid:
		return memd.StatusSubDocPathInvalid
	case crudproc.ErrSdPathMismatch:
		return memd.StatusSubDocPathMismatch
	case crudproc.ErrSdPathNotFound:
		return memd.StatusSubDocPathNotFound
	case crudproc.ErrSdPathExists:
		return memd.StatusSubDocPathExists
	}

	log.Printf("Recieved unexpected crud proc error: %s", err)
	return memd.StatusInternalError
}

func (x *kvImplCrud) writeProcErr(source mock.KvClient, pak *memd.Packet, err error) {
	x.writeStatusReply(source, pak, x.translateProcErr(err))
}

func (x *kvImplCrud) handleGetRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Get(crudproc.GetOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		source.WritePacket(&memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleGetReplicaRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.GetReplica(crudproc.GetOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		source.WritePacket(&memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleAddRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		if pak.Cas != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		flags := binary.BigEndian.Uint32(pak.Extras[0:])
		expiry := binary.BigEndian.Uint32(pak.Extras[4:])

		resp, err := proc.Add(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Datatype:     pak.Datatype,
			Value:        pak.Value,
			Flags:        flags,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleSetRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		flags := binary.BigEndian.Uint32(pak.Extras[0:])
		expiry := binary.BigEndian.Uint32(pak.Extras[4:])

		resp, err := proc.Set(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Datatype:     pak.Datatype,
			Value:        pak.Value,
			Flags:        flags,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleReplaceRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 8 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		flags := binary.BigEndian.Uint32(pak.Extras[0:])
		expiry := binary.BigEndian.Uint32(pak.Extras[4:])

		resp, err := proc.Replace(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Datatype:     pak.Datatype,
			Value:        pak.Value,
			Flags:        flags,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleDeleteRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Delete(crudproc.DeleteOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleIncrementRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 20 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		delta := binary.BigEndian.Uint64(pak.Extras[0:])
		initial := binary.BigEndian.Uint64(pak.Extras[8:])
		expiry := binary.BigEndian.Uint32(pak.Extras[16:])

		resp, err := proc.Increment(crudproc.CounterOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Initial:      initial,
			Delta:        delta,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		valueBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBuf[0:], resp.Value)

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBuf,
		})
	}
}

func (x *kvImplCrud) handleDecrementRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 20 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		delta := binary.BigEndian.Uint64(pak.Extras[0:])
		initial := binary.BigEndian.Uint64(pak.Extras[8:])
		expiry := binary.BigEndian.Uint32(pak.Extras[16:])

		resp, err := proc.Decrement(crudproc.CounterOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Initial:      initial,
			Delta:        delta,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		valueBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(valueBuf[0:], resp.Value)

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBuf,
		})
	}
}

func (x *kvImplCrud) handleAppendRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Append(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Expiry:       0,
			Value:        pak.Value,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handlePrependRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Prepend(crudproc.StoreOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
			Expiry:       0,
			Value:        pak.Value,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleTouchRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 4 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		expiry := binary.BigEndian.Uint32(pak.Extras[0:])

		resp, err := proc.Touch(crudproc.TouchOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleGATRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 4 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		expiry := binary.BigEndian.Uint32(pak.Extras[0:])

		resp, err := proc.GetAndTouch(crudproc.GetAndTouchOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Expiry:       expiry,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		source.WritePacket(&memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleGetLockedRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 4 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		lockTime := binary.BigEndian.Uint32(pak.Extras[0:])

		resp, err := proc.GetLocked(crudproc.GetLockedOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			LockTime:     lockTime,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		extrasBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf[0:], resp.Flags)

		source.WritePacket(&memd.Packet{
			Magic:    memd.CmdMagicRes,
			Command:  pak.Command,
			Opaque:   pak.Opaque,
			Status:   memd.StatusSuccess,
			Cas:      resp.Cas,
			Datatype: resp.Datatype,
			Value:    resp.Value,
			Extras:   extrasBuf,
		})
	}
}

func (x *kvImplCrud) handleUnlockRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		if len(pak.Extras) != 0 {
			x.writeStatusReply(source, pak, memd.StatusInvalidArgs)
			return
		}

		resp, err := proc.Unlock(crudproc.UnlockOptions{
			Vbucket:      uint(pak.Vbucket),
			CollectionID: uint(pak.CollectionID),
			Key:          pak.Key,
			Cas:          pak.Cas,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
		})
	}
}

func (x *kvImplCrud) handleMultiLookupRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		var docFlags memd.SubdocDocFlag
		if len(pak.Extras) >= 1 {
			docFlags = memd.SubdocDocFlag(pak.Extras[0])
		}

		ops := make([]*crudproc.SubDocOp, 0)
		opData := pak.Value
		for byteIdx := 0; byteIdx < len(opData); byteIdx++ {
			opCode := memd.SubDocOpType(opData[byteIdx])

			switch opCode {
			case memd.SubDocOpGet:
				fallthrough
			case memd.SubDocOpExists:
				fallthrough
			case memd.SubDocOpGetCount:
				fallthrough
			case memd.SubDocOpGetDoc:
				if byteIdx+4 > len(opData) {
					log.Printf("not enough bytes 1")
					x.writeProcErr(source, pak, crudproc.ErrInternal)
					return
				}

				opFlags := memd.SubdocFlag(opData[byteIdx+1])
				pathLen := int(binary.BigEndian.Uint16(opData[byteIdx+2:]))
				if byteIdx+4+pathLen > len(opData) {
					log.Printf("not enough bytes 2 - %d - %d", byteIdx, pathLen)
					x.writeProcErr(source, pak, crudproc.ErrInternal)
					return
				}

				path := string(opData[byteIdx+4 : byteIdx+4+pathLen])

				ops = append(ops, &crudproc.SubDocOp{
					Op:          opCode,
					Path:        path,
					Value:       nil,
					IsXattrPath: opFlags&memd.SubdocFlagXattrPath != 0,
				})

				byteIdx += 4 + pathLen - 1

			default:
				log.Printf("unsupported op type")
				x.writeProcErr(source, pak, crudproc.ErrNotSupported)
				return
			}
		}

		resp, err := proc.MultiLookup(crudproc.MultiLookupOptions{
			Vbucket:       uint(pak.Vbucket),
			CollectionID:  uint(pak.CollectionID),
			Key:           pak.Key,
			AccessDeleted: docFlags&memd.SubdocDocFlagAccessDeleted != 0,
			Ops:           ops,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		valueBytes := make([]byte, 0)
		for _, opRes := range resp.Ops {
			opBytes := make([]byte, 6)
			resStatus := x.translateProcErr(opRes.Err)

			binary.BigEndian.PutUint16(opBytes[0:], uint16(resStatus))
			binary.BigEndian.PutUint32(opBytes[2:], uint32(len(opRes.Value)))
			opBytes = append(opBytes, opRes.Value...)

			valueBytes = append(valueBytes, opBytes...)
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBytes,
		})
	}
}

func (x *kvImplCrud) handleMultiMutateRequest(source mock.KvClient, pak *memd.Packet) {
	if proc := x.makeProc(source, pak); proc != nil {
		var docFlags memd.SubdocDocFlag
		if len(pak.Extras) >= 1 {
			docFlags = memd.SubdocDocFlag(pak.Extras[0])
		}

		ops := make([]*crudproc.SubDocOp, 0)
		opData := pak.Value
		for byteIdx := 0; byteIdx < len(opData); byteIdx++ {
			opCode := memd.SubDocOpType(opData[byteIdx])

			switch opCode {
			case memd.SubDocOpDictAdd:
				fallthrough
			case memd.SubDocOpDictSet:
				fallthrough
			case memd.SubDocOpDelete:
				fallthrough
			case memd.SubDocOpReplace:
				fallthrough
			case memd.SubDocOpArrayPushLast:
				fallthrough
			case memd.SubDocOpArrayPushFirst:
				fallthrough
			case memd.SubDocOpArrayInsert:
				fallthrough
			case memd.SubDocOpArrayAddUnique:
				fallthrough
			case memd.SubDocOpCounter:
				fallthrough
			case memd.SubDocOpSetDoc:
				fallthrough
			case memd.SubDocOpAddDoc:
				fallthrough
			case memd.SubDocOpDeleteDoc:
				if byteIdx+8 > len(opData) {
					log.Printf("not enough bytes 11")
					x.writeProcErr(source, pak, crudproc.ErrInternal)
					return
				}

				opFlags := memd.SubdocFlag(opData[byteIdx+1])
				pathLen := int(binary.BigEndian.Uint16(opData[byteIdx+2:]))
				valueLen := int(binary.BigEndian.Uint32(opData[byteIdx+4:]))
				if byteIdx+8+pathLen+valueLen > len(opData) {
					log.Printf("not enough bytes 12 - %d - %d", byteIdx, pathLen)
					x.writeProcErr(source, pak, crudproc.ErrInternal)
					return
				}

				path := string(opData[byteIdx+8 : byteIdx+8+pathLen])
				value := opData[byteIdx+8+pathLen : byteIdx+8+pathLen+valueLen]

				ops = append(ops, &crudproc.SubDocOp{
					Op:           opCode,
					Path:         path,
					Value:        value,
					CreatePath:   opFlags&memd.SubdocFlagMkDirP != 0,
					IsXattrPath:  opFlags&memd.SubdocFlagXattrPath != 0,
					ExpandMacros: opFlags&memd.SubdocFlagExpandMacros != 0,
				})

				byteIdx += 8 + pathLen + valueLen - 1

			default:
				log.Printf("unsupported op type")
				x.writeProcErr(source, pak, crudproc.ErrInternal)
				return
			}
		}

		resp, err := proc.MultiMutate(crudproc.MultiMutateOptions{
			Vbucket:         uint(pak.Vbucket),
			CollectionID:    uint(pak.CollectionID),
			Key:             pak.Key,
			AccessDeleted:   docFlags&memd.SubdocDocFlagAccessDeleted != 0,
			CreateAsDeleted: docFlags&memd.SubdocDocFlagCreateAsDeleted != 0,
			CreateIfMissing: docFlags&memd.SubdocDocFlagMkDoc != 0,
			CreateOnly:      docFlags&memd.SubdocDocFlagAddDoc != 0,
			Ops:             ops,
		})
		if err != nil {
			x.writeProcErr(source, pak, err)
			return
		}

		failedOpIdx := -1
		for opIdx, opRes := range resp.Ops {
			if opRes.Err != nil {
				failedOpIdx = opIdx
			}
		}

		if failedOpIdx >= 0 {
			resStatus := x.translateProcErr(resp.Ops[failedOpIdx].Err)

			valueBytes := make([]byte, 3)
			valueBytes[0] = uint8(failedOpIdx)
			binary.BigEndian.PutUint16(valueBytes[1:], uint16(resStatus))

			// TODO(brett19): Confirm that sub-document errors return 0 CAS.
			source.WritePacket(&memd.Packet{
				Magic:   memd.CmdMagicRes,
				Command: pak.Command,
				Opaque:  pak.Opaque,
				Status:  memd.StatusSubDocBadMulti,
				Cas:     0,
				Value:   valueBytes,
			})
			return
		}

		valueBytes := make([]byte, 0)
		for opIdx, opRes := range resp.Ops {
			if opRes.Err == nil {
				opBytes := make([]byte, 7)
				resStatus := x.translateProcErr(opRes.Err)

				opBytes[0] = uint8(opIdx)
				binary.BigEndian.PutUint16(opBytes[1:], uint16(resStatus))
				binary.BigEndian.PutUint32(opBytes[3:], uint32(len(opRes.Value)))
				opBytes = append(opBytes, opRes.Value...)

				valueBytes = append(valueBytes, opBytes...)
			}
		}

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusSuccess,
			Cas:     resp.Cas,
			Value:   valueBytes,
		})
	}
}

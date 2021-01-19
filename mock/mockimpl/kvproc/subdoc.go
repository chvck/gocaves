package kvproc

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
	"hash/crc32"
)

const subdocMultiMaxPaths = 16

func (e *Engine) executeSdOps(doc, newMeta *mockdb.Document, ops []*SubDocOp) ([]*SubDocResult, error) {
	if len(ops) > subdocMultiMaxPaths {
		return nil, ErrSdBadCombo
	}

	opReses := make([]*SubDocResult, len(ops))

	for opIdx, op := range ops {
		var executor SubDocExecutor
		base := baseSubDocExecutor{
			doc:     doc,
			newMeta: newMeta,
		}

		switch op.Op {
		case memd.SubDocOpGet:
			executor = SubDocGetExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpExists:
			executor = SubDocExistsExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpGetCount:
			executor = SubDocGetCountExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpGetDoc:
			executor = SubDocGetDocExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpDictAdd:
			executor = SubDocDictAddExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpDictSet:
			executor = SubDocDictSetExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpDelete:
			executor = SubDocDeleteExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpReplace:
			executor = SubDocReplaceExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpArrayPushLast:
			executor = SubDocArrayPushLastExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpArrayPushFirst:
			executor = SubDocArrayPushFirstExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpArrayInsert:
			executor = SubDocArrayInsertExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpArrayAddUnique:
			executor = SubDocArrayAddUniqueExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpCounter:
			executor = SubDocCounterExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpSetDoc:
			executor = SubDocDictSetFullExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpDeleteDoc:
			executor = SubDocDeleteFullDocExecutor{
				baseSubDocExecutor: base,
			}
		case memd.SubDocOpAddDoc:
		}

		if executor == nil {
			return nil, ErrInternal
		}

		opRes, err := executor.Execute(op)

		if err != nil {
			return nil, err
		}
		if opRes == nil {
			return nil, ErrInternal
		}

		opReses[opIdx] = opRes
	}

	return opReses, nil
}

func itemErrorResult(err error) (*SubDocResult, error) {
	return &SubDocResult{
		Value: nil,
		Err:   err,
	}, nil
}

func datatypeToString(datatype uint8) []string {
	if datatype == 0x00 {
		return []string{"raw"}
	}

	var typ []string
	if datatype&0x01 == 1 {
		typ = append(typ, "json")
	}
	if datatype&0x02 == 1 {
		typ = append(typ, "snappy")
	}
	if datatype&0x04 == 1 {
		typ = append(typ, "xattr")
	}

	return typ
}

// SubDocExecutor is an executor for subdocument operations.
type SubDocExecutor interface {
	Execute(op *SubDocOp) (*SubDocResult, error)
}

type baseSubDocExecutor struct {
	doc     *mockdb.Document
	newMeta *mockdb.Document
}

func (e baseSubDocExecutor) getVattr(path string) (interface{}, error) {
	// TODO: revid
	table := crc32.MakeTable(crc32.Castagnoli)

	vattr := map[string]interface{}{
		"exptime":       e.doc.Expiry.Unix(),
		"CAS":           fmt.Sprintf("0x%016x", e.doc.Cas),
		"datatype":      datatypeToString(e.doc.Datatype), // WRONG
		"deleted":       e.doc.IsDeleted,
		"flags":         e.doc.Flags,
		"last_modified": fmt.Sprintf("%d", e.doc.ModifiedTime.Unix()),
		"seqno":         fmt.Sprintf("0x%016x", e.doc.SeqNo),
		"value_bytes":   len(e.doc.Value),
		"vbucket_uuid":  fmt.Sprintf("0x%016x", e.doc.VbUUID),
		"value_crc32c":  fmt.Sprintf("0x%x", crc32.Checksum(e.doc.Value, table)),
	}

	var val interface{}
	switch path {
	case "exptime":
		val = vattr["exptime"]
	case "CAS":
		val = vattr["CAS"]
	case "datatype":
		val = vattr["datatype"]
	case "deleted":
		val = vattr["deleted"]
	case "flags":
		val = vattr["flags"]
	case "last_modified":
		val = vattr["last_modified"]
	case "seqno":
		val = vattr["seqno"]
	case "value_bytes":
		val = vattr["value_bytes"]
	case "vbucket_uuid":
		val = vattr["vbucket_uuid"]
	case "$document":
		val = vattr
	default:
		return nil, ErrSdPathNotFound
	}

	return val, nil
}

// SubDocGetExecutor is an executor for subdocument get operations.
type SubDocGetExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocGetExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.get(e.doc.Value, op)
}

func (e SubDocGetExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	p := pathParts[0].Path
	if p == "$document" {
		var vattrPath string
		if len(pathParts) == 1 {
			vattrPath = p
		} else {
			vattrPath = pathParts[1].Path
		}

		vattr, err := e.getVattr(vattrPath)
		if err != nil {
			return itemErrorResult(err)
		}

		b, err := json.Marshal(vattr)
		if err != nil {
			return itemErrorResult(err)
		}

		return &SubDocResult{
			Value: b,
			Err:   nil,
		}, nil
	}

	return e.get(e.doc.Xattrs[p], op)
}

func (e SubDocGetExecutor) get(val []byte, op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(val)
	if err != nil {
		return itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, false, false)
	if err != nil {
		return itemErrorResult(err)
	}

	pathBytes, err := pathVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: pathBytes,
		Err:   nil,
	}, nil
}

// SubDocExistsExecutor is an executor for subdocument operations.
type SubDocExistsExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocExistsExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(e.doc.Value, op)
}

func (e SubDocExistsExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	p := pathParts[0].Path
	if p == "$document" {
		var vattrPath string
		if len(pathParts) == 1 {
			vattrPath = p
		} else {
			vattrPath = pathParts[1].Path
		}

		_, err := e.getVattr(vattrPath)
		if err != nil {
			return itemErrorResult(err)
		}

		return &SubDocResult{
			Value: nil,
			Err:   nil,
		}, nil
	}

	return e.execute(e.doc.Xattrs[pathParts[0].Path], op)
}

func (e SubDocExistsExecutor) execute(val []byte, op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(val)
	if err != nil {
		return itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, false, false)
	if err != nil {
		return itemErrorResult(err)
	}

	_, err = pathVal.Get()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocGetCountExecutor is an executor for subdocument operations.
type SubDocGetCountExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocGetCountExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(e.doc.Value, op)
}

func (e SubDocGetCountExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	p := pathParts[0].Path
	if p == "$document" {
		if len(pathParts) > 1 {
			return itemErrorResult(ErrSdBadCombo)
		}

		vattr, err := e.getVattr(p)
		if err != nil {
			return itemErrorResult(err)
		}

		var toMarshal interface{}
		switch v := vattr.(type) {
		case map[string]interface{}:
			toMarshal = len(v)
		default:
			toMarshal = v
		}

		b, err := json.Marshal(toMarshal)
		if err != nil {
			return itemErrorResult(err)
		}

		return &SubDocResult{
			Value: b,
			Err:   nil,
		}, nil
	}

	return e.execute(e.doc.Xattrs[pathParts[0].Path], op)
}

func (e SubDocGetCountExecutor) execute(val []byte, op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(val)
	if err != nil {
		return itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, false, false)
	if err != nil {
		return itemErrorResult(err)
	}

	pathData, err := pathVal.Get()
	if err != nil {
		return itemErrorResult(err)
	}

	elemCount := 0
	switch typedPathData := pathData.(type) {
	case []interface{}:
		elemCount = len(typedPathData)
	case map[string]interface{}:
		elemCount = len(typedPathData)
	case int64:
		elemCount = int(typedPathData)
	case float64:
		elemCount = int(typedPathData)
	default:
		return itemErrorResult(ErrSdPathMismatch)
	}

	countBytes := []byte(fmt.Sprintf("%d", elemCount))

	return &SubDocResult{
		Value: countBytes,
		Err:   nil,
	}, nil
}

// SubDocGetDocExecutor is an executor for subdocument operations.
type SubDocGetDocExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocGetDocExecutor) Execute(_ *SubDocOp) (*SubDocResult, error) {
	// TODO: check what happens with a full doc against an xattr
	return &SubDocResult{
		Value: e.doc.Value,
		Err:   nil,
	}, nil
}

func docValFromXattrs(xattrs map[string][]byte, path string) (*subDocManip, error) {
	value, ok := xattrs[path]
	if ok {
		return newSubDocManip(value)
	}

	return &subDocManip{
		root: make(map[string]interface{}),
		path: nil,
	}, nil
}

func doMutation(docVal *subDocManip, op *SubDocOp, mutationFn func(pathVal *subDocManip, valueObj interface{}) error) ([]byte, error) {
	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, true)
	if err != nil {
		return nil, err
	}

	var valueObj interface{}
	err = json.Unmarshal(op.Value, &valueObj)
	if err != nil {
		return nil, err
	}

	err = mutationFn(pathVal, valueObj)
	if err != nil {
		return nil, err
	}

	return docVal.GetJSON()
}

// SubDocDictSetExecutor is an executor for subdocument operations.
type SubDocDictSetExecutor struct {
	baseSubDocExecutor
}

func (e SubDocDictSetExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(op)
}

func (e SubDocDictSetExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	docVal, err := docValFromXattrs(e.doc.Xattrs, pathParts[0].Path)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Xattrs[pathParts[0].Path], err = doMutation(docVal, op, func(pathVal *subDocManip, valueObj interface{}) error {
		return pathVal.Set(valueObj)
	})
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocDictSetExecutor) execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = doMutation(docVal, op, func(pathVal *subDocManip, valueObj interface{}) error {
		return pathVal.Set(valueObj)
	})
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocDictAddExecutor is an executor for subdocument operations.
type SubDocDictAddExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocDictAddExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(op)
}

func (e SubDocDictAddExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	docVal, err := docValFromXattrs(e.doc.Xattrs, pathParts[0].Path)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Xattrs[pathParts[0].Path], err = doMutation(docVal, op, func(pathVal *subDocManip, valueObj interface{}) error {
		return pathVal.Insert(valueObj)
	})
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocDictAddExecutor) execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = doMutation(docVal, op, func(pathVal *subDocManip, valueObj interface{}) error {
		return pathVal.Insert(valueObj)
	})
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocReplaceExecutor is an executor for subdocument operations.
type SubDocReplaceExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocReplaceExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(op)
}

func (e SubDocReplaceExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	docVal, err := docValFromXattrs(e.doc.Xattrs, pathParts[0].Path)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Xattrs[pathParts[0].Path], err = doMutation(docVal, op, func(pathVal *subDocManip, valueObj interface{}) error {
		return pathVal.Replace(valueObj)
	})
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocReplaceExecutor) execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = doMutation(docVal, op, func(pathVal *subDocManip, valueObj interface{}) error {
		return pathVal.Replace(valueObj)
	})
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocDeleteExecutor is an executor for subdocument operations.
type SubDocDeleteExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocDeleteExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(op)
}

func (e SubDocDeleteExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathComps, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	docVal, err := docValFromXattrs(e.doc.Xattrs, op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	err = e.processDelete(docVal, pathComps, op)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Xattrs[pathComps[0].Path], err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocDeleteExecutor) execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	pathComps, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}
	if len(pathComps) <= 0 {
		// Need to at least specify the index to insert at.
		return itemErrorResult(ErrSdPathInvalid)
	}

	err = e.processDelete(docVal, pathComps, op)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocDeleteExecutor) processDelete(docVal *subDocManip, pathComps []SubDocPathComponent, op *SubDocOp) error {
	lastPathComp := pathComps[len(pathComps)-1]

	// Calculate the path without the array index
	deOpPath := StringifySubDocPath(pathComps[:len(pathComps)-1])

	pathVal, err := docVal.GetByPath(deOpPath, op.CreatePath, false)
	if err != nil {
		return err
	}

	val, err := pathVal.Get()
	if err != nil {
		return err
	}

	if lastPathComp.Path != "" {
		// Map
		mapVal, isMap := val.(map[string]interface{})
		if !isMap {
			return ErrSdPathMismatch
		}

		_, hasElem := mapVal[lastPathComp.Path]
		if !hasElem {
			return ErrSdPathNotFound
		}

		delete(mapVal, lastPathComp.Path)

		err = pathVal.Replace(mapVal)
		if err != nil {
			return err
		}
	} else {
		// Array
		arrVal, isArr := val.([]interface{})
		if !isArr {
			return ErrSdPathMismatch
		}

		if lastPathComp.ArrayIndex < 0 {
			lastPathComp.ArrayIndex = len(arrVal) + lastPathComp.ArrayIndex
		}
		if lastPathComp.ArrayIndex >= len(arrVal) {
			return ErrSdPathNotFound
		}

		newArrVal := make([]interface{}, 0)
		newArrVal = append(newArrVal, arrVal[:lastPathComp.ArrayIndex]...)
		newArrVal = append(newArrVal, arrVal[lastPathComp.ArrayIndex+1:]...)

		err = pathVal.Replace(newArrVal)
		if err != nil {
			return err
		}
	}

	return nil
}

// SubDocCounterExecutor is an executor for subdocument operations.
type SubDocCounterExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocCounterExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(op)
}

func (e SubDocCounterExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	log.Printf("%#v\n", pathParts)

	docVal, err := docValFromXattrs(e.doc.Xattrs, op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	log.Printf("dv %+v\n", docVal)

	newVal, err := e.doCounter(docVal, op)
	if err != nil {
		log.Printf("err %+v\n", err)
		return itemErrorResult(err)
	}

	e.doc.Xattrs[pathParts[0].Path], err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	log.Printf("x %+v\n", e.doc.Xattrs)

	return &SubDocResult{
		Value: []byte(strconv.FormatInt(newVal, 10)),
		Err:   nil,
	}, nil
}

func (e SubDocCounterExecutor) execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	newVal, err := e.doCounter(docVal, op)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: []byte(strconv.FormatInt(newVal, 10)),
		Err:   nil,
	}, nil
}

func (e *SubDocCounterExecutor) doCounter(docVal *subDocManip, op *SubDocOp) (int64, error) {
	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, true)
	if err != nil {
		return 0, err
	}

	var delta float64
	err = json.Unmarshal(op.Value, &delta)
	if err != nil {
		return 0, err
	}

	// TODO(brett19): Check the behaviour when the path doesn't exist.

	val, err := pathVal.Get()
	if err != nil {
		if errors.Is(err, ErrSdPathNotFound) {
			val = float64(0)
		} else {
			return 0, err
		}
	}

	floatVal, isFloat := val.(float64)
	if !isFloat {
		return 0, ErrSdPathMismatch
	}

	newVal := int64(floatVal + delta)

	err = pathVal.Set(newVal)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

func subDocArrayPush(docVal *subDocManip, op *SubDocOp, pushFront bool) error {
	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, false)
	if err != nil {
		return err
	}

	var fullValue []interface{}
	fullValueBytes := append([]byte("["), append(append([]byte{}, op.Value...), []byte("]")...)...)
	err = json.Unmarshal(fullValueBytes, &fullValue)
	if err != nil {
		return err
	}

	val, err := pathVal.Get()
	if err != nil {
		return err
	}

	arrVal, isArr := val.([]interface{})
	if !isArr {
		return ErrSdPathMismatch
	}

	if pushFront {
		arrVal = append(fullValue, arrVal...)
	} else {
		arrVal = append(arrVal, fullValue...)
	}

	return pathVal.Replace(arrVal)
}

// SubDocArrayPushFirstExecutor is an executor for subdocument operations.
type SubDocArrayPushFirstExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayPushFirstExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(op)
}

func (e SubDocArrayPushFirstExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	docVal, err := docValFromXattrs(e.doc.Xattrs, op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	err = subDocArrayPush(docVal, op, true)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Xattrs[pathParts[0].Path], err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocArrayPushFirstExecutor) execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	err = subDocArrayPush(docVal, op, true)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocArrayPushLastExecutor is an executor for subdocument operations.
type SubDocArrayPushLastExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayPushLastExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(op)
}

func (e SubDocArrayPushLastExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	docVal, err := docValFromXattrs(e.doc.Xattrs, op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	err = subDocArrayPush(docVal, op, false)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Xattrs[pathParts[0].Path], err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocArrayPushLastExecutor) execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	err = subDocArrayPush(docVal, op, false)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocArrayInsertExecutor is an executor for subdocument operations.
type SubDocArrayInsertExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayInsertExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(op)
}

func (e SubDocArrayInsertExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	docVal, err := docValFromXattrs(e.doc.Xattrs, op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	err = e.insert(docVal, op, pathParts)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Xattrs[pathParts[0].Path], err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocArrayInsertExecutor) execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	pathComps, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}
	if len(pathComps) <= 0 {
		// Need to at least specify the index to insert at.
		return itemErrorResult(ErrSdPathInvalid)
	}

	err = e.insert(docVal, op, pathComps)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocArrayInsertExecutor) insert(docVal *subDocManip, op *SubDocOp, pathComps []SubDocPathComponent) error {
	lastPathComp := pathComps[len(pathComps)-1]
	if lastPathComp.Path != "" {
		// Last index should be an array index.
		return ErrSdPathInvalid
	}

	// Calculate the path without the array index
	deOpPath := StringifySubDocPath(pathComps[:len(pathComps)-1])

	pathVal, err := docVal.GetByPath(deOpPath, op.CreatePath, false)
	if err != nil {
		return err
	}

	var fullValue []interface{}
	fullValueBytes := append([]byte("["), append(append([]byte{}, op.Value...), []byte("]")...)...)
	err = json.Unmarshal(fullValueBytes, &fullValue)
	if err != nil {
		return err
	}

	val, err := pathVal.Get()
	if err != nil {
		return err
	}

	arrVal, isArr := val.([]interface{})
	if !isArr {
		return ErrSdPathMismatch
	}

	if lastPathComp.ArrayIndex < 0 {
		lastPathComp.ArrayIndex = len(arrVal) + lastPathComp.ArrayIndex
	}
	if lastPathComp.ArrayIndex >= len(arrVal) {
		// Cant insert past the end of the array
		return ErrSdPathMismatch
	}

	var newArrVal []interface{}
	newArrVal = append(newArrVal, arrVal[:lastPathComp.ArrayIndex]...)
	newArrVal = append(newArrVal, fullValue...)
	newArrVal = append(newArrVal, arrVal[lastPathComp.ArrayIndex:]...)

	return pathVal.Replace(newArrVal)
}

// SubDocArrayAddUniqueExecutor is an executor for subdocument operations.
type SubDocArrayAddUniqueExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocArrayAddUniqueExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.IsXattrPath {
		return e.executeXattr(op)
	}

	return e.execute(op)
}

func (e SubDocArrayAddUniqueExecutor) executeXattr(op *SubDocOp) (*SubDocResult, error) {
	if op.Path == "" {
		return itemErrorResult(ErrSdInvalidXattr)
	}

	pathParts, err := ParseSubDocPath(op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	docVal, err := docValFromXattrs(e.doc.Xattrs, op.Path)
	if err != nil {
		return itemErrorResult(err)
	}

	err = e.addUnique(docVal, op)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Xattrs[pathParts[0].Path], err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocArrayAddUniqueExecutor) execute(op *SubDocOp) (*SubDocResult, error) {
	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	err = e.addUnique(docVal, op)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

func (e SubDocArrayAddUniqueExecutor) addUnique(docVal *subDocManip, op *SubDocOp) error {
	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, false)
	if err != nil {
		return err
	}

	var valueObj interface{}
	err = json.Unmarshal(op.Value, &valueObj)
	if err != nil {
		return err
	}

	val, err := pathVal.Get()
	if err != nil {
		return err
	}

	arrVal, isArr := val.([]interface{})
	if !isArr {
		return ErrSdPathMismatch
	}

	foundExisting := false
	for _, arrElem := range arrVal {
		if reflect.DeepEqual(arrElem, valueObj) {
			foundExisting = true
			break
		}
	}

	if foundExisting {
		return ErrSdPathExists
	}

	arrVal = append(arrVal, valueObj)

	return pathVal.Replace(arrVal)
}

// SubDocDeleteFullDocExecutor is an executor for subdocument operations.
type SubDocDeleteFullDocExecutor struct {
	baseSubDocExecutor
}

// Execute performs the subdocument operation.
func (e SubDocDeleteFullDocExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.Path != "" {
		return itemErrorResult(ErrInvalidArgument)
	}
	if len(op.Value) != 0 {
		return itemErrorResult(ErrInvalidArgument)
	}

	e.doc.IsDeleted = true

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

// SubDocExistsExecutor is an executor for subdocument operations.
type SubDocDictSetFullExecutor struct {
	baseSubDocExecutor
}

func (e SubDocDictSetFullExecutor) Execute(op *SubDocOp) (*SubDocResult, error) {
	if op.Path != "" {
		return itemErrorResult(ErrInvalidArgument)
	}

	docVal, err := newSubDocManip(e.doc.Value)
	if err != nil {
		return itemErrorResult(err)
	}

	pathVal, err := docVal.GetByPath(op.Path, op.CreatePath, true)
	if err != nil {
		return itemErrorResult(err)
	}

	var valueObj interface{}
	err = json.Unmarshal(op.Value, &valueObj)
	if err != nil {
		return itemErrorResult(err)
	}

	err = pathVal.Set(valueObj)
	if err != nil {
		return itemErrorResult(err)
	}

	e.doc.Value, err = docVal.GetJSON()
	if err != nil {
		return itemErrorResult(err)
	}

	return &SubDocResult{
		Value: nil,
		Err:   nil,
	}, nil
}

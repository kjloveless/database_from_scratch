package main

import (
  "encoding/binary"
)

type BNode []byte // can be dumped to disk

const (
  BTREE_PAGE_SIZE     = 4096
  BTREE_MAX_KEY_SIZE  = 1000
  BTREE_MAX_VAL_SIZE  = 3000
)

// getters
func (node BNode) btype() uint16 {
  return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
  return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) nbytes() uint16 {   // node size in bytes
  return node.kvPos(node.nkeys()) // uses the offset value of the last key
}

// setter
func (node BNode) setHeader(btype uint16, nkeys uint16) {
  binary.LittleEndian.PutUint16(node[0:2], btype)
  binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// read and write the child pointers array
func (node BNode) getPtr(idx uint16) uint64 {
  assert(idx < node.nkeys())
  pos := 4 + 8 * idx
  return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
  assert(idx < node.nkeys())
  pos := 4 + 8 * idx
  binary.LittleEndian.PutUint64(node[pos:], val)
}

// read the 'offsets' array
func (node BNode) getOffset(idx uint16) uint16 {
  if idx == 0 {
    return 0
  }
  pos := 4 + 8 * node.nkeys() + 2 * (idx - 1)
  return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) kvPos(idx uint16) uint16 {
  assert(idx <= node.nkeys())
  return 4 + 8 * node.nkeys() + 2 * node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
  assert(idx < node.nkeys())
  pos := node.kvPos(idx)
  klen := binary.LittleEndian.Uint16(node[pos:])
  return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
  assert(idx < node.nkeys())
  pos := node.kvPos(idx)
  klen := binary.LittleEndian.Uint16(node[pos+0:])
  vlen := binary.LittleEndian.Uint16(node[pos+2:])
  return node[pos+4+klen:][:vlen]
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
  // ptrs
  new.setPtr(idx, ptr)
  // KVs
  pos := new.kvPos(idx)   // uses the offset value of the previous key
  // 4-bytes KV sizes
  binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
  binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
  // KV data
  copy(new[pos+4:], key)
  copy(new[pos+4+uint16(len(key)):], val)
  // update the offset value for the next key
  new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
  new.setHeader(BNODE_LEAF, old.nkeys()+1)
  nodeAppendRange(new, old, 0, 0, idx)    // copy the keys before 'idx'
  nodeAppend(new, idx, 0, key, val)       // the new key
  nodeAppendRange(new, old, idx + 1, idx, old.nkeys() - idx)  // keys from 'idx'
}

// copy multiple keys, values, and pointers into the position
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
  for i := uint16(0); i < n; i++ {
    dst, src := dstNew + i, srcOld + i
    nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
  }
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
  new.setHeader(BNODE_LEAF, old.nkeys())

}

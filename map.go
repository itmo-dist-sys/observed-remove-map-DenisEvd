package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

func (v Version) Less(other Version) bool {
	if v.Counter == other.Counter {
		return v.NodeID < other.NodeID
	}
	return v.Counter < other.Counter
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	id       string
	allNodes []string

	mu    sync.RWMutex
	clock uint64
	state MapState
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	return &CRDTMapNode{
		BaseNode: hive.NewBaseNode(id),
		id:       id,
		allNodes: allNodeIDs,
		mu:       sync.RWMutex{},
		clock:    0,
		state:    make(MapState),
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state := n.State()
				for _, peer := range n.allNodes {
					if peer != n.id {
						_ = n.Send(peer, state)
					}
				}
			}
		}
	}()
	return nil
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   n.newVersion(),
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entry, ok := n.state[k]
	if !ok || entry.Tombstone {
		return "", false
	}
	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state[k] = StateEntry{
		Tombstone: true,
		Version:   n.newVersion(),
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for k, remoteEntry := range remote {
		localEntry, ok := n.state[k]
		if !ok || localEntry.Version.Less(remoteEntry.Version) {
			n.state[k] = remoteEntry
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	res := make(MapState, len(n.state))
	for k, v := range n.state {
		res[k] = v
	}
	return res
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	res := make(map[string]string)
	for k, v := range n.state {
		if !v.Tombstone {
			res[k] = v.Value
		}
	}
	return res
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	if state, ok := msg.Payload.(MapState); ok {
		n.Merge(state)
	}
	return nil
}

func (n *CRDTMapNode) newVersion() Version {
	n.clock++
	return Version{Counter: n.clock, NodeID: n.id}
}

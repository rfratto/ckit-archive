package ckit

// An Observer watches a Node, waiting for its peers to change.
type Observer interface {
	// NotifyPeersChanged is invoked any time the set of Peers for a node
	// changes. Call Node.Peers to get the current list of peers.
	//
	// If NotifyPeersChanged returns false, the Observer will no longer receive
	// any notifications. This can be used for single-use watches.
	NotifyPeersChanged() (reregister bool)
}

// FuncObserver implements Observer.
type FuncObserver func() (reregister bool)

// NotifyPeersChanged implements Observer.
func (f FuncObserver) NotifyPeersChanged() (reregister bool) { return f() }

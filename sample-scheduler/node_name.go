package main

import (
	"context"
	"fmt"
)

// NodeName is a plugin that checks if a pod spec node name matches the current node.
type NodeName struct{}

var _ PreFilterPlugin = &NodeName{}
var _ FilterPlugin = &NodeName{}

//var _ framework.FilterPlugin = &NodeName{}
//var _ framework.EnqueueExtensions = &NodeName{}

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = "NodeName"

	// ErrReason returned when node name doesn't match.
	ErrReason = "node(s) didn't match the requested node name"
)

// Name returns name of the plugin. It is used in logs, etc.
func (pl *NodeName) Name() string {
	return Name
}

// Filter invoked at the filter extension point.
func (pl *NodeName) PreFilter(_ context.Context, _ *Pod) (*PreFilterResult, error) {
	fmt.Printf("%s PreFilter called\n", Name)
	return nil, nil
}

func (pl *NodeName) Filter(ctx context.Context, pod *Pod, nodeInfo *NodeInfo) error {
	if nodeInfo.node == nil {
		fmt.Errorf("node not found")
	}

	if pod.nodeName != nodeInfo.node.name {
		fmt.Errorf("%s", ErrReason)
	}

	return nil
}

// New initializes a new plugin and returns it.
func New() (Plugin, error) {
	return &NodeName{}, nil
}

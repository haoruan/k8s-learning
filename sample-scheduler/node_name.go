package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// NodeName is a plugin that checks if a pod spec node name matches the current node.
type NodeName struct{}

var _ PreFilterPlugin = &NodeName{}
var _ FilterPlugin = &NodeName{}
var _ ScorePlugin = &NodeName{}
var _ ReservePlugin = &NodeName{}
var _ PermitPlugin = &NodeName{}
var _ PreBindPlugin = &NodeName{}
var _ PostBindPlugin = &NodeName{}

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
		return fmt.Errorf("node not found")
	}

	nodeName := strings.Split(nodeInfo.node.name, "-")[0]

	if pod.nodeName != nodeName {
		return fmt.Errorf("%s", ErrReason)
	}

	return nil
}

func (pl *NodeName) PreScore(ctx context.Context, pod *Pod, nodes []*Node) error {
	return nil
}

func (pl *NodeName) Score(ctx context.Context, p *Pod, node *Node) (int64, error) {
	return node.score, nil
}

func (pl *NodeName) Reserve(ctx context.Context, p *Pod, nodeName string) error {
	return nil
}

func (pl *NodeName) Permit(ctx context.Context, p *Pod, nodeName string) (time.Duration, error) {
	return 0, nil
}

// PreBind is the functions invoked by the framework at "prebind" extension point.
func (pl *NodeName) PreBind(ctx context.Context, pod *Pod, nodeName string) error {
	if pod == nil {
		return fmt.Errorf("pod cannot be nil")
	}
	return nil
}

func (pl *NodeName) PostBind(ctx context.Context, pod *Pod, nodeName string) {
	fmt.Println("PostBind")
}

// New initializes a new plugin and returns it.
func New() (Plugin, error) {
	return &NodeName{}, nil
}

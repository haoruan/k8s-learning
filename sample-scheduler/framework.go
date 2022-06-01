package main

import (
	"context"
	"fmt"
)

type Framework interface {
	// RunFilterPlugins runs the set of configured Filter plugins for pod on
	// the given node. Note that for the node being evaluated, the passed nodeInfo
	// reference could be different from the one in NodeInfoSnapshot map (e.g., pods
	// considered to be running on the node could be different). For example, during
	// preemption, we may pass a copy of the original nodeInfo object that has some pods
	// removed from it to evaluate the possibility of preempting them to
	// schedule the target pod.
	RunFilterPlugins(context.Context, *Pod, *NodeInfo) error

	// RunPreFilterPlugins runs the set of configured PreFilter plugins. It returns
	// *Status and its code is set to non-success if any of the plugins returns
	// anything but Success. If a non-success status is returned, then the scheduling
	// cycle is aborted.
	// It also returns a PreFilterResult, which may influence what or how many nodes to
	// evaluate downstream.
	RunPreFilterPlugins(ctx context.Context, pod *Pod) (*PreFilterResult, error)
	RunReservePluginsReserve(ctx context.Context, pod *Pod, nodeName string) error
	RunPermitPlugins(ctx context.Context, pod *Pod, nodeName string) error
	// HasFilterPlugins returns true if at least one Filter plugin is defined.
	HasFilterPlugins() bool
	// HasScorePlugins returns true if at least one Score plugin is defined.
	HasScorePlugins() bool

	Parallelizer() Parallelizer
}

// Plugin is the parent type for all the scheduling framework plugins.
type Plugin interface {
	Name() string
}

// PreFilterPlugin is an interface that must be implemented by "PreFilter" plugins.
// These plugins are called at the beginning of the scheduling cycle.
type PreFilterPlugin interface {
	Plugin
	// PreFilter is called at the beginning of the scheduling cycle. All PreFilter
	// plugins must return success or the pod will be rejected. PreFilter could optionally
	// return a PreFilterResult to influence which nodes to evaluate downstream. This is useful
	// for cases where it is possible to determine the subset of nodes to process in O(1) time.
	PreFilter(context.Context, *Pod) (*PreFilterResult, error)
	// PreFilterExtensions returns a PreFilterExtensions interface if the plugin implements one,
	// or nil if it does not. A Pre-filter plugin can provide extensions to incrementally
	// modify its pre-processed info. The framework guarantees that the extensions
	// AddPod/RemovePod will only be called after PreFilter, possibly on a cloned
	// CycleState, and may call those functions more than once before calling
	// Filter again on a specific node.
	// PreFilterExtensions() PreFilterExtensions
}

// FilterPlugin is an interface for Filter plugins. These plugins are called at the
// filter extension point for filtering out hosts that cannot run a pod.
// This concept used to be called 'predicate' in the original scheduler.
// These plugins should return "Success", "Unschedulable" or "Error" in Status.code.
// However, the scheduler accepts other valid codes as well.
// Anything other than "Success" will lead to exclusion of the given host from
// running the pod.
type FilterPlugin interface {
	Plugin
	// Filter is called by the scheduling framework.
	// All FilterPlugins should return "Success" to declare that
	// the given node fits the pod. If Filter doesn't return "Success",
	// it will return "Unschedulable", "UnschedulableAndUnresolvable" or "Error".
	// For the node being evaluated, Filter plugins should look at the passed
	// nodeInfo reference for this particular node's information (e.g., pods
	// considered to be running on the node) instead of looking it up in the
	// NodeInfoSnapshot because we don't guarantee that they will be the same.
	// For example, during preemption, we may pass a copy of the original
	// nodeInfo object that has some pods removed from it to evaluate the
	// possibility of preempting them to schedule the target pod.
	Filter(ctx context.Context, pod *Pod, nodeInfo *NodeInfo) error
}

type frameworkImpl struct {
	preFilterPlugins []PreFilterPlugin
	filterPlugins    []FilterPlugin
	parallelizer     Parallelizer
}

// PreFilterResult wraps needed info for scheduler framework to act upon PreFilter phase.
type PreFilterResult struct {
	// The set of nodes that should be considered downstream; if nil then
	// all nodes are eligible.
	NodeNames map[string]struct{}
}

func (p *PreFilterResult) Merge(in *PreFilterResult) *PreFilterResult {
	if p.AllNodes() && in.AllNodes() {
		return nil
	}

	r := PreFilterResult{}
	if p.AllNodes() {
		for k := range in.NodeNames {
			r.NodeNames[k] = struct{}{}
		}
		return &r
	}
	if in.AllNodes() {
		for k := range p.NodeNames {
			r.NodeNames[k] = struct{}{}
		}
		return &r
	}

	// Intersection
	var walk, other map[string]struct{}
	if len(p.NodeNames) < len(in.NodeNames) {
		walk = p.NodeNames
		other = in.NodeNames
	} else {
		walk = in.NodeNames
		other = p.NodeNames
	}
	for key := range walk {
		if _, f := other[key]; f {
			r.NodeNames[key] = struct{}{}
		}
	}

	return &r
}

func (p *PreFilterResult) AllNodes() bool {
	return p == nil || p.NodeNames == nil
}

// RunFilterPlugins runs the set of configured Filter plugins for pod on
// the given node. If any of these plugins doesn't return "Success", the
// given node is not suitable for running pod.
// Meanwhile, the failure message and status are set for the given node.
func (f *frameworkImpl) RunFilterPlugins(
	ctx context.Context,
	pod *Pod,
	nodeInfo *NodeInfo,
) error {
	for _, pl := range f.filterPlugins {
		err := f.runFilterPlugin(ctx, pl, pod, nodeInfo)
		if err != nil {
			return err
		}
	}

	return nil
}

// RunPreFilterPlugins runs the set of configured PreFilter plugins. It returns
// *Status and its code is set to non-success if any of the plugins returns
// anything but Success. If a non-success status is returned, then the scheduling
// cycle is aborted.
func (f *frameworkImpl) RunPreFilterPlugins(ctx context.Context, pod *Pod) (*PreFilterResult, error) {
	// startTime := time.Now()
	// defer func() {
	// 	metrics.FrameworkExtensionPointDuration.WithLabelValues(preFilter, status.Code().String(), f.profileName).Observe(metrics.SinceInSeconds(startTime))
	// }()
	var result *PreFilterResult
	var pluginsWithNodes []string
	for _, pl := range f.preFilterPlugins {
		r, err := f.runPreFilterPlugin(ctx, pl, pod)
		if err != nil {
			return nil, err
		}
		if !r.AllNodes() {
			pluginsWithNodes = append(pluginsWithNodes, pl.Name())
		}
		result = result.Merge(r)
		if !result.AllNodes() && len(result.NodeNames) == 0 {
			msg := fmt.Sprintf("node(s) didn't satisfy plugin(s) %v simultaneously", pluginsWithNodes)
			if len(pluginsWithNodes) == 1 {
				msg = fmt.Sprintf("node(s) didn't satisfy plugin %v", pluginsWithNodes[0])
			}
			return nil, fmt.Errorf("%s", msg)
		}

	}
	return result, nil
}

func (f *frameworkImpl) runPreFilterPlugin(ctx context.Context, pl PreFilterPlugin, pod *Pod) (*PreFilterResult, error) {
	return pl.PreFilter(ctx, pod)
}

func (f *frameworkImpl) runFilterPlugin(ctx context.Context, pl FilterPlugin, pod *Pod, nodeInfo *NodeInfo) error {
	return pl.Filter(ctx, pod, nodeInfo)
}

func (f *frameworkImpl) RunReservePluginsReserve(ctx context.Context, pod *Pod, nodeName string) error {
	return nil
}

func (f *frameworkImpl) RunPermitPlugins(ctx context.Context, pod *Pod, nodeName string) error {
	return nil
}

func (f *frameworkImpl) HasFilterPlugins() bool {
	return false
}

// Parallelizer returns a parallelizer holding parallelism for scheduler.
func (f *frameworkImpl) Parallelizer() Parallelizer {
	return f.parallelizer
}

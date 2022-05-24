package main

import (
	"context"
	"fmt"

	"k8s.io/kubernetes/pkg/scheduler/framework"
	v1 "k8s.io/kubernetes/staging/src/k8s.io/api/core/v1"
)

type Framework interface {
	RunPreFilterPlugins(ctx context.Context, pod *Pod) (*PreFilterResult, error)
	RunReservePluginsReserve(ctx context.Context, pod *Pod, nodeName string) error
	RunPermitPlugins(ctx context.Context, pod *Pod, nodeName string) error
	// HasFilterPlugins returns true if at least one Filter plugin is defined.
	HasFilterPlugins() bool
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
	PreFilter(ctx context.Context, state *CycleState, p *v1.Pod) (*PreFilterResult, *Status)
	// PreFilterExtensions returns a PreFilterExtensions interface if the plugin implements one,
	// or nil if it does not. A Pre-filter plugin can provide extensions to incrementally
	// modify its pre-processed info. The framework guarantees that the extensions
	// AddPod/RemovePod will only be called after PreFilter, possibly on a cloned
	// CycleState, and may call those functions more than once before calling
	// Filter again on a specific node.
	PreFilterExtensions() PreFilterExtensions
}

type frameworkImpl struct {
	preFilterPlugins []PreFilterPlugin
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
			return nil, framework.NewStatus(framework.Unschedulable, msg)
		}

	}
	return result, nil
}

func (f *frameworkImpl) runPreFilterPlugin(ctx context.Context, pl PreFilterPlugin, pod *Pod) (*PreFilterResult, error) {
	return nil, nil
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

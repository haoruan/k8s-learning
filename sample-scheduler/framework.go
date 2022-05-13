package main

import (
	"context"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type Framework interface {
	RunPreFilterPlugins(ctx context.Context, pod *Pod) (*PreFilterResult, error)
	// HasFilterPlugins returns true if at least one Filter plugin is defined.
	HasFilterPlugins() bool
}

type frameworkImpl struct {
	preFilterPlugins []framework.PreFilterPlugin
}

// PreFilterResult wraps needed info for scheduler framework to act upon PreFilter phase.
type PreFilterResult struct {
	// The set of nodes that should be considered downstream; if nil then
	// all nodes are eligible.
	NodeNames map[string]struct{}
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
	// var pluginsWithNodes []string
	// for _, pl := range f.preFilterPlugins {
	// 	r, s := f.runPreFilterPlugin(ctx, pl, state, pod)
	// 	if !s.IsSuccess() {
	// 		s.SetFailedPlugin(pl.Name())
	// 		if s.IsUnschedulable() {
	// 			return nil, s
	// 		}
	// 		return nil, framework.AsStatus(fmt.Errorf("running PreFilter plugin %q: %w", pl.Name(), status.AsError())).WithFailedPlugin(pl.Name())
	// 	}
	// 	if !r.AllNodes() {
	// 		pluginsWithNodes = append(pluginsWithNodes, pl.Name())
	// 	}
	// 	result = result.Merge(r)
	// 	if !result.AllNodes() && len(result.NodeNames) == 0 {
	// 		msg := fmt.Sprintf("node(s) didn't satisfy plugin(s) %v simultaneously", pluginsWithNodes)
	// 		if len(pluginsWithNodes) == 1 {
	// 			msg = fmt.Sprintf("node(s) didn't satisfy plugin %v", pluginsWithNodes[0])
	// 		}
	// 		return nil, framework.NewStatus(framework.Unschedulable, msg)
	// 	}

	// }
	return result, nil
}

func (f *frameworkImpl) HasFilterPlugins() bool {
	return false
}

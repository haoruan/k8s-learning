package main

import (
	"context"
	"fmt"
	"time"
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

	RunPreScorePlugins(ctx context.Context, pod *Pod, nodes []*Node) error
	RunScorePlugins(ctx context.Context, pod *Pod, nodes []*Node) (map[string][]NodeScore, error)
	// HasFilterPlugins returns true if at least one Filter plugin is defined.

	// RunPreBindPlugins runs the set of configured PreBind plugins. It returns
	// *Status and its code is set to non-success if any of the plugins returns
	// anything but Success. If the Status code is "Unschedulable", it is
	// considered as a scheduling check failure, otherwise, it is considered as an
	// internal error. In either case the pod is not going to be bound.
	RunPreBindPlugins(ctx context.Context, pod *Pod, nodeName string) error

	// RunPostBindPlugins runs the set of configured PostBind plugins.
	RunPostBindPlugins(ctx context.Context, pod *Pod, nodeName string)

	// RunBindPlugins runs the set of configured Bind plugins. A Bind plugin may choose
	// whether or not to handle the given Pod. If a Bind plugin chooses to skip the
	// binding, it should return code=5("skip") status. Otherwise, it should return "Error"
	// or "Success". If none of the plugins handled binding, RunBindPlugins returns
	// code=5("skip") status.
	RunBindPlugins(ctx context.Context, pod *Pod, nodeName string) error

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

// PreScorePlugin is an interface for "PreScore" plugin. PreScore is an
// informational extension point. Plugins will be called with a list of nodes
// that passed the filtering phase. A plugin may use this data to update internal
// state or to generate logs/metrics.
type PreScorePlugin interface {
	Plugin
	// PreScore is called by the scheduling framework after a list of nodes
	// passed the filtering phase. All prescore plugins must return success or
	// the pod will be rejected
	PreScore(ctx context.Context, pod *Pod, nodes []*Node) error
}

// ScorePlugin is an interface that must be implemented by "Score" plugins to rank
// nodes that passed the filtering phase.
type ScorePlugin interface {
	Plugin
	// Score is called on each filtered node. It must return success and an integer
	// indicating the rank of the node. All scoring plugins must return success or
	// the pod will be rejected.
	Score(ctx context.Context, p *Pod, node *Node) (int64, error)
}

// ReservePlugin is an interface for plugins with Reserve and Unreserve
// methods. These are meant to update the state of the plugin. This concept
// used to be called 'assume' in the original scheduler. These plugins should
// return only Success or Error in Status.code. However, the scheduler accepts
// other valid codes as well. Anything other than Success will lead to
// rejection of the pod.
type ReservePlugin interface {
	Plugin
	// Reserve is called by the scheduling framework when the scheduler cache is
	// updated. If this method returns a failed Status, the scheduler will call
	// the Unreserve method for all enabled ReservePlugins.
	Reserve(ctx context.Context, p *Pod, nodeName string) error
	// Unreserve is called by the scheduling framework when a reserved pod was
	// rejected, an error occurred during reservation of subsequent plugins, or
	// in a later phase. The Unreserve method implementation must be idempotent
	// and may be called by the scheduler even if the corresponding Reserve
	// method for the same plugin was not called.
	// Unreserve(ctx context.Context, p *Pod, nodeName string)
}

// PermitPlugin is an interface that must be implemented by "Permit" plugins.
// These plugins are called before a pod is bound to a node.
type PermitPlugin interface {
	Plugin
	// Permit is called before binding a pod (and before prebind plugins). Permit
	// plugins are used to prevent or delay the binding of a Pod. A permit plugin
	// must return success or wait with timeout duration, or the pod will be rejected.
	// The pod will also be rejected if the wait timeout or the pod is rejected while
	// waiting. Note that if the plugin returns "wait", the framework will wait only
	// after running the remaining plugins given that no other plugin rejects the pod.
	Permit(ctx context.Context, p *Pod, nodeName string) (time.Duration, error)
}

// PreBindPlugin is an interface that must be implemented by "PreBind" plugins.
// These plugins are called before a pod being scheduled.
type PreBindPlugin interface {
	Plugin
	// PreBind is called before binding a pod. All prebind plugins must return
	// success or the pod will be rejected and won't be sent for binding.
	PreBind(ctx context.Context, p *Pod, nodeName string) error
}

// PostBindPlugin is an interface that must be implemented by "PostBind" plugins.
// These plugins are called after a pod is successfully bound to a node.
type PostBindPlugin interface {
	Plugin
	// PostBind is called after a pod is successfully bound. These plugins are
	// informational. A common application of this extension point is for cleaning
	// up. If a plugin needs to clean-up its state after a pod is scheduled and
	// bound, PostBind is the extension point that it should register.
	PostBind(ctx context.Context, p *Pod, nodeName string)
}

// BindPlugin is an interface that must be implemented by "Bind" plugins. Bind
// plugins are used to bind a pod to a Node.
type BindPlugin interface {
	Plugin
	// Bind plugins will not be called until all pre-bind plugins have completed. Each
	// bind plugin is called in the configured order. A bind plugin may choose whether
	// or not to handle the given Pod. If a bind plugin chooses to handle a Pod, the
	// remaining bind plugins are skipped. When a bind plugin does not handle a pod,
	// it must return Skip in its Status code. If a bind plugin returns an Error, the
	// pod is rejected and will not be bound.
	Bind(ctx context.Context, p *Pod, nodeName string) error
}

type frameworkImpl struct {
	preFilterPlugins []PreFilterPlugin
	filterPlugins    []FilterPlugin
	preScorePlugins  []PreScorePlugin
	scorePlugins     []ScorePlugin
	parallelizer     Parallelizer
	reservePlugins   []ReservePlugin
	permitPlugins    []PermitPlugin
	preBindPlugins   []PreBindPlugin
	postBindPlugins  []PostBindPlugin
	bindPlugins      []BindPlugin
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

// RunPreScorePlugins runs the set of configured pre-score plugins. If any
// of these plugins returns any status other than "Success", the given pod is rejected.
func (f *frameworkImpl) RunPreScorePlugins(
	ctx context.Context,
	pod *Pod,
	nodes []*Node,
) error {
	for _, pl := range f.preScorePlugins {
		err := f.runPreScorePlugin(ctx, pl, pod, nodes)
		if err != nil {
			return err
		}
	}

	return nil
}

// RunScorePlugins runs the set of configured scoring plugins. It returns a list that
// stores for each scoring plugin name the corresponding NodeScoreList(s).
// It also returns *Status, which is set to non-success if any of the plugins returns
// a non-success status.
func (f *frameworkImpl) RunScorePlugins(ctx context.Context, pod *Pod, nodes []*Node) (map[string][]NodeScore, error) {
	pluginToNodeScores := make(map[string][]NodeScore, len(f.scorePlugins))
	for _, pl := range f.scorePlugins {
		pluginToNodeScores[pl.Name()] = make([]NodeScore, len(nodes))
	}
	ctx, cancel := context.WithCancel(ctx)
	errCh := NewErrorChannel()

	// Run Score method for each node in parallel.
	f.Parallelizer().Until(ctx, len(nodes), func(index int) {
		for _, pl := range f.scorePlugins {
			nodeName := nodes[index].name
			s, err := f.runScorePlugin(ctx, pl, pod, nodes[index])
			if err != nil {
				errCh.SendErrorWithCancel(fmt.Errorf("plugin %q failed with: %w", pl.Name(), err), cancel)
				return
			}
			pluginToNodeScores[pl.Name()][index] = NodeScore{
				Name:  nodeName,
				Score: s,
			}
		}
	})
	if err := errCh.ReceiveError(); err != nil {
		return nil, err
	}

	return pluginToNodeScores, nil
}

func (f *frameworkImpl) RunReservePluginsReserve(ctx context.Context, pod *Pod, nodeName string) error {
	for _, pl := range f.reservePlugins {
		err := f.runReservePluginReserve(ctx, pl, pod, nodeName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *frameworkImpl) RunPermitPlugins(ctx context.Context, pod *Pod, nodeName string) error {
	for _, pl := range f.permitPlugins {
		_, err := f.runPermitPlugin(ctx, pl, pod, nodeName)
		if err != nil {
			return err
		}
	}
	return nil
}

// RunPreBindPlugins runs the set of configured prebind plugins. It returns a
// failure (bool) if any of the plugins returns an error. It also returns an
// error containing the rejection message or the error occurred in the plugin.
func (f *frameworkImpl) RunPreBindPlugins(ctx context.Context, pod *Pod, nodeName string) error {
	for _, pl := range f.preBindPlugins {
		if err := f.runPreBindPlugin(ctx, pl, pod, nodeName); err != nil {
			return err
		}
	}

	return nil
}

// RunPostBindPlugins runs the set of configured postbind plugins.
func (f *frameworkImpl) RunPostBindPlugins(ctx context.Context, pod *Pod, nodeName string) {
	for _, pl := range f.postBindPlugins {
		f.runPostBindPlugin(ctx, pl, pod, nodeName)
	}
}

// RunBindPlugins runs the set of configured bind plugins until one returns a non `Skip` status.
func (f *frameworkImpl) RunBindPlugins(ctx context.Context, pod *Pod, nodeName string) error {
	for _, bp := range f.bindPlugins {
		if err := f.runBindPlugin(ctx, bp, pod, nodeName); err != nil {
			continue
		}
		break
	}
	return nil

}

func (f *frameworkImpl) runPreFilterPlugin(ctx context.Context, pl PreFilterPlugin, pod *Pod) (*PreFilterResult, error) {
	return pl.PreFilter(ctx, pod)
}

func (f *frameworkImpl) runFilterPlugin(ctx context.Context, pl FilterPlugin, pod *Pod, nodeInfo *NodeInfo) error {
	return pl.Filter(ctx, pod, nodeInfo)
}

func (f *frameworkImpl) runPreScorePlugin(ctx context.Context, pl PreScorePlugin, pod *Pod, nodes []*Node) error {
	return pl.PreScore(ctx, pod, nodes)
}

func (f *frameworkImpl) runScorePlugin(ctx context.Context, pl ScorePlugin, pod *Pod, node *Node) (int64, error) {
	return pl.Score(ctx, pod, node)
}

func (f *frameworkImpl) runReservePluginReserve(ctx context.Context, pl ReservePlugin, pod *Pod, nodeName string) error {
	return pl.Reserve(ctx, pod, nodeName)
}

func (f *frameworkImpl) runPermitPlugin(ctx context.Context, pl PermitPlugin, pod *Pod, nodeName string) (time.Duration, error) {
	return pl.Permit(ctx, pod, nodeName)
}

func (f *frameworkImpl) runPreBindPlugin(ctx context.Context, pl PreBindPlugin, pod *Pod, nodeName string) error {
	return pl.PreBind(ctx, pod, nodeName)
}

func (f *frameworkImpl) runPostBindPlugin(ctx context.Context, pl PostBindPlugin, pod *Pod, nodeName string) {
	pl.PostBind(ctx, pod, nodeName)
}

func (f *frameworkImpl) runBindPlugin(ctx context.Context, bp BindPlugin, pod *Pod, nodeName string) error {
	return bp.Bind(ctx, pod, nodeName)
}

func (f *frameworkImpl) HasScorePlugins() bool {
	return true
}

func (f *frameworkImpl) HasFilterPlugins() bool {
	return true
}

// Parallelizer returns a parallelizer holding parallelism for scheduler.
func (f *frameworkImpl) Parallelizer() Parallelizer {
	return f.parallelizer
}

func NewFrameWork() Framework {
	return &frameworkImpl{
		parallelizer:     NewParallelizer(DefaultParallelism),
		preFilterPlugins: []PreFilterPlugin{&NodeName{}},
		filterPlugins:    []FilterPlugin{&NodeName{}},
		preScorePlugins:  []PreScorePlugin{&NodeName{}},
		scorePlugins:     []ScorePlugin{&NodeName{}},
		reservePlugins:   []ReservePlugin{&NodeName{}},
		permitPlugins:    []PermitPlugin{&NodeName{}},
		preBindPlugins:   []PreBindPlugin{&NodeName{}},
		postBindPlugins:  []PostBindPlugin{&NodeName{}},
		bindPlugins:      []BindPlugin{&NodeName{}},
	}
}

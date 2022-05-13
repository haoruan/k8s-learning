package main

import (
	"context"
	"fmt"
)

const (
	// SchedulerError is the reason recorded for events when an error occurs during scheduling a pod.
	SchedulerError = "SchedulerError"
	// Percentage of plugin metrics to be sampled.
	pluginMetricsSamplePercent = 10
	// minFeasibleNodesToFind is the minimum number of nodes that would be scored
	// in each scheduling cycle. This is a semi-arbitrary value to ensure that a
	// certain minimum of nodes are checked for feasibility. This in turn helps
	// ensure a minimum level of spreading.
	minFeasibleNodesToFind = 100
	// minFeasibleNodesPercentageToFind is the minimum percentage of nodes that
	// would be scored in each scheduling cycle. This is a semi-arbitrary value
	// to ensure that a certain minimum of nodes are checked for feasibility.
	// This in turn helps ensure a minimum level of spreading.
	minFeasibleNodesPercentageToFind = 5
)

// ErrNoNodesAvailable is used to describe the error that no nodes available to schedule pods.
var ErrNoNodesAvailable = fmt.Errorf("no nodes available to schedule pods")

// ScheduleResult represents the result of scheduling a pod.
type ScheduleResult struct {
	// Name of the selected node.
	SuggestedHost string
	// The number of nodes the scheduler evaluated the pod against in the filtering
	// phase and beyond.
	EvaluatedNodes int
	// The number of nodes out of the evaluated ones that fit the pod.
	FeasibleNodes int
}

type Scheduler struct {
	Cache              *cacheImpl
	nodeInfoSnapshot   *Snapshot
	NextPod            func() *PodInfo
	Profiles           map[string]Framework
	nextStartNodeIndex int
}

func NewScheduler() *Scheduler {
	return &Scheduler{}
}

func MakeNextPodFunc(queue *PriorityQueue) func() *PodInfo {
	return func() *PodInfo {
		pod := queue.Get()
		fmt.Printf("About to try and schedule pod %s\n", pod.name)
		return pod
	}
}

func (sched *Scheduler) Run(ctx context.Context) {
	sched.scheduleOne(ctx)
	<-ctx.Done()
}

func (sched *Scheduler) frameworkForPod(pod *PodInfo) Framework {
	return sched.Profiles[pod.schedulerName]
}

func (sched *Scheduler) scheduleOne(ctx context.Context) {
	// 1. Get the next pod for scheduling
	pod := sched.NextPod()

	// 2. Schedule a pod with provided algorithm
	fwk := sched.frameworkForPod(pod)

	fmt.Printf("Attempting to schedule pod %s\n", pod.name)
	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	scheduleResult, err := sched.schedulePod(schedulingCycleCtx, fwk, pod)

	// 3. If a pod fails to be scheduled due to FitError,
	//	  run preemption plugin in PostFilterPlugin (if the plugin is registered)
	//    to nominate a node where the pods can run.
	//    If preemption was successful, let the current pod be aware of the nominated node.
	//    Handle the error, get the next pod and start over.
	// 4. If the scheduling algorithm finds a suitable node,
	//    store the pod into the scheduler cache (AssumePod operation)
	//    and run plugins from the Reserve and Permit extension point in that order.
	//    In case any of the plugins fails, end the current scheduling cycle,
	//    increase relevant metrics and handle the scheduling error through the Error handler.
	// 5.Upon successfully running all extension points, proceed to the binding cycle.
	//   At the same time start processing another pod (if there’s any).
}

// schedulePod tries to schedule the given pod to one of the nodes in the node list.
// If it succeeds, it will return the name of the node.
// If it fails, it will return a FitError with reasons.
func (sched *Scheduler) schedulePod(ctx context.Context, fwk Framework, pod *Pod) (result ScheduleResult, err error) {
	if err := sched.Cache.UpdateSnapshot(sched.nodeInfoSnapshot); err != nil {
		return result, err
	}
	fmt.Printf("Snapshotting scheduler cache and node infos done\n")

	if sched.nodeInfoSnapshot.NumNodes() == 0 {
		return result, ErrNoNodesAvailable
	}

	feasibleNodes, err := sched.findNodesThatFitPod(ctx, fwk, pod)
	if err != nil {
		return result, err
	}
	fmt.Printf("Computing predicates done\n")
	numAllNodes := sched.nodeInfoSnapshot.NumNodes()

	if len(feasibleNodes) == 0 {
		return result, fmt.Errorf("FeasibleNodes not found for pod %s, numallnodes %d\n", pod.name, numAllNodes)
	}

	// When only one node after predicate, just use it.
	if len(feasibleNodes) == 1 {
		return ScheduleResult{
			SuggestedHost:  feasibleNodes[0].name,
			EvaluatedNodes: numAllNodes - 1,
			FeasibleNodes:  1,
		}, nil
	}

	priorityList, err := prioritizeNodes(ctx, sched.Extenders, fwk, state, pod, feasibleNodes)
	if err != nil {
		return result, err
	}

	host, err := selectHost(priorityList)
	fmt.Printf("Prioritizing done")

	return ScheduleResult{
		SuggestedHost:  host,
		EvaluatedNodes: numAllNodes - len(feasibleNodes),
		FeasibleNodes:  len(feasibleNodes),
	}, err
}

// Filters the nodes to find the ones that fit the pod based on the framework
// filter plugins and filter extenders.
func (sched *Scheduler) findNodesThatFitPod(ctx context.Context, fwk Framework, pod *Pod) ([]*Node, error) {
	// Run "prefilter" plugins.
	preRes, s := fwk.RunPreFilterPlugins(ctx, pod)
	allNodes, err := sched.nodeInfoSnapshot.List()

	if s != nil {
		return nil, s
	}

	nodes := allNodes
	if !preRes.AllNodes() {
		nodes = make([]*NodeInfo, 0, len(preRes.NodeNames))
		for n := range preRes.NodeNames {
			nInfo, err := sched.nodeInfoSnapshot.Get(n)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, nInfo)
		}
	}

	feasibleNodes, err := sched.findNodesThatPassFilters(ctx, fwk, pod, nodes)
	if err != nil {
		return nil, err
	}

	feasibleNodes, err = findNodesThatPassExtenders(pod, feasibleNodes)
	if err != nil {
		return nil, err
	}
	return feasibleNodes, nil

}

// findNodesThatPassFilters finds the nodes that fit the filter plugins.
func (sched *Scheduler) findNodesThatPassFilters(
	ctx context.Context,
	fwk Framework,
	pod *Pod,
	nodes []*NodeInfo) ([]*Node, error) {
	numNodesToFind := sched.numFeasibleNodesToFind(int32(len(nodes)))

	// Create feasible list with enough space to avoid growing it
	// and allow assigning.
	feasibleNodes := make([]*Node, numNodesToFind)

	if !fwk.HasFilterPlugins() {
		length := len(nodes)
		for i := range feasibleNodes {
			feasibleNodes[i] = nodes[(sched.nextStartNodeIndex+i)%length].node
		}
		sched.nextStartNodeIndex = (sched.nextStartNodeIndex + len(feasibleNodes)) % length
		return feasibleNodes, nil
	}

}

// numFeasibleNodesToFind returns the number of feasible nodes that once found, the scheduler stops
// its search for more feasible nodes.
func (sched *Scheduler) numFeasibleNodesToFind(numAllNodes int32) (numNodes int32) {
	if numAllNodes < minFeasibleNodesToFind {
		return numAllNodes
	}

	adaptivePercentage := int32(50)
	//if adaptivePercentage <= 0 {
	//	basePercentageOfNodesToScore := int32(50)
	//	adaptivePercentage = basePercentageOfNodesToScore - numAllNodes/125
	//	if adaptivePercentage < minFeasibleNodesPercentageToFind {
	//		adaptivePercentage = minFeasibleNodesPercentageToFind
	//	}
	//}

	numNodes = numAllNodes * adaptivePercentage / 100
	if numNodes < minFeasibleNodesToFind {
		return minFeasibleNodesToFind
	}

	return numNodes
}

func findNodesThatPassExtenders(pod *Pod, feasibleNodes []*Node) ([]*Node, error) {
	return feasibleNodes, nil
}

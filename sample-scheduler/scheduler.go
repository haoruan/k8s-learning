package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// SchedulerError is the reason recorded for events when an error occurs during scheduling a pod.
	SchedulerError = "SchedulerError"
	// Percentage of plugin metrics to be sampled.
	// pluginMetricsSamplePercent = 10
	// minFeasibleNodesToFind is the minimum number of nodes that would be scored
	// in each scheduling cycle. This is a semi-arbitrary value to ensure that a
	// certain minimum of nodes are checked for feasibility. This in turn helps
	// ensure a minimum level of spreading.
	minFeasibleNodesToFind = 100
	// minFeasibleNodesPercentageToFind is the minimum percentage of nodes that
	// would be scored in each scheduling cycle. This is a semi-arbitrary value
	// to ensure that a certain minimum of nodes are checked for feasibility.
	// This in turn helps ensure a minimum level of spreading.
	// minFeasibleNodesPercentageToFind = 5
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
	Extenders          []Extender
	nextStartNodeIndex int
	SchedulingQueue    *PriorityQueue
	wg                 sync.WaitGroup

	// Error is called if there is an error. It is passed the pod in
	// question, and the error
	Error func(*PodInfo, error)
}

func NewScheduler(queue *PriorityQueue) *Scheduler {
	profiles := map[string]Framework{
		"default-scheduler": NewFrameWork(),
	}
	return &Scheduler{
		Cache:            NewCache(),
		nodeInfoSnapshot: NewEmptySnapshot(),
		NextPod:          MakeNextPodFunc(queue),
		Profiles:         profiles,
		SchedulingQueue:  queue,
		Error:            MakeDefaultErrorFunc(queue),
	}
}

func MakeNextPodFunc(queue *PriorityQueue) func() *PodInfo {
	return func() *PodInfo {
		if queue.Len() > 0 {
			podInfo := queue.Pop()
			fmt.Printf("About to try and schedule pod %s\n", podInfo.pod.name)
			return podInfo
		} else {
			return nil
		}
	}
}

// MakeDefaultErrorFunc construct a function to handle pod scheduler error
func MakeDefaultErrorFunc(podQueue *PriorityQueue) func(*PodInfo, error) {
	return func(podInfo *PodInfo, err error) {
		if err := podQueue.AddUnschedulableIfNotPresent(podInfo, podQueue.SchedulingCycle()); err != nil {
			fmt.Printf("Error occurred: %s\n", err)
		}
	}
}

func (sched *Scheduler) Run(ctx context.Context) {
	sched.SchedulingQueue.Run()
	sched.wg.Add(1)
	defer fmt.Printf("scheduler stopped\n")

loop:
	for {
		select {
		case <-ctx.Done():
			sched.wg.Done()
			break loop
		default:
			sched.scheduleOne(ctx)
			time.Sleep(time.Second)
		}
	}
}

func (sched *Scheduler) Close() {
	fmt.Printf("Closing scheduler\n")
	defer fmt.Printf("Closed scheduler\n")
	sched.SchedulingQueue.Close()
	sched.wg.Wait()
}

func (sched *Scheduler) frameworkForPod(pod *PodInfo) Framework {
	return sched.Profiles[pod.schedulerName]
}

func (sched *Scheduler) scheduleOne(ctx context.Context) {
	// 1. Get the next pod for scheduling
	podInfo := sched.NextPod()
	if podInfo == nil {
		return
	}

	pod := podInfo.pod

	// 2. Schedule a pod with provided algorithm
	fwk := sched.frameworkForPod(podInfo)

	fmt.Printf("Attempting to schedule pod %s\n", pod.name)
	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	scheduleResult, err := sched.schedulePod(schedulingCycleCtx, fwk, pod)

	// 3. If a pod fails to be scheduled due to FitError,
	//	  run preemption plugin in PostFilterPlugin (if the plugin is registered)
	//    to nominate a node where the pods can run.
	//    If preemption was successful, let the current pod be aware of the nominated node.
	//    Handle the error, get the next pod and start over.
	if err != nil {
		sched.Error(podInfo, err)
		fmt.Printf("%s\n", err)
		return
	}

	// 4. If the scheduling algorithm finds a suitable node,
	//    store the pod into the scheduler cache (AssumePod operation)
	//    and run plugins from the Reserve and Permit extension point in that order.
	//    In case any of the plugins fails, end the current scheduling cycle,
	//    increase relevant metrics and handle the scheduling error through the Error handler.

	// Tell the cache to assume that a pod now is running on a given node, even though it hasn't been bound yet.
	// This allows us to keep scheduling without waiting on binding to occur.
	assumedPodInfo := podInfo
	assumedPod := assumedPodInfo.pod
	// assume modifies `assumedPod` by setting NodeName=scheduleResult.SuggestedHost
	err = sched.assume(assumedPod, scheduleResult.SuggestedHost)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	// Run the Reserve method of reserve plugins.
	if err = fwk.RunReservePluginsReserve(schedulingCycleCtx, assumedPod, scheduleResult.SuggestedHost); err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	// Run "permit" plugins.
	if err = fwk.RunPermitPlugins(schedulingCycleCtx, assumedPod, scheduleResult.SuggestedHost); err != nil {
		fmt.Printf("%s\n", err)
	}

	// sched.SchedulingQueue.Activate(pod)

	// 5.Upon successfully running all extension points, proceed to the binding cycle.
	//   At the same time start processing another pod (if there???s any).
	go func() {
		bindingCycleCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Run "prebind" plugins.
		if err := fwk.RunPreBindPlugins(bindingCycleCtx, assumedPod, scheduleResult.SuggestedHost); err != nil {
			fmt.Printf("%s\n", err)
		}

		if err := sched.bind(bindingCycleCtx, fwk, assumedPod, scheduleResult.SuggestedHost); err != nil {
			return
		}

		// Run "postbind" plugins.
		fwk.RunPostBindPlugins(bindingCycleCtx, assumedPod, scheduleResult.SuggestedHost)
	}()
}

// bind binds a pod to a given node defined in a binding object.
// The precedence for binding is: (1) extenders and (2) framework plugins.
// We expect this to run asynchronously, so we handle binding metrics internally.
func (sched *Scheduler) bind(ctx context.Context, fwk Framework, assumed *Pod, targetNode string) (err error) {
	defer func() {
		sched.finishBinding(fwk, assumed, targetNode, err)
	}()

	bound, err := sched.extendersBinding(assumed, targetNode)
	if bound {
		return err
	}
	if err = fwk.RunBindPlugins(ctx, assumed, targetNode); err != nil {
		return fmt.Errorf("bind status: %s", err)
	}

	return nil
}

func (sched *Scheduler) extendersBinding(pod *Pod, node string) (bool, error) {
	return false, nil
}

func (sched *Scheduler) finishBinding(fwk Framework, assumed *Pod, targetNode string, err error) {
	if finErr := sched.Cache.FinishBinding(assumed); finErr != nil {
		fmt.Printf("Scheduler cache FinishBinding failed: %s\n", finErr)
	}
	if err != nil {
		fmt.Printf("Failed to bind pod %s\n", assumed.name)
	}
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
		return result, fmt.Errorf("FeasibleNodes not found for pod %s, numallnodes %d", pod.name, numAllNodes)
	}

	// When only one node after predicate, just use it.
	if len(feasibleNodes) == 1 {
		return ScheduleResult{
			SuggestedHost:  feasibleNodes[0].name,
			EvaluatedNodes: numAllNodes - 1,
			FeasibleNodes:  1,
		}, nil
	}

	priorityList, err := prioritizeNodes(ctx, sched.Extenders, fwk, pod, feasibleNodes)
	if err != nil {
		return result, err
	}

	host, err := selectHost(priorityList)
	fmt.Printf("Prioritizing done\n")

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
	allNodes, _ := sched.nodeInfoSnapshot.List()

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

	feasibleNodes, err = findNodesThatPassExtenders(sched.Extenders, pod, feasibleNodes)
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

	ctx, cancel := context.WithCancel(ctx)
	var feasibleNodesLen int32

	checkNode := func(i int) {
		// We check the nodes starting from where we left off in the previous scheduling cycle,
		// this is to make sure all nodes have the same chance of being examined across pods.
		nodeInfo := nodes[(sched.nextStartNodeIndex+i)%len(nodes)]
		err := fwk.RunFilterPlugins(ctx, pod, nodeInfo)
		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}

		length := atomic.AddInt32(&feasibleNodesLen, 1)
		if length > numNodesToFind {
			cancel()
			atomic.AddInt32(&feasibleNodesLen, -1)
		} else {
			feasibleNodes[length-1] = nodeInfo.node
		}
	}

	// Stops searching for more nodes once the configured number of feasible nodes
	// are found.
	fwk.Parallelizer().Until(ctx, len(nodes), checkNode)
	processedNodes := int(feasibleNodesLen) + int(numNodesToFind)
	sched.nextStartNodeIndex = (sched.nextStartNodeIndex + processedNodes) % len(nodes)

	feasibleNodes = feasibleNodes[:feasibleNodesLen]
	return feasibleNodes, nil
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

func findNodesThatPassExtenders(extenders []Extender, pod *Pod, feasibleNodes []*Node) ([]*Node, error) {
	// Extenders are called sequentially.
	// Nodes in original feasibleNodes can be excluded in one extender, and pass on to the next
	// extender in a decreasing manner.
	for _, extender := range extenders {
		if len(feasibleNodes) == 0 {
			break
		}
		if !extender.IsInterested(pod) {
			continue
		}

		// Status of failed nodes in failedAndUnresolvableMap will be added or overwritten in <statuses>,
		// so that the scheduler framework can respect the UnschedulableAndUnresolvable status for
		// particular nodes, and this may eventually improve preemption efficiency.
		// Note: users are recommended to configure the extenders that may return UnschedulableAndUnresolvable
		// status ahead of others.
		feasibleList, err := extender.Filter(pod, feasibleNodes)
		if err != nil {
			if extender.IsIgnorable() {
				fmt.Printf("Skipping extender as it returned error and has ignorable flag set, extender %s, err %s\n", extender.Name(), err)
				continue
			}
			return nil, err
		}

		feasibleNodes = feasibleList
	}
	return feasibleNodes, nil
}

// prioritizeNodes prioritizes the nodes by running the score plugins,
// which return a score for each node from the call to RunScorePlugins().
// The scores from each plugin are added together to make the score for that node, then
// any extenders are run as well.
// All scores are finally combined (added) to get the total weighted scores of all nodes
func prioritizeNodes(
	ctx context.Context,
	extenders []Extender,
	fwk Framework,
	pod *Pod,
	nodes []*Node,
) ([]NodeScore, error) {
	// If no priority configs are provided, then all nodes will have a score of one.
	// This is required to generate the priority list in the required format
	if len(extenders) == 0 && !fwk.HasScorePlugins() {
		result := make([]NodeScore, 0, len(nodes))
		for i := range nodes {
			result = append(result, NodeScore{
				Name:  nodes[i].name,
				Score: 1,
			})
		}
		return result, nil
	}

	// Run PreScore plugins.
	err := fwk.RunPreScorePlugins(ctx, pod, nodes)
	if err != nil {
		return nil, err
	}

	// Run the Score plugins.
	scoresMap, err := fwk.RunScorePlugins(ctx, pod, nodes)
	if err != nil {
		return nil, err
	}

	// Summarize all scores.
	result := make([]NodeScore, 0, len(nodes))
	for i := range nodes {
		result = append(result, NodeScore{Name: nodes[i].name, Score: 0})
		for j := range scoresMap {
			result[i].Score += scoresMap[j][i].Score
		}
	}

	if len(extenders) != 0 && nodes != nil {
		var mu sync.Mutex
		var wg sync.WaitGroup
		combinedScores := make(map[string]int64, len(nodes))
		for i := range extenders {
			if !extenders[i].IsInterested(pod) {
				continue
			}
			wg.Add(1)
			go func(extIndex int) {
				defer wg.Done()
				prioritizedList, weight, err := extenders[extIndex].Prioritize(pod, nodes)
				if err != nil {
					// Prioritization errors from extender can be ignored, let k8s/other extenders determine the priorities
					fmt.Printf("Failed to run extender's priority function. No score given by this extender, error %s, pod %s, extender %s\n", err, pod.name, extenders[extIndex].Name())
					return
				}
				mu.Lock()
				for _, v := range prioritizedList {
					host, score := v.Name, v.Score
					fmt.Printf("Extender scored node for pod %s, extender %s, node %s, score %d\n", pod.name, extenders[extIndex].Name(), host, score)
					combinedScores[host] += score * weight
				}
				mu.Unlock()
			}(i)
		}
		// wait for all go routines to finish
		wg.Wait()
		for i := range result {
			result[i].Score += combinedScores[result[i].Name]
		}
	}

	return result, nil
}

// selectHost takes a prioritized list of nodes and then picks one
// in a reservoir sampling manner from the nodes that had the highest score.
func selectHost(nodeScoreList []NodeScore) (string, error) {
	if len(nodeScoreList) == 0 {
		return "", fmt.Errorf("empty priorityList")
	}
	maxScore := nodeScoreList[0].Score
	selected := nodeScoreList[0].Name
	cntOfMaxScore := 1
	for _, ns := range nodeScoreList[1:] {
		if ns.Score > maxScore {
			maxScore = ns.Score
			selected = ns.Name
			cntOfMaxScore = 1
		} else if ns.Score == maxScore {
			cntOfMaxScore++
			if rand.Intn(cntOfMaxScore) == 0 {
				// Replace the candidate with probability of 1/cntOfMaxScore
				selected = ns.Name
			}
		}
	}
	return selected, nil
}

// assume signals to the cache that a pod is already in the cache, so that binding can be asynchronous.
// assume modifies `assumed`.
func (sched *Scheduler) assume(assumed *Pod, host string) error {
	// Optimistically assume that the binding will succeed and send it to apiserver
	// in the background.
	// If the binding fails, scheduler will release resources allocated to assumed pod
	// immediately.
	assumed.nodeName = host

	if err := sched.Cache.AssumePod(assumed); err != nil {
		err := fmt.Errorf("Scheduler cache AssumePod failed")
		return err
	} else {
		fmt.Printf("pod %s is assumed on node %s\n", assumed.name, assumed.nodeName)
	}
	// if "assumed" is a nominated pod, we should remove it from internal cache
	//if sched.SchedulingQueue != nil {
	//	sched.SchedulingQueue.DeleteNominatedPodIfExists(assumed)
	//}

	return nil
}

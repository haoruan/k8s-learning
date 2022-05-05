package main

import (
	"context"
	"fmt"
)

type Scheduler struct {
	NextPod func() *PodInfo
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

func (sched *Scheduler) scheduleOne(ctx context.Context) {
	// 1. Get the next pod for scheduling
	pod := sched.NextPod()

	// 2. Schedule a pod with provided algorithm
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

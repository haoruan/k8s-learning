package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

var onlyOneSignalHandler = make(chan struct{})
var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

func SetupSignalContext() context.Context {
	close(onlyOneSignalHandler)

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		cancel()
		<-c
		os.Exit(1)
	}()

	return ctx
}

func createNodes(n int) []*Node {
	nodes := []*Node{}

	for i := 0; i < n; i++ {
		for j := 0; j < 3; j++ {
			node := &Node{
				fmt.Sprintf("node%d-%d", i, j),
				[]ContainerImage{},
				fmt.Sprintf("zone%d", i),
				int64(j),
			}
			nodes = append(nodes, node)

		}
	}

	return nodes
}

func createPodInfos(n int) []*PodInfo {
	podInfos := []*PodInfo{}
	for i := 0; i < n; i++ {
		pod := &Pod{
			uid:      fmt.Sprintf("%d", i+1),
			name:     fmt.Sprintf("pod%d", i+1),
			nodeName: fmt.Sprintf("node%d", i),
		}
		podInfo := NewPodInfo(pod)
		podInfo.schedulerName = "default-scheduler"
		podInfos = append(podInfos, podInfo)
	}

	return podInfos
}

func main() {
	queue := &PriorityQueue{}
	ctx := SetupSignalContext()
	sched := NewScheduler(queue)

	for _, node := range createNodes(10) {
		sched.Cache.AddNode(node)
	}

	for _, podInfo := range createPodInfos(10) {
		queue.Add(podInfo)
	}

	sched.Run(ctx)
}

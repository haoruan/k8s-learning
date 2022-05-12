package main

// Snapshot is a snapshot of cache NodeInfo and NodeTree order. The scheduler takes a
// snapshot at the beginning of each scheduling cycle and uses it for its operations in that cycle.
type Snapshot struct {
	// nodeInfoMap a map of node name to a snapshot of its NodeInfo.
	nodeInfoMap map[string]*NodeInfo
	// nodeInfoList is the list of nodes as ordered in the cache's nodeTree.
	nodeInfoList []*NodeInfo
	generation   int64
}

// NewEmptySnapshot initializes a Snapshot struct and returns it.
func NewEmptySnapshot() *Snapshot {
	return &Snapshot{
		nodeInfoMap: make(map[string]*NodeInfo),
	}
}

// NewSnapshot initializes a Snapshot struct and returns it.
func NewSnapshot(pods []*Pod, nodes []*Node) *Snapshot {
	nodeInfoMap := createNodeInfoMap(pods, nodes)
	nodeInfoList := make([]*NodeInfo, 0, len(nodeInfoMap))
	for _, v := range nodeInfoMap {
		nodeInfoList = append(nodeInfoList, v)
	}

	s := NewEmptySnapshot()
	s.nodeInfoMap = nodeInfoMap
	s.nodeInfoList = nodeInfoList

	return s
}

// createNodeInfoMap obtains a list of pods and pivots that list into a map
// where the keys are node names and the values are the aggregated information
// for that node.
func createNodeInfoMap(pods []*Pod, nodes []*Node) map[string]*NodeInfo {
	nodeNameToInfo := make(map[string]*NodeInfo)
	for _, pod := range pods {
		if _, ok := nodeNameToInfo[pod.nodeName]; !ok {
			nodeNameToInfo[pod.nodeName] = NewNodeInfo()
		}
		nodeNameToInfo[pod.nodeName].AddPod(pod)
	}
	imageExistenceMap := createImageExistenceMap(nodes)

	for _, node := range nodes {
		if _, ok := nodeNameToInfo[node.name]; !ok {
			nodeNameToInfo[node.name] = NewNodeInfo()
		}
		nodeInfo := nodeNameToInfo[node.name]
		nodeInfo.node = node
		nodeInfo.ImageStates = getNodeImageStates(node, imageExistenceMap)
	}
	return nodeNameToInfo
}

// createImageExistenceMap returns a map recording on which nodes the images exist, keyed by the images' names.
func createImageExistenceMap(nodes []*Node) map[string]map[string]struct{} {
	imageExistenceMap := make(map[string]map[string]struct{})
	for _, node := range nodes {
		for _, image := range node.images {
			if _, ok := imageExistenceMap[image.Name]; !ok {
				imageExistenceMap[image.Name] = make(map[string]struct{})
			}
			imageExistenceMap[image.Name][node.name] = struct{}{}
		}
	}
	return imageExistenceMap
}

// getNodeImageStates returns the given node's image states based on the given imageExistence map.
func getNodeImageStates(node *Node, imageExistenceMap map[string]map[string]struct{}) map[string]*ImageStateSummary {
	imageStates := make(map[string]*ImageStateSummary)

	for _, image := range node.images {
		imageStates[image.Name] = &ImageStateSummary{
			Size:     image.SizeBytes,
			NumNodes: len(imageExistenceMap[image.Name]),
		}
	}
	return imageStates
}

// NumNodes returns the number of nodes in the snapshot.
func (s *Snapshot) NumNodes() int {
	return len(s.nodeInfoList)
}

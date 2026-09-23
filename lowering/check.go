package lowering

import (
	"slices"

	ps "github.com/HeaInSeo/PipelineStore"
)

// CheckSpec verifies a RunSpec independently of how it was produced. It covers
// the structural rules JUMI's ValidateExecutableRunSpec enforces (non-empty
// RunID and nodes, unique node IDs, required image, well-formed acyclic edges,
// non-empty binding fields) plus the PIPE-D2 v1 explicit policy and the
// same-source invariant JUMI does not check: BindingName equals
// ChildInputName, which is a declared consumer input; the producer node exists
// and ProducerOutputName is one of its outputs; the [producer, consumer]
// dependency edge exists; and every edge carries at least one binding.
//
// Several bindings may share one edge (distinct ports between the same node
// pair); edges and bindings are not required to be 1:1.
func CheckSpec(s RunSpec) error {
	if s.Run.RunID == "" {
		return missing("run.runId")
	}
	if err := checkTimestamp(s.Run.SubmittedAt, "run.submittedAt"); err != nil {
		return err
	}
	if s.Run.FailurePolicy.Mode != FailureModeFailFast {
		return newError(ps.CodeInvalidContract, "run.failurePolicy.mode is %q, want %q", s.Run.FailurePolicy.Mode, FailureModeFailFast)
	}
	if s.Defaults.RetryPolicy.MaxAttempts != MaxAttemptsV1 {
		return newError(ps.CodeInvalidContract, "defaults.retryPolicy.maxAttempts is %d, want %d", s.Defaults.RetryPolicy.MaxAttempts, MaxAttemptsV1)
	}
	if len(s.Graph.Nodes) == 0 {
		return newError(ps.CodeInvalidContract, "graph.nodes must not be empty")
	}

	nodes := make(map[string]*Node, len(s.Graph.Nodes))
	for i := range s.Graph.Nodes {
		n := &s.Graph.Nodes[i]
		if n.NodeID == "" {
			return newError(ps.CodeInvalidContract, "graph.nodes[%d] has empty nodeId", i)
		}
		if _, dup := nodes[n.NodeID]; dup {
			return newError(ps.CodeDuplicate, "duplicate nodeId %q", n.NodeID)
		}
		if !pinnedImage.MatchString(n.Image) {
			return newError(CodeRunnableUnresolved, "node %q: image %q is not pinned by sha256 digest", n.NodeID, n.Image)
		}
		nodes[n.NodeID] = n
	}

	edgeSet := make(map[[2]string]bool, len(s.Graph.Edges))
	adj := make(map[string][]string, len(nodes))
	for _, e := range s.Graph.Edges {
		if len(e) != 2 {
			return newError(ps.CodeEdgeInvalid, "edge %v must have exactly two endpoints", e)
		}
		pair := [2]string{e[0], e[1]}
		if pair[0] == pair[1] {
			return newError(ps.CodeEdgeInvalid, "self-loop on %q", pair[0])
		}
		if nodes[pair[0]] == nil || nodes[pair[1]] == nil {
			return newError(ps.CodeEdgeInvalid, "edge %v references an unknown node", e)
		}
		if _, dup := edgeSet[pair]; dup {
			return newError(ps.CodeDuplicate, "duplicate edge %v", e)
		}
		edgeSet[pair] = false
		adj[pair[0]] = append(adj[pair[0]], pair[1])
	}
	if node, ok := findCycle(s.Graph.Nodes, adj); ok {
		return newError(ps.CodeGraphCycle, "graph edges form a cycle through %q", node)
	}

	for i := range s.Graph.Nodes {
		consumer := &s.Graph.Nodes[i]
		names := make(map[string]bool, len(consumer.ArtifactBindings))
		for _, b := range consumer.ArtifactBindings {
			if b.BindingName == "" || b.ProducerNodeID == "" || b.ProducerOutputName == "" {
				return newError(ps.CodeInvalidContract, "node %q has a binding with an empty bindingName, producerNodeId or producerOutputName", consumer.NodeID)
			}
			if names[b.BindingName] {
				return sameSource("node %q binds input %q more than once", consumer.NodeID, b.BindingName)
			}
			names[b.BindingName] = true
			if b.ChildInputName != b.BindingName {
				return sameSource("node %q binding %q has childInputName %q", consumer.NodeID, b.BindingName, b.ChildInputName)
			}
			if !slices.Contains(consumer.Inputs, b.ChildInputName) {
				return sameSource("node %q binding %q targets an undeclared input", consumer.NodeID, b.BindingName)
			}
			producer := nodes[b.ProducerNodeID]
			if producer == nil {
				return sameSource("node %q binding %q names unknown producer %q", consumer.NodeID, b.BindingName, b.ProducerNodeID)
			}
			if !slices.Contains(producer.Outputs, b.ProducerOutputName) {
				return sameSource("node %q binding %q names undeclared output %q of %q", consumer.NodeID, b.BindingName, b.ProducerOutputName, b.ProducerNodeID)
			}
			pair := [2]string{b.ProducerNodeID, consumer.NodeID}
			if _, ok := edgeSet[pair]; !ok {
				return sameSource("node %q binding %q has no edge from %q", consumer.NodeID, b.BindingName, b.ProducerNodeID)
			}
			edgeSet[pair] = true
		}
	}
	for _, e := range s.Graph.Edges {
		if !edgeSet[[2]string{e[0], e[1]}] {
			return sameSource("edge %v carries no artifact binding", e)
		}
	}
	return nil
}

// findCycle returns a node on a cycle of the directed graph adj, if any.
func findCycle(nodes []Node, adj map[string][]string) (string, bool) {
	const (
		white = iota
		gray
		black
	)
	color := make(map[string]int, len(nodes))
	var visit func(string) (string, bool)
	visit = func(u string) (string, bool) {
		color[u] = gray
		for _, v := range adj[u] {
			switch color[v] {
			case gray:
				return v, true
			case white:
				if node, ok := visit(v); ok {
					return node, true
				}
			}
		}
		color[u] = black
		return "", false
	}
	for _, n := range nodes {
		if color[n.NodeID] == white {
			if node, ok := visit(n.NodeID); ok {
				return node, true
			}
		}
	}
	return "", false
}

func sameSource(format string, args ...any) error {
	return newError(CodeSameSourceViolation, format, args...)
}

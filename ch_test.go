package consistent_hashing

import (
	"testing"
)

// Test adding nodes to the consistent hash ring.
func TestAddNode(t *testing.T) {
	ch := NewConsistentHash(3)
	ch.AddNode("NodeA")
	ch.AddNode("NodeB")

	if len(ch.nodeSet) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(ch.nodeSet))
	}

	if _, ok := ch.nodeSet["NodeA"]; !ok {
		t.Error("NodeA not found in nodeSet")
	}

	if _, ok := ch.nodeSet["NodeB"]; !ok {
		t.Error("NodeB not found in nodeSet")
	}
}

// Test removing nodes from the consistent hash ring.
func TestRemoveNode(t *testing.T) {
	ch := NewConsistentHash(3)
	ch.AddNode("NodeA")
	ch.AddNode("NodeB")
	ch.RemoveNode("NodeA")

	if len(ch.nodeSet) != 1 {
		t.Errorf("Expected 1 node, got %d", len(ch.nodeSet))
	}

	if _, ok := ch.nodeSet["NodeA"]; ok {
		t.Error("NodeA should have been removed from nodeSet")
	}

	if _, ok := ch.nodeSet["NodeB"]; !ok {
		t.Error("NodeB not found in nodeSet")
	}
}

// Test getting the appropriate node for a given key.
func TestGetNode(t *testing.T) {
	ch := NewConsistentHash(3)
	ch.AddNode("NodeA")
	ch.AddNode("NodeB")
	ch.AddNode("NodeC")

	key := "my-key"
	node, ok := ch.GetNode(key)
	if !ok {
		t.Error("Expected to find a node for key 'my-key'")
	}

	if node == "" {
		t.Error("Expected a non-empty node name")
	}
}

// Test edge case where no nodes are present.
func TestGetNodeNoNodes(t *testing.T) {
	ch := NewConsistentHash(3)

	key := "my-key"
	_, ok := ch.GetNode(key)
	if ok {
		t.Error("Expected not to find a node for key 'my-key' when no nodes are present")
	}
}

// Test adding and removing nodes dynamically.
func TestDynamicAddRemoveNodes(t *testing.T) {
	ch := NewConsistentHash(3)
	ch.AddNode("NodeA")
	ch.AddNode("NodeB")

	key := "tottenham"
	node1, ok1 := ch.GetNode(key)
	if !ok1 {
		t.Error("Expected to find a node for key")
	}
	ch.RemoveNode(node1)
	node2, ok2 := ch.GetNode(key)
	if !ok2 {
		t.Error("Expected to find a node for key")
	}
	if node2 == node1 {
		t.Error("Expected different nodes after removal")
	}
	ch.RemoveNode(node2)

	ch.AddNode(node1)
	ch.AddNode(node2)
	node3, ok3 := ch.GetNode(key)
	if !ok3 {
		t.Error("Expected to find a node for key")
	}
	if node3 != node1 {
		t.Error("Expected same as initial node")
	}
}

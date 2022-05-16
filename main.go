package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/xlab/treeprint"
)

const (
	ghostKey string = "_ghost"
	rootKey  string = "_root"
)

// VectorClock is a simplified version of a vector clock,
// where the client id and time are just simple integers.
type VectorClock map[int]int

// Before checks whether 'v' happened before 'other'.
// It uses the definition of ordering from: https://en.wikipedia.org/wiki/Vector_clock
// i.e. 'v' is less than 'y' if and only if 'v' is less than or equal to 'other' for all dimensions,
// and at least one of those relationships is strictly smaller.
func (v VectorClock) Before(other VectorClock) bool {
	strictlySmaller := false

	for id, vDT := range v {
		if otherDT, existsInOther := other[id]; existsInOther && vDT > otherDT {
			return false
		} else if existsInOther && vDT < otherDT {
			strictlySmaller = true
		}
	}

	// variation on the algorithm: they equal in all known dimensions then
	// use the number of dimensions as a tie-break. (we want ordering to
	// always be deterministic).
	if !strictlySmaller && len(v) < len(other) {
		return true
	}

	return strictlySmaller
}

// Event is an update or delete event that adds 'item' to 'target item'.
type Event struct {
	// Type is 'update' or 'delete'.
	Type string
	// VectorClock is the VectorClock of this event.
	VectorClock   VectorClock
	ItemKey       string
	TargetItemKey string
}

// CRDT is the main CRDT structure.
type CRDT struct {
	nodes map[string]*node
}

func NewCRDT() *CRDT {
	ghost := &node{
		key: ghostKey,
	}

	root := &node{
		key: rootKey,
	}

	root.AttachChild(ghost)

	return &CRDT{
		nodes: map[string]*node{
			rootKey:  root,
			ghostKey: ghost,
		},
	}
}

// Traverse returns a channel that will contain nodes in the order the
// CRDT should be in.
// It is implemented as a Depth First Search over the nodes, skipping the
// root, ghost and children of ghost nodes (as an implementation detail).
func (crdt *CRDT) Traverse() <-chan *node {
	ch := make(chan *node)
	go func() {
		defer close(ch)
		root := crdt.nodes[rootKey]
		queue := []*node{root}
		for len(queue) > 0 {
			n := queue[0]
			children := make([]*node, len(n.children))
			copy(children, n.children)
			queue = append(children, queue[1:]...)
			if n.key == rootKey || n.key == ghostKey || n.parent.key == ghostKey {
				continue
			}
			ch <- n
		}
	}()
	return ch
}

// Apply adds an Event into the CRDT.
func (crdt *CRDT) Apply(e Event) {
	if e.Type == "update" {
		crdt.update(e)
	} else {
		crdt.delete(e)
	}
}

func (crdt *CRDT) update(e Event) {
	item, exists := crdt.nodes[e.ItemKey]
	if !exists {
		// if the item doesn't exist let's create a new node
		// and set its vector clock to the one of the event.
		item = crdt.newNode(e.ItemKey, e.VectorClock)
	}

	// if the event happened before the latest time the item knows
	// about, we don't do anything
	if e.VectorClock.Before(item.latestVectorClock) {
		return
	}

	// set the latest vector clock this item knows about to be the
	// one for this event.
	item.latestVectorClock = e.VectorClock

	target, exists := crdt.nodes[e.TargetItemKey]
	if !exists {
		// if the target doesn't exist, we create a 'ghost' node,
		// that is, one that doesn't have a vector clock (it will come
		// at the end of the ordered children list) and we set the target
		// to be a child of the ghost node so that the target does not
		// appear in the traversal (we don't know what this node is at this
		// point in time!)
		target = crdt.newNode(e.TargetItemKey, VectorClock{})
		crdt.addGhostNode(target)
	}

	target.AttachChild(item)
}

func (crdt *CRDT) delete(e Event) {
	item, exists := crdt.nodes[e.ItemKey]
	if !exists {
		// even if the item doesn't exist, we need to create it
		// so that it can become a 'ghost' node, that is, one that
		// won't be output by the traversal function (it has been deleted, then).
		// we need this incase any nodes need to be attached to this deleted node
		// when we receive out of order messages.
		item = crdt.newNode(e.ItemKey, e.VectorClock)
	}

	// if the event happened before the latest time the item knows
	// about, we don't do anything
	if e.VectorClock.Before(item.latestVectorClock) {
		return
	}

	// set the latest vector clock this item knows about to be the
	// one for this event.
	item.latestVectorClock = e.VectorClock

	// move the children nodes of the deleted node to the parent
	// of the deleted node, if the parent exists and the parent isn't
	// the ghost. (We don't move if the parent is the ghost because
	// then they'd become 'ghost' nodes, which isn't the desired behaviour).
	if item.parent != nil && item.parent.key != ghostKey {
		for _, c := range item.children {
			item.parent.AttachChild(c)
		}
		item.children = []*node{}
	}

	crdt.addGhostNode(item)
}

func (crdt *CRDT) newNode(key string, vectorClock VectorClock) *node {
	n := &node{
		key:               key,
		latestVectorClock: vectorClock,
	}
	crdt.nodes[key] = n
	return n
}

func (crdt *CRDT) addGhostNode(n *node) {
	ghost := crdt.nodes[ghostKey]
	ghost.AttachChild(n)
}

// String implements Stringer so that we can get a nicely printable
// version of the CRDT internal tree structure.
func (crdt *CRDT) String() string {
	var addNode func(t treeprint.Tree, n *node)
	addNode = func(t treeprint.Tree, n *node) {
		treeNode := t.AddBranch(fmt.Sprintf("%s (%v)", n.key, n.latestVectorClock))
		for _, c := range n.children {
			addNode(treeNode, c)
		}
	}

	tree := treeprint.New()
	rootNode := crdt.nodes[rootKey]
	addNode(tree, rootNode)

	return tree.String()
}

type node struct {
	key               string
	parent            *node
	children          []*node
	latestVectorClock VectorClock
}

// AttachChild adds the child node into the correct ordered position of the
// parents child array, sets the parent on the child node, and removes the
// child from the old parents child array
func (n *node) AttachChild(child *node) {
	// remove this child from its old parent children array
	if child.parent != nil {
		newParentChildren := make([]*node, 0)
		for _, c := range child.parent.children {
			if c.key != child.key {
				newParentChildren = append(newParentChildren, c)
			}
		}
		child.parent.children = newParentChildren
	}

	// check whether index 0 is the ghost node or not.
	// if it is, we will need to start our array search operation
	// from after the ghost so that it stays at index 0.
	startIndex := 0
	if len(n.children) > 0 && n.children[0].key == ghostKey {
		startIndex = 1
	}

	// Find the index where the new child should be added in to the children array
	index := startIndex + sort.Search(len(n.children)-startIndex, func(i int) bool {
		return n.children[i+startIndex].latestVectorClock.Before(child.latestVectorClock)
	})

	n.children = insert(n.children, index, child)

	child.parent = n
}

func (n *node) String() string {
	return fmt.Sprintf("Node{key: %s, lvc: %d, children: %v}", n.key, n.latestVectorClock, n.children)
}

// insert inserts the node at the index of the array.
func insert(a []*node, index int, value *node) []*node {
	if len(a) == index { // nil or empty slice or after last element
		return append(a, value)
	}
	a = append(a[:index+1], a[index:]...) // index < len(a)
	a[index] = value
	return a
}

func main() {
	// Create a set of events to happen.
	events := map[int]Event{
		1:  {Type: "update", ItemKey: "a", TargetItemKey: rootKey, VectorClock: VectorClock{1: 1}},
		2:  {Type: "update", ItemKey: "b", TargetItemKey: "a", VectorClock: VectorClock{1: 2}},
		3:  {Type: "update", ItemKey: "c", TargetItemKey: "b", VectorClock: VectorClock{1: 3}},
		4:  {Type: "delete", ItemKey: "b", VectorClock: VectorClock{1: 4}},
		5:  {Type: "update", ItemKey: "c", TargetItemKey: "a", VectorClock: VectorClock{1: 5}}, // This is a client generate event so that c stays after a when the middle 'b' is deleted.
		6:  {Type: "update", ItemKey: "d", TargetItemKey: "c", VectorClock: VectorClock{1: 6}},
		7:  {Type: "update", ItemKey: "f", TargetItemKey: "c", VectorClock: VectorClock{1: 6, 2: 1}},
		8:  {Type: "update", ItemKey: "b", TargetItemKey: "a", VectorClock: VectorClock{1: 6, 2: 2}},
		9:  {Type: "update", ItemKey: "h", TargetItemKey: rootKey, VectorClock: VectorClock{1: 8}},
		10: {Type: "delete", ItemKey: "f", VectorClock: VectorClock{1: 9, 2: 3}},
	}

	results := map[string][][]int{}

	// for each combination of event ordering, check what the returned CRDT ordering is
	// so that we can check if all orders return the same output (they should!)
	for _, combo := range permutations([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) {
		// fmt.Printf("== %v\n", combo)
		crdt := NewCRDT()
		// apply each event to the crdt.
		for _, id := range combo {
			e := events[id]
			// fmt.Println(e)
			crdt.Apply(e)
			// fmt.Println(crdt) // Print out the CRDT if you want to after each move
			// An example:
			// .
			// └── _root (map[])
			//     ├── _ghost (map[])
			//     │   └── f (map[1:9 2:3])
			//     ├── h (map[1:8])
			//     └── a (map[1:1])
			//         ├── b (map[1:6 2:2])
			//         └── c (map[1:5])
			//             └── d (map[1:6])
		}
		// capture the output ordering
		keys := []string{}
		for n := range crdt.Traverse() {
			keys = append(keys, n.key)
		}
		resultKey := strings.Join(keys, ",")
		combos, ok := results[resultKey]
		if !ok {
			combos = [][]int{}
		}
		combos = append(combos, combo)
		results[resultKey] = combos
	}

	// print all the output orders, and an example event ordering that
	// caused it.
	for k, v := range results {
		fmt.Printf("%s: %d -> %v\n", k, len(v), v[0])
	}
}

// permutations is a helper function that returns all permutations
// of the input array
func permutations(arr []int) [][]int {
	var helper func([]int, int)
	res := [][]int{}

	helper = func(arr []int, n int) {
		if n == 1 {
			tmp := make([]int, len(arr))
			copy(tmp, arr)
			res = append(res, tmp)
		} else {
			for i := 0; i < n; i++ {
				helper(arr, n-1)
				if n%2 == 1 {
					tmp := arr[i]
					arr[i] = arr[n-1]
					arr[n-1] = tmp
				} else {
					tmp := arr[0]
					arr[0] = arr[n-1]
					arr[n-1] = tmp
				}
			}
		}
	}
	helper(arr, len(arr))
	return res
}

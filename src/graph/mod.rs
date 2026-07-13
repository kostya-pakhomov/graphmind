//! Graph module -- adjacency list + bidirectional edges
//!
//! Based on TECH-SPEC.md Section 3 Model Data

mod node;
mod edge;

pub use node::{Metadata, Node, NodeType, Level, Status, NodeId, StoredNode};
pub use edge::{Edge, EdgeId, Relation, Provenance};

use std::collections::{HashMap, HashSet};

/// Chain link returned by traversal
#[derive(Debug, Clone)]
pub struct ChainLink {
    pub edge: Edge,
    pub depth: usize,
    pub node: Node,
}

/// Direction for traversal
#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Incoming,
    Outgoing,
}

/// In-memory graph with adjacency list
#[derive(Debug)]
pub struct Graph {
    nodes: HashMap<NodeId, Node>,
    out_edges: HashMap<NodeId, Vec<Edge>>,
    in_edges: HashMap<NodeId, Vec<Edge>>,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            out_edges: HashMap::new(),
            in_edges: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: Node) -> NodeId {
        let id = node.id.clone();
        self.nodes.insert(id.clone(), node);
        id
    }

    pub fn add_edge(&mut self, edge: Edge) -> EdgeId {
        let edge_id = edge.id.clone();
        self.out_edges
            .entry(edge.source.clone())
            .or_default()
            .push(edge.clone());
        self.in_edges
            .entry(edge.target.clone())
            .or_default()
            .push(edge.clone());
        edge_id
    }

    pub fn get_node(&self, id: &NodeId) -> Option<&Node> {
        self.nodes.get(id)
    }

    pub fn all_nodes(&self) -> &HashMap<NodeId, Node> {
        &self.nodes
    }

    pub fn out_edges(&self, id: &NodeId) -> Option<&Vec<Edge>> {
        self.out_edges.get(id)
    }

    pub fn in_edges(&self, id: &NodeId) -> Option<&Vec<Edge>> {
        self.in_edges.get(id)
    }

    /// Traverse backward: from symptom to root cause. Uses in_edges.
    pub fn chain_backward(&self, start: &NodeId, max_depth: usize) -> Vec<ChainLink> {
        let mut chain = Vec::new();
        let mut visited = HashSet::new();
        self._traverse(start, Direction::Incoming, max_depth, 1, &mut chain, &mut visited);
        chain
    }

    /// Traverse forward: from cause to possible effects. Uses out_edges.
    pub fn chain_forward_pre(&self, start: &NodeId, max_depth: usize) -> Vec<ChainLink> {
        let mut chain = Vec::new();
        let mut visited = HashSet::new();
        self._traverse(start, Direction::Outgoing, max_depth, 1, &mut chain, &mut visited);
        chain
    }

    /// Internal recursive traversal.
    /// - `remaining`: how many hops left before stopping (decremented on recursion)
    /// - `current_depth`: distance from start (1 for direct neighbours, incremented on recursion)
    fn _traverse(
        &self,
        node_id: &NodeId,
        dir: Direction,
        remaining: usize,
        current_depth: usize,
        chain: &mut Vec<ChainLink>,
        visited: &mut HashSet<NodeId>,
    ) {
        if remaining == 0 || visited.contains(node_id) {
            return;
        }
        visited.insert(node_id.clone());

        let edges = match dir {
            Direction::Incoming => self.in_edges.get(node_id),
            Direction::Outgoing => self.out_edges.get(node_id),
        };

        let Some(edges) = edges else {
            return;
        };

        for edge in edges {
            let next = match dir {
                Direction::Incoming => &edge.source,
                Direction::Outgoing => &edge.target,
            };

            if let Some(node) = self.nodes.get(next) {
                chain.push(ChainLink {
                    edge: edge.clone(),
                    depth: current_depth,
                    node: node.clone(),
                });
                if remaining > 1 {
                    self._traverse(next, dir, remaining - 1, current_depth + 1, chain, visited);
                }
            }
        }
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_basic() {
        let mut graph = Graph::new();
        let node1 = Node::new(NodeType::Atom, "test1");
        let node2 = Node::new(NodeType::Atom, "test2");
        let id1 = graph.add_node(node1);
        let id2 = graph.add_node(node2);
        assert!(graph.get_node(&id1).is_some());
        assert!(graph.get_node(&id2).is_some());
    }
}

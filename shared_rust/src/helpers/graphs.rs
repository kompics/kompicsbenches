use std::{
    cmp::{Eq, PartialEq, PartialOrd},
    fmt, mem,
};

use rand::{rngs::SmallRng, Rng, SeedableRng};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Phase {
    Initial,
    Normal(usize),
}
impl Phase {
    pub fn k(&self) -> usize {
        match self {
            Phase::Normal(k) => *k,
            _ => unimplemented!(),
        }
    }

    pub fn inc(&mut self) -> () {
        match self {
            Phase::Initial => {
                *self = Phase::Normal(0usize);
            },
            Phase::Normal(k) => {
                *k += 1usize;
            },
        }
    }

    pub fn incremented(&self) -> Phase {
        match self {
            Phase::Initial => Phase::Normal(0usize),
            Phase::Normal(k) => Phase::Normal(k + 1usize),
        }
    }
}
impl Default for Phase {
    fn default() -> Self { Phase::Initial }
}
impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Phase::Initial => write!(f, "<init>"),
            Phase::Normal(k) => write!(f, "{}", k),
        }
    }
}
impl PartialEq<usize> for Phase {
    fn eq(&self, other: &usize) -> bool {
        match self {
            Phase::Initial => false,
            Phase::Normal(k) => k == other,
        }
    }
}

pub const WEIGHT_FACTOR: f64 = 10.0;
pub const WEIGHT_CUTOFF: f64 = 5.0;

pub fn generate_graph(number_of_nodes: usize) -> Graph<f64> {
    let mut random = SmallRng::seed_from_u64(number_of_nodes as u64);

    let mut data: Vec<Vec<f64>> = Vec::with_capacity(number_of_nodes);
    for i in 0usize..number_of_nodes {
        let mut row: Vec<f64> = Vec::with_capacity(number_of_nodes);
        for j in 0usize..number_of_nodes {
            if i == j {
                row.push(0.0); // self distance better be zero
            } else {
                let weight_unscaled: f64 = random.gen();
                let weight_raw = weight_unscaled * WEIGHT_FACTOR;
                let weight =
                    if weight_raw > WEIGHT_CUTOFF { std::f64::INFINITY } else { weight_raw };
                row.push(weight);
            }
        }
        data.push(row);
    }
    Graph::with(data, format_f64)
}

fn format_f64(v: &f64) -> String {
    if v.is_infinite() {
        if v.is_sign_positive() {
            " +∞  ".into()
        } else {
            " -∞  ".into()
        }
    } else if v.is_nan() {
        " NaN ".into()
    } else {
        format!("{:05.1}", v)
    }
}

pub trait GraphOps<T>: Clone {
    fn num_nodes(&self) -> usize;
    fn get(&self, i: usize, j: usize) -> &T;
    fn set(&mut self, i: usize, j: usize, v: T) -> T;
}

#[derive(Clone)]
pub struct Graph<T> {
    data:      Vec<Vec<T>>,
    formatter: fn(&T) -> String,
}

impl<T> Graph<T> {
    pub fn with(data: Vec<Vec<T>>, formatter: fn(&T) -> String) -> Graph<T> {
        assert_eq!(data.len(), data[0].len(), "Graph Adjacency Matrices must be square!");

        Graph { data, formatter }
    }

    pub fn format(&self, v: &T) -> String { (self.formatter)(v) }
}

impl<T: Clone> Graph<T> {
    pub fn get_block(&self, block_id: usize, block_size: usize) -> Block<T> {
        let num_blocks_per_dim = self.data.len() / block_size;
        let global_start_row = (block_id / num_blocks_per_dim) * block_size;
        let global_start_col = (block_id % num_blocks_per_dim) * block_size;

        let mut block: Vec<Vec<T>> = Vec::with_capacity(block_size);
        for i in 0usize..block_size {
            let mut row: Vec<T> = Vec::with_capacity(block_size);
            for j in 0usize..block_size {
                row.push(self.get(global_start_row + i, global_start_col + j).clone());
            }
            block.push(row);
        }
        Block::with(
            block_id,
            num_blocks_per_dim,
            global_start_row,
            global_start_col,
            block,
            self.formatter,
        )
    }

    pub fn break_into_blocks(&self, block_size: usize) -> Vec<Vec<Block<T>>> {
        assert_eq!(self.data.len() % block_size, 0usize, "Only break evenly into blocks!");

        let num_blocks_per_dim = self.data.len() / block_size;
        let mut blocks: Vec<Vec<Block<T>>> = Vec::with_capacity(num_blocks_per_dim);
        for i in 0usize..num_blocks_per_dim {
            let mut row: Vec<Block<T>> = Vec::with_capacity(num_blocks_per_dim);
            for j in 0usize..num_blocks_per_dim {
                let block_id = (i * num_blocks_per_dim) + j;
                row.push(self.get_block(block_id, block_size));
            }
            blocks.push(row);
        }
        blocks
    }

    pub fn assemble_from_blocks(blocks: Vec<Vec<Block<T>>>) -> Graph<T> {
        assert_eq!(blocks.len(), blocks[0].len(), "Block Matrices must be square!");

        let block_size = blocks[0][0].block_size();
        let total_size = blocks.len() * block_size;
        let mut data: Vec<Vec<T>> = Vec::with_capacity(total_size);
        for i in 0usize..total_size {
            let mut row: Vec<T> = Vec::with_capacity(total_size);
            for j in 0usize..total_size {
                let block_row = i / block_size;
                let block_col = j / block_size;
                let local_row = i % block_size;
                let local_col = j % block_size;
                let v: T = blocks[block_row][block_col].get(local_row, local_col).clone();
                row.push(v);
            }
            data.push(row);
        }
        Graph::with(data, blocks[0][0].graph.formatter)
    }
}

pub trait Numeric: PartialOrd + std::ops::Add<Output = Self> + Sized {
    // just a marker trait
}
impl<T: PartialOrd + std::ops::Add<Output = T> + Sized> Numeric for T {
    // still just a marker trait
}

impl<T: Numeric + Copy> Graph<T> {
    pub fn compute_floyd_warshall(&mut self) -> () {
        for k in 0usize..self.data.len() {
            for i in 0usize..self.data.len() {
                for j in 0usize..self.data.len() {
                    let new_value: T = *self.get(i, k) + *self.get(k, j);
                    if *self.get(i, j) > new_value {
                        self.set(i, j, new_value);
                    }
                }
            }
        }
    }
}

impl<T> fmt::Debug for Graph<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut matrix = String::new();
        for i in 0usize..self.data.len() {
            for j in 0usize..self.data.len() {
                matrix.push_str(&(self.format(&self.data[i][j])));
                matrix.push(' ');
            }
            matrix.push('\n');
        }
        write!(f, "Graph(\n{})", matrix)
    }
}

impl<T: Clone> GraphOps<T> for Graph<T> {
    fn num_nodes(&self) -> usize { self.data.len() }

    fn get(&self, i: usize, j: usize) -> &T { &self.data[i][j] }

    fn set(&mut self, i: usize, j: usize, mut v: T) -> T {
        mem::swap(&mut self.data[i][j], &mut v);
        v
    }
}

impl<T: PartialEq> PartialEq for Graph<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
        // if self.data.len() == other.data.len() {

        // } else {
        // 	return false;
        // }
    }
}

impl<T: Eq> Eq for Graph<T> {}

#[derive(Clone)]
pub struct Block<T> {
    block_id:           usize,
    num_blocks_per_dim: usize,
    row_offset:         usize,
    col_offset:         usize,
    block_size:         usize,
    graph:              Graph<T>,
}

impl<T> Block<T> {
    pub fn with(
        block_id: usize,
        num_blocks_per_dim: usize,
        row_offset: usize,
        col_offset: usize,
        data: Vec<Vec<T>>,
        formatter: fn(&T) -> String,
    ) -> Block<T> {
        let block_size = data.len();
        let graph = Graph::with(data, formatter);
        Block { block_id, num_blocks_per_dim, row_offset, col_offset, block_size, graph }
    }

    pub fn block_size(&self) -> usize { self.block_size }

    pub fn block_position(&self) -> (usize, usize) {
        let i = self.block_id / self.num_blocks_per_dim;
        let j = self.block_id % self.num_blocks_per_dim;
        (i, j)
    }

    pub fn block_id(&self) -> usize { self.block_id }
}

impl<T: Clone + 'static> Block<T> {
    fn element_at<'s, 'f, 'b, 'c, F>(&'s self, row: usize, col: usize, neighbours: &'f F) -> &'c T
    where
        'f: 'b,
        'b: 'c,
        's: 'c,
        F: Fn(usize) -> &'b Block<T>,
    {
        let dest_block_id =
            ((row / self.block_size) * self.num_blocks_per_dim) + (col / self.block_size);
        let local_row = row % self.block_size;
        let local_col = col % self.block_size;

        if dest_block_id == self.block_id {
            self.get(local_row, local_col)
        } else {
            let block = neighbours(dest_block_id);
            block.get(local_row, local_col)
        }
    }
}

impl<T: Clone> GraphOps<T> for Block<T> {
    fn num_nodes(&self) -> usize { self.graph.num_nodes() }

    fn get(&self, i: usize, j: usize) -> &T { self.graph.get(i, j) }

    fn set(&mut self, i: usize, j: usize, v: T) -> T { self.graph.set(i, j, v) }
}

impl<T: Numeric + Copy + 'static> Block<T> {
    pub fn compute_floyd_warshall_inner<'f, 'b, F>(&mut self, neighbours: &'f F, k: usize) -> ()
    where
        'f: 'b,
        F: Fn(usize) -> &'b Block<T>,
    {
        for i in 0usize..self.graph.data.len() {
            for j in 0usize..self.graph.data.len() {
                let global_i = self.row_offset + i;
                let global_j = self.col_offset + j;

                let new_value: T = {
                    let dist1: &T = self.element_at(global_i, k, neighbours);
                    let dist2: &T = self.element_at(k, global_j, neighbours);
                    *dist1 + *dist2
                };
                if *self.get(i, j) > new_value {
                    self.set(i, j, new_value);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_generation() {
        let g = generate_graph(5);
        let g2 = g.clone();
        assert_eq!(g, g2);
    }

    #[test]
    fn graph_blocking_roundtrip() {
        let g = generate_graph(10);
        println!("{:?}", g);
        let blocks = g.break_into_blocks(5);
        for bi in 0usize..blocks.len() {
            for bj in 0usize..blocks.len() {
                let (i, j) = blocks[bi][bj].block_position();
                assert_eq!(bi, i);
                assert_eq!(bj, j);
            }
        }
        let g2 = Graph::assemble_from_blocks(blocks);
        assert_eq!(g, g2);
    }

    #[test]
    fn graph_block_floyd_warshall() {
        let g = generate_graph(10);
        let mut blocks = g.break_into_blocks(5);

        for k in 0usize..g.num_nodes() {
            let old_blocks = blocks.clone();
            let lookup_fun = |block_id: usize| {
                let i = block_id / old_blocks.len();
                let j = block_id % old_blocks.len();
                &old_blocks[i][j]
            };
            for row in blocks.iter_mut() {
                for block in row.iter_mut() {
                    block.compute_floyd_warshall_inner(&lookup_fun, k);
                }
            }
        }
        let g_blocked = Graph::assemble_from_blocks(blocks);
        let mut g2 = g.clone();
        g2.compute_floyd_warshall();
        assert_eq!(g_blocked, g2);
    }
}

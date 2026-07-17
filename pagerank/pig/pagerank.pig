--------------------------------------------------------------------------------
-- Project : PageRank on Hadoop using Apache Pig
--
-- Description:
--     Execute ONE iteration of the PageRank algorithm.
--
-- Input:
--     INPUT
--
-- Output:
--     OUTPUT
--
-- Parameters:
--     INPUT
--     OUTPUT
--     NUM_NODES
--     DAMPING
--
-- Input Format
--
-- First iteration
--     Node<TAB>AdjacencyList
--
-- Later iterations
--     Node<TAB>PageRank<TAB>AdjacencyList
--
-- Output Format
--
--     Node<TAB>PageRank<TAB>AdjacencyList
--
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- SECTION 1
-- Load Input
--------------------------------------------------------------------------------

-- TODO
-- Load data from HDFS
--
-- Example
-- A    B,C,D
--
-- or
--
-- A    0.2000    B,C,D



--------------------------------------------------------------------------------
-- SECTION 2
-- Detect Input Format
--------------------------------------------------------------------------------

-- TODO
--
-- Detect whether the current input contains
--
-- 2 columns
--     Node
--     Adjacency List
--
-- or
--
-- 3 columns
--     Node
--     Current Rank
--     Adjacency List
--
-- Initialize PageRank if needed.



--------------------------------------------------------------------------------
-- SECTION 3
-- Parse Graph
--------------------------------------------------------------------------------

-- TODO
--
-- Parse
--
-- Node
-- CurrentRank
-- AdjacencyList
--
-- Calculate
--
-- OutDegree



--------------------------------------------------------------------------------
-- SECTION 4
-- Generate Contributions
--------------------------------------------------------------------------------

-- TODO
--
-- For every outgoing edge
--
-- Contribution =
--
-- CurrentRank / OutDegree
--
-- Emit
--
-- DestinationNode
-- Contribution



--------------------------------------------------------------------------------
-- SECTION 5
-- Preserve Graph Structure
--------------------------------------------------------------------------------

-- TODO
--
-- Preserve adjacency list
--
-- Node
-- LINKS
--
-- This information is required
-- for the next iteration.



--------------------------------------------------------------------------------
-- SECTION 6
-- Merge Intermediate Records
--------------------------------------------------------------------------------

-- TODO
--
-- Union
--
-- Contributions
--
-- +
--
-- Graph Structure



--------------------------------------------------------------------------------
-- SECTION 7
-- Group By Node
--------------------------------------------------------------------------------

-- TODO
--
-- GROUP all records
--
-- BY
--
-- Node



--------------------------------------------------------------------------------
-- SECTION 8
-- Sum Contributions
--------------------------------------------------------------------------------

-- TODO
--
-- Sum all incoming contributions
--
-- Ignore graph metadata
--
-- Keep adjacency list



--------------------------------------------------------------------------------
-- SECTION 9
-- Handle Dangling Nodes
--------------------------------------------------------------------------------

-- TODO
--
-- Optional
--
-- Redistribute dangling mass
--
-- If not implemented,
-- clearly document it.



--------------------------------------------------------------------------------
-- SECTION 10
-- Calculate New PageRank
--------------------------------------------------------------------------------

-- TODO
--
-- PR =
--
-- (1-D)/N
--
-- +
--
-- D * IncomingRank



--------------------------------------------------------------------------------
-- SECTION 11
-- Build Output Record
--------------------------------------------------------------------------------

-- TODO
--
-- Output
--
-- Node
-- NewRank
-- AdjacencyList
--
-- Format
--
-- Node<TAB>Rank<TAB>AdjacencyList



--------------------------------------------------------------------------------
-- SECTION 12
-- Sort Output (Optional)
--------------------------------------------------------------------------------

-- TODO
--
-- Sort by
--
-- Node
--
-- This helps
--
-- debugging
--
-- comparison
--
-- convergence checking



--------------------------------------------------------------------------------
-- SECTION 13
-- Store Result
--------------------------------------------------------------------------------

-- TODO
--
-- Store output
--
-- into HDFS
--
-- OUTPUT



--------------------------------------------------------------------------------
-- SECTION 14
-- Validation (Optional)
--------------------------------------------------------------------------------

-- Suggested checks
--
-- Number of nodes unchanged
--
-- No duplicated nodes
--
-- Every node has one record
--
-- Rank is numeric
--
-- Output format is correct
--
-- Ready for next iteration



--------------------------------------------------------------------------------
-- END OF FILE
--------------------------------------------------------------------------------

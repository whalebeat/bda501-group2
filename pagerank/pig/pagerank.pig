--------------------------------------------------------------------------------
-- Project : PageRank on Hadoop using Apache Pig
--
-- Description:
--     Execute ONE iteration of the PageRank algorithm.
--     Mirrors the logic of python/mapper.py + python/reducer.py.
--
-- Input format  (always 3 columns — run.sh preprocesses the first iteration):
--     Node<TAB>CurrentPageRank<TAB>AdjacencyList
--     Adjacency list is comma-separated; dangling nodes have an empty adj field.
--
-- Output format:
--     Node<TAB>NewPageRank<TAB>AdjacencyList
--
-- Parameters (passed with  pig -p KEY=VALUE):
--     INPUT      HDFS input path
--     OUTPUT     HDFS output path
--     NUM_NODES  total number of nodes  (integer)
--     DAMPING    damping factor          (e.g. 0.85)
--     DANGLING   total dangling rank mass from the previous iteration
--
-- PageRank formula (same as python/reducer.py):
--
--     new_rank = (1 - d) / N  +  d * ( sum_incoming_contributions + DANGLING / N )
--
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- SECTION 1
-- Load Input
--------------------------------------------------------------------------------

records = LOAD '$INPUT' USING PigStorage('\t')
    AS (node:chararray, rank:double, adj:chararray);


--------------------------------------------------------------------------------
-- SECTION 3
-- Parse Graph
--------------------------------------------------------------------------------

-- Compute out-degree:
--   Adjacency list is comma-separated → replace ',' with ' ' then TOKENIZE.
--   Dangling nodes (adj = "" or NULL) get out_degree = 0.

parsed = FOREACH records GENERATE
    node,
    rank,
    adj,
    (adj IS NULL OR adj == ''
        ? 0
        : (int) SIZE(TOKENIZE(REPLACE(adj, ',', ' ')))
    ) AS out_degree:int;


--------------------------------------------------------------------------------
-- SECTION 4
-- Generate Contributions
--------------------------------------------------------------------------------

-- Only non-dangling nodes distribute rank to their neighbours.
-- Contribution per neighbour = current_rank / out_degree.
-- Dangling mass is handled globally in SECTION 9 via the DANGLING parameter.

non_dangling = FILTER parsed BY out_degree > 0;

flat = FOREACH non_dangling {
    neighbors = TOKENIZE(REPLACE(adj, ',', ' '));
    GENERATE
        adj  AS adj:chararray,
        rank / (double) out_degree AS contribution:double,
        FLATTEN(neighbors) AS neighbor:chararray;
};

contributions = FOREACH flat GENERATE
    neighbor     AS dest:chararray,
    contribution AS amount:double;


--------------------------------------------------------------------------------
-- SECTION 5
-- Preserve Graph Structure
--------------------------------------------------------------------------------

-- Keep each node's adjacency list so the next iteration can re-use it.

structure = FOREACH records GENERATE node AS node:chararray,
                                      adj  AS adj:chararray;


--------------------------------------------------------------------------------
-- SECTION 7
-- Group By Node, Sum Contributions, Handle Dangling Nodes
--------------------------------------------------------------------------------

-- COGROUP groups both `structure` and `contributions` by the same key in one step
--
-- For every node in `structure`:
--   - If the node received link contributions:  contributions bag is non-empty.
--   - If the node received NO contributions    contributions bag is empty
--     (dangling node, or a node nobody links to) → SUM returns NULL → 0.0.
--
-- Using COGROUP avoids the HASH_JOIN execution strategy that caused job
-- failures in Pig 0.18 on single-node clusters.

cogrp = COGROUP structure BY node, contributions BY dest;


--------------------------------------------------------------------------------
-- SECTION 8
-- Calculate New PageRank
--------------------------------------------------------------------------------

-- new_rank = (1 - d) / N  +  d * ( rank_sum  +  DANGLING / N )
--
-- FLATTEN(structure.adj):
--   Each node appears exactly once in `structure`, so the bag has one element.
--   FLATTEN extracts that single adj value into the output tuple.
--   Nodes that appear only in `contributions` (not in `structure`) are
--   naturally dropped because FLATTEN of an empty bag produces 0 rows.

new_ranks = FOREACH cogrp {
    rank_sum = SUM(contributions.amount);
    GENERATE
        group AS node:chararray,
        (1.0 - (double)$DAMPING) / (double)$NUM_NODES
            + (double)$DAMPING
              * ( (rank_sum IS NULL ? 0.0 : rank_sum)
                + (double)$DANGLING / (double)$NUM_NODES )
        AS new_rank:double,
        FLATTEN(structure.adj) AS adj:chararray;
};


--------------------------------------------------------------------------------
-- SECTION 9
-- Store Result
--------------------------------------------------------------------------------

STORE new_ranks INTO '$OUTPUT' USING PigStorage('\t');


--------------------------------------------------------------------------------
-- END OF FILE
--------------------------------------------------------------------------------

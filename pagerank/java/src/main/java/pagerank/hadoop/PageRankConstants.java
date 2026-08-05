package pagerank.hadoop;

/**
 * Cac hang so dung chung cho toan bo pipeline PageRank tren Hadoop
 * (PreprocessJob + PageRankIterationJob + Driver).
 *
 * Tham so mac dinh lay theo PageRank_Hadoop_Project_Setup_Guide.txt:
 *   - Damping Factor = 0.85
 *   - Initial Rank = 1/N
 *   - Max Iteration = 20
 *   - Convergence Threshold = 1e-6 (optional)
 *   - Reducers = 1
 */
public final class PageRankConstants {

    private PageRankConstants() {
    }

    // Ten cac tham so truyen qua Configuration giua Driver va Mapper/Reducer
    public static final String CONF_NUM_NODES = "pagerank.numNodes";
    public static final String CONF_DAMPING = "pagerank.damping";
    public static final String CONF_DANGLING_SUM = "pagerank.danglingSumPrev";

    // He so nhan de chuyen double -> long cho Hadoop Counter (chi nhan so nguyen)
    public static final long SCALE = 1_000_000_000_000L; // 10^12

    public static final double DEFAULT_DAMPING = 0.85;
    public static final double DEFAULT_TOLERANCE = 1e-6;
    public static final int DEFAULT_MAX_ITERATIONS = 20;
    public static final int NUM_REDUCERS = 1;

    public static final String COUNTER_GROUP = "PAGERANK";
    public static final String COUNTER_NEXT_DANGLING_MASS_SCALED = "NEXT_DANGLING_MASS_SCALED";
    public static final String COUNTER_INITIAL_DANGLING_MASS_SCALED = "INITIAL_DANGLING_MASS_SCALED";
}

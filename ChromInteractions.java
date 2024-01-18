import java.util.ArrayList;
import java.util.List;
import java.lang.Math;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * A Spark program to get the count of how many of each chromosome
 * interaction takes place (same bins)
 */
public class ChromInteractions {

    // array for count of base pairs of each chromosome
    private static int[] basePairs = { 0, 248956422, 242193529, 198295559, 190214555, 181538259, 170805979, 159245973,
            145138636, 138394717, 133797422, 135086622, 133275309, 114364328, 107043718, 101991189, 90338345, 83257441,
            80373285, 58617616, 64444167, 46709983, 50818468, 156040895 };
    // array for indices of what bin each chromosome starts at. Bin size is 100,000
    private static int[] bins = { 0, 0, 2490, 4912, 6895, 8798, 10614, 12323, 13916, 15368, 16752, 18090, 19441, 20774,
            21918, 22989, 24009, 24913, 25746, 26550, 27137, 27782, 28250, 28759, 30320 };

    // check the bounds of each interactions to see if valid
    public static boolean CheckBounds(int chromNum, int bound1, int bound2) {
        int chrom = basePairs[chromNum];
        return chrom >= bound1 && chrom >= bound2;
    }

    // Main method to configure and run program in Spark for counting interactions
    public static void main(String[] args) {

        // create spark configuration and new context
        SparkConf conf = new SparkConf().setAppName("chromosome interactions");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load input file as a JavaRDD of strings
        JavaRDD<String> lines = sc.textFile(args[0]);

        // map each line to a pair of chromosome interaction bins and filter out invalid
        // interactions
        JavaPairRDD<String, Integer> pairs = lines.flatMapToPair(line -> {
            String[] nums = line.split("\t");
            List<Tuple2<String, Integer>> output = new ArrayList<>();

            // check if interaction is within valid bounds
            if (CheckBounds(Integer.parseInt(nums[0]), Integer.parseInt(nums[1]), Integer.parseInt(nums[2]))
                    && CheckBounds(Integer.parseInt(nums[3]), Integer.parseInt(nums[4]), Integer.parseInt(nums[5]))) {

                // calculate bin numbers
                int bin1 = bins[Integer.parseInt(nums[0])] + (int) Math.ceil(Integer.parseInt(nums[1]) / 100000) + 1;
                int bin2 = bins[Integer.parseInt(nums[3])] + (int) Math.ceil(Integer.parseInt(nums[4]) / 100000) + 1;

                // ensure bin1 <= bin2
                if (bin1 > bin2) {
                    int temp = bin1;
                    bin1 = bin2;
                    bin2 = temp;
                }

                // create string representation of the bins and add it to output
                String str = "(" + bin1 + ", " + bin2 + ")";
                output.add(new Tuple2<>(str, 1));
            }
            return output.iterator();
        });

        // reduce the pairs by key, adding up the counts for each bin pair
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum);

        // save counts as text files
        counts.saveAsTextFile("./output");

        sc.stop();
    }
}

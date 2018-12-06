import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.TreeMap;


public class TopJobTypesApproved {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private String[] row;
        private String jobTitle;

        /**
         * Writes a <Text, IntWritable> key/value pair to the Context if the application with the corresponding Job
         * Title was Certified (Approved).  Text will contain the jobTitle, and the IntWritable will be wrapping an
         * int value of 1 which will be used for a frequency count in the combiner.
         *
         * @param key     (Object) - Input key.  Not used in this function.
         * @param value   (Text) - value is the Text object wrapping a row in the dataset csv
         * @param context (Context) - The Context that we will be writing map output <key, value> pairs to
         */
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            row = value.toString().split(","); // Split the csv row by comma

            // Use "CER" instead of full "CERTIFIED" to speed up execution. It is only contained by CASE_STATUS fields
            // that are CERTIFIED or CERTIFIED-WITHDRAWN
            if (row[1].length() == 0 || !row[1].contains("CER"))
                return; // If application is not certified or has blank CASE_STATUS, return to discard it


            jobTitle = getJobTitle(row);
            if (jobTitle.length() > 1) // Job Titles should be more than 1 letter, discards bad data
                context.write(new Text(jobTitle), one);
        }

        /**
         * This helper function is necessary because of the difficulties of parsing a csv file. Some of the Job Titles
         * in the JOB_TITLE column had commas in them, meaning that using split(",") on a row from the csv (comma
         * separated value file) would not result in a clean separation of the columns.  If the JOB_TITLE column value
         * contains a comma, then it will be split across multiple elements of row[].
         * <p>
         * This function uses helper function getColumnStringEndIndex() to figure out the starting index in row[] of our
         * Job Title.  If it is not split across multiple elements, then it returns the value stored at the Job Title
         * index in row[].  Otherwise, it uses getColumnStringEndIndex() to find the end index of the Job Title value
         * in row[], builds the jobTitle String, and returns it.
         *
         * @param row (String[]) - An array of Strings that is the result of a split(",") op on a csv row.
         * @return String of the Job Title, not surrounded by quotes (Columns of the csv that are Strings are read in
         * as "Column value", so we discard the quotes.
         */
        public String getJobTitle(String[] row) {
            int employerNameStartIndex = 2;
            int socNameStartIndex = getColumnStringEndIndex(employerNameStartIndex, row) + 1;
            if (socNameStartIndex == 0) // getColumnStringEndIndex returned - 1
                return "";

            int jobTitleStartIndex = getColumnStringEndIndex(socNameStartIndex, row) + 1;
            if (jobTitleStartIndex == 0) // getColumnStringEndIndex returned - 1
                return "";
            else {
                String JTStart = row[jobTitleStartIndex];
                if (JTStart.length() < 2)
                    return "";
                // If job title is not split across multiple elements, return it
                if (JTStart.charAt(JTStart.length() - 1) == '"' && JTStart.charAt(0) == '"')
                    return JTStart.substring(1, JTStart.length() - 1); // return Job Title without quotes
            }

            // Job Title is spread across multiple row[] elements, get its end index
            int jobTitleEndIndex = getColumnStringEndIndex(jobTitleStartIndex, row);
            if (jobTitleEndIndex == -1) // getColumnStringEndIndex returned -1
                return "";

            String jobTitle = "";
            //Build jobTitle String
            for (int index = jobTitleStartIndex; index <= jobTitleEndIndex; index++) {
                jobTitle += row[index];
                if (index != jobTitleEndIndex)
                    jobTitle += ",";
            }
            return jobTitle.substring(1, jobTitle.length() - 1);
        }

        /**
         * This helper function finds the last index of the Column value that started at row[startIndex]. This is
         * used for Column values that are spread across multiple row[] elements because of the split(",") operation
         * performed on the csv row. Column values are spread across multiple row[] elements only if they are Strings
         * that contain commas.
         *
         * @param startIndex (int) - starting index of a column value in row[]
         * @param row        (String[]) - An array of Strings that is the result of a split(",") op on a csv row.
         * @return Returns an int.
         * Returns -1 if the csv row was improperly formatted or if the Column value was left blank.
         * NOTE: While there were column values left blank, there did not appear to be any improperly formatted rows,
         * so that aspect is purely a preemptive measure.
         * Returns the last index of the Column value that started at startIndex in row[].
         */
        public int getColumnStringEndIndex(int startIndex, String[] row) {
            int index = startIndex;
            while (index < row.length) {
                String token = row[index];
                if (token.length() < 1 || token.charAt(token.length() - 1) == '"')
                    return index; // return index if column is empty or we've reached end of String for this column
                index++;
            }
            return -1; // Reached end of row when shouldn't have, row was incorrectly formatted, return -1
        }
    }

    /**
     * This Combiner extends Reducer and reduces by frequency count on each Job Title written to the context by the
     * mapper it is assigned to.
     * It writes <Text, IntWritable> key/value pairs to the context which will be the input for the reducer
     */
    public static class IntSumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * @param key     (Text) - Text wrapper for the Job Title
         * @param values  (Iterable<IntWritable>) - An Iterable of IntWritables each storing int values of 1.
         * @param context (Context) - The context to write the key/value pairs to
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // Sum the frequencies for this key (Job Title)
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopTwentyReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * topTwentyJobTitles will be a TreeMap with keys = int frequency counts, and values = LinkedList of Job
         * Titles (Text Objects) that have the corresponding frequency count specified by their key. The TreeMap is
         * ranked by frequency count in descending order.
         * The values must be LinkedLists if you want to include Job Titles that have the same frequency counts (are
         * tied for their place in the top twenty).  While this may only only occur on rare occasion, it did not slow
         * down execution times much.
         */
        private TreeMap<Integer, LinkedList<Text>> topTwentyJobTitles = new TreeMap<>();
        private Text jobTitle;

        /**
         * Reduce the combiner frequency outputs for a single Job Title into its total frequency count, and offer that
         * Job Title and its frequency to the Top Twenty helper function that will determine where, if anywhere it
         * belongs in the Top Twenty.
         *
         * @param key     (Text) - Text object wrapping the Job Title String
         * @param values  (Iterable<IntWritable>) - The Iterable containing the frequency counts (as IntWritables) for
         *                the corresponding Job Title output by the combiners.
         * @param context Unused, but required as a parameter.
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int freq = 0;
            // Sum the frequencies for this key (Job Title)
            for (IntWritable val : values) {
                freq += val.get();
            }
            jobTitle = new Text(key.toString());

            // Offer this jobTitle and its corresponding total frequency to the Top Twenty
            offerToTopTwenty(freq, jobTitle);
        }

        /**
         * This function is called after the last call to reduce() is made.
         * At this point we will write the ranking stored in topTwentyJobTitles to the Context for output
         *
         * @param context (Context) - The context to write the output to
         */
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (int freq : topTwentyJobTitles.descendingKeySet()) {
                LinkedList<Text> jobTitleList = topTwentyJobTitles.get(freq);
                for (Text jobTitle : jobTitleList) {
                    context.write(jobTitle, new IntWritable(freq));
                }
            }
        }

        /**
         * This is a helper function whose only job is to determine if a Job Title belongs in the Top Twenty.
         * If it does, then it will pass that Job Title and its frequency to addToTopTwenty() which will insert them into
         * their proper place in topTwentyJobTitles
         *
         * @param freq     (int) - Total frequency count of jobTitle
         * @param jobTitle (Text) - Text object wrapping Job Title String
         */
        public void offerToTopTwenty(int freq, Text jobTitle) {
            if (topTwentyJobTitles.size() < 20 || freq == topTwentyJobTitles.firstKey())
                addToTopTwenty(freq, jobTitle);
            else if (freq > topTwentyJobTitles.firstKey()) {
                topTwentyJobTitles.pollFirstEntry(); // IMPORTANT: Removes the rank 20 element to make room for new addition
                addToTopTwenty(freq, jobTitle);
            }
        }

        /**
         * This function inserts a Job Title into its corresponding LinkedList in topTwentyJobTitles, or inserts it as the
         * first element of a new LinkedList with a certain frequency count and inserts that LinkedList into the
         * topTwentyJobTitles. (See documentation above the topTwentyJobTitles declaration)
         * topTwentyJobTitles will always contain 19 elements or less before this function is called, leaving room for
         * whatever <freq, jobTitle> pair is to be inserted.
         *
         * @param freq     (int) - frequency that this jobTitle was certified
         * @param jobTitle (Text) - Text object of the Job Title string
         */
        public void addToTopTwenty(int freq, Text jobTitle) {
            LinkedList<Text> jobTitleList = topTwentyJobTitles.get(freq);
            if (jobTitleList == null) {
                //topTwentyJobTitles.size() < 20 at this point, make new LinkedList for this frequency
                jobTitleList = new LinkedList<Text>();
                jobTitleList.add(jobTitle); // Add jobTitle as the first element of this list
                topTwentyJobTitles.put(freq, jobTitleList);
            } else // There is a tie, there is already a list of Job Titles for this frequency.
                jobTitleList.add(jobTitle);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " : ");
        Job job = Job.getInstance(conf, "Most Commonly Certified Job Titles");
        job.setJarByClass(TopJobTypesApproved.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(TopTwentyReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
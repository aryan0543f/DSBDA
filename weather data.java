// importing Libraries
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class MyMaxMin
{
    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        public static final int MISSING = 9999;

        @Override
        public void map(LongWritable arg0, Text Value, Context context) throws IOException, InterruptedException
        {
            String line = Value.toString();
            if (!(line.length() == 0))
            {
                String date = line.substring(6, 14);
                float temp_Max = Float.parseFloat(line.substring(39, 45).trim());
                float temp_Min = Float.parseFloat(line.substring(47, 53).trim());
                if (temp_Max > 30.0)
                {
                    context.write(new Text("The Day is Hot Day :" + date),
                        new Text(String.valueOf(temp_Max)));
                }
                if (temp_Min < 15)
                {
                    context.write(new Text("The Day is Cold Day :" + date),
                        new Text(String.valueOf(temp_Min)));
                }
            }
        }
    }

    public static class MaxTemperatureReducer extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text Key, Iterator<Text> values, Context context) throws IOException, InterruptedException
        {
            String temperature = values.next().toString();
            context.write(Key, new Text(temperature));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "weather example");
        job.setJarByClass(MyMaxMin.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path OutputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        OutputPath.getFileSystem(conf).delete(OutputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
 * ============================================================
 * STEPS TO RUN ON UBUNTU SYSTEM WITH HADOOP INSTALLED
 * ============================================================
 *
 * STEP 1 - Check Hadoop is working
 * ---------------------------------
 *   hadoop version
 *
 * STEP 2 - Start Hadoop services
 * --------------------------------
 *   start-dfs.sh
 *   start-yarn.sh
 *   jps
 *   (jps should show: NameNode, DataNode, ResourceManager, NodeManager)
 *
 * STEP 3 - Download the Weather dataset
 * ---------------------------------------
 *   Dataset: NCDC weather data (fixed-width text format)
 *   Place the weather data file (e.g. weather.txt) in your local system
 *
 * STEP 4 - Upload input file to HDFS
 * ------------------------------------
 *   hdfs dfs -mkdir /input3000
 *   hdfs dfs -put weather.txt /input3000
 *   hdfs dfs -ls /input3000
 *
 * STEP 5 - Compile the Java file
 * --------------------------------
 *   javac -classpath $(hadoop classpath) -d . MyMaxMin.java
 *
 * STEP 6 - Create JAR file
 * -------------------------
 *   jar -cvf weather.jar -C . .
 *
 * STEP 7 - Run the MapReduce job
 * --------------------------------
 *   hadoop jar weather.jar MyMaxMin /input3000 /output3000
 *
 * STEP 8 - View the output
 * -------------------------
 *   hdfs dfs -cat /output3000/part-00000
 *
 * EXPECTED OUTPUT:
 *   The Day is Cold Day :20200101    -21.8
 *   The Day is Cold Day :20200102    -23.4
 *   The Day is Cold Day :20200103    -25.4
 *   The Day is Cold Day :20200104    -26.8
 *   The Day is Cold Day :20200105    -28.8
 *   The Day is Cold Day :20200106    -30.0
 *   The Day is Cold Day :20200107    -31.4
 *   The Day is Cold Day :20200108    -33.6
 *   The Day is Cold Day :20200109    -26.6
 *   The Day is Cold Day :20200110    -24.3
 *
 * NOTE - If output directory already exists (re-run error):
 * ----------------------------------------------------------
 *   hdfs dfs -rm -r /output3000
 *   (Then run Step 7 again)
 *   OR the code already handles this automatically via:
 *   OutputPath.getFileSystem(conf).delete(OutputPath);
 *
 * STEP 9 - Stop Hadoop when done
 * ---------------------------------
 *   stop-dfs.sh
 *   stop-yarn.sh
 *
 * ============================================================
 *
 * LOGIC EXPLANATION:
 * -------------------
 *   - temp_Max > 30.0  => Day is classified as HOT DAY
 *   - temp_Min < 15    => Day is classified as COLD DAY
 *   - Date is extracted from characters 6-14 of each line
 *   - temp_Max is extracted from characters 39-45
 *   - temp_Min is extracted from characters 47-53
 *
 * ============================================================
 */

/*
 * ============================================================
 * IMPORTANT NOTES FOR ORAL EXAM
 * WEATHER DATA - MAX/MIN TEMPERATURE USING MapReduce
 * ============================================================
 *
 * ------------------------------------------------------------
 * WHAT IS THIS PRACTICAL ABOUT?
 * ------------------------------------------------------------
 * This practical processes NCDC weather data using Hadoop MapReduce
 * to classify days as HOT or COLD based on temperature thresholds:
 *   - HOT DAY  : max temperature > 30.0 degrees
 *   - COLD DAY : min temperature < 15 degrees
 *
 * This practical uses the NEW MapReduce API
 * (org.apache.hadoop.mapreduce) unlike WordCount and Sales Country
 * which use the OLD API (org.apache.hadoop.mapred).
 *
 * All three classes (Mapper, Reducer, Driver) are in ONE file
 * as inner/nested classes inside the MyMaxMin class.
 *
 * ------------------------------------------------------------
 * DATASET USED - NCDC Weather Data
 * ------------------------------------------------------------
 * NCDC = National Climatic Data Center
 * The weather data file is a FIXED-WIDTH text format.
 * Each line represents one day's weather record.
 * Data is extracted by CHARACTER POSITION (not by comma split):
 *   Characters 6-14  : Date (e.g., 20200101 = Jan 1, 2020)
 *   Characters 39-45 : Maximum temperature (temp_Max)
 *   Characters 47-53 : Minimum temperature (temp_Min)
 *
 * ------------------------------------------------------------
 * CODE EXPLANATION - MaxTemperatureMapper
 * ------------------------------------------------------------
 * extends Mapper<LongWritable, Text, Text, Text>
 *   - Input Key   : LongWritable - byte offset of the line.
 *   - Input Value : Text         - one full line of weather data.
 *   - Output Key  : Text         - label like "The Day is Hot Day: date"
 *   - Output Value: Text         - temperature value as string.
 *   NOTE: Both output key and value are Text (unlike WordCount where
 *         output value was IntWritable).
 *
 * public static final int MISSING = 9999;
 *   - Constant for missing temperature value in NCDC data.
 *   - Defined but not used in filtering here (just declared).
 *
 * map() method step by step:
 *   String line = Value.toString();
 *     -> Converts Hadoop Text to Java String.
 *
 *   if (!(line.length() == 0))
 *     -> Skips empty lines to avoid errors.
 *
 *   String date = line.substring(6, 14);
 *     -> Extracts characters at positions 6 to 13 (8 chars = date).
 *     -> substring(start, end) in Java: start is INCLUSIVE, end is EXCLUSIVE.
 *     -> Example: "20200101" = January 1, 2020.
 *
 *   float temp_Max = Float.parseFloat(line.substring(39, 45).trim());
 *     -> Extracts characters 39-44 (max temperature as string).
 *     -> .trim() removes leading/trailing whitespace.
 *     -> Float.parseFloat() converts the string to a float number.
 *
 *   float temp_Min = Float.parseFloat(line.substring(47, 53).trim());
 *     -> Extracts characters 47-52 (min temperature as string).
 *     -> Same conversion as temp_Max.
 *
 *   if (temp_Max > 30.0)
 *     context.write(new Text("The Day is Hot Day :" + date),
 *                   new Text(String.valueOf(temp_Max)));
 *     -> If max temp > 30, emit (hot_day_label, temp_Max).
 *     -> String.valueOf() converts float to String.
 *
 *   if (temp_Min < 15)
 *     context.write(new Text("The Day is Cold Day :" + date),
 *                   new Text(String.valueOf(temp_Min)));
 *     -> If min temp < 15, emit (cold_day_label, temp_Min).
 *
 *   NOTE: context.write() is used instead of output.collect()
 *         because this uses the NEW MapReduce API.
 *
 * ------------------------------------------------------------
 * CODE EXPLANATION - MaxTemperatureReducer
 * ------------------------------------------------------------
 * extends Reducer<Text, Text, Text, Text>
 *   - Input Key   : Text - day label (Hot/Cold Day + date)
 *   - Input Value : Text - temperature value
 *   - Output Key  : Text - same day label
 *   - Output Value: Text - temperature value
 *
 * reduce() method:
 *   String temperature = values.next().toString();
 *     -> Gets the first (and only) temperature value for this key.
 *     -> Since each date is unique, there is only one value per key.
 *
 *   context.write(Key, new Text(temperature));
 *     -> Writes the day label and temperature to the output.
 *
 * ------------------------------------------------------------
 * CODE EXPLANATION - main() method (Driver)
 * ------------------------------------------------------------
 * Configuration conf = new Configuration();
 *   -> Creates a Hadoop configuration object with default settings.
 *
 * Job job = new Job(conf, "weather example");
 *   -> Creates a new MapReduce job with the name "weather example".
 *
 * job.setJarByClass(MyMaxMin.class);
 *   -> Tells Hadoop which JAR file to use (the one containing MyMaxMin).
 *
 * job.setMapOutputKeyClass(Text.class);
 * job.setMapOutputValueClass(Text.class);
 *   -> Sets the output types of the Mapper (Text, Text).
 *
 * job.setMapperClass(MaxTemperatureMapper.class);
 * job.setReducerClass(MaxTemperatureReducer.class);
 *   -> Registers the Mapper and Reducer classes.
 *
 * job.setInputFormatClass(TextInputFormat.class);
 * job.setOutputFormatClass(TextOutputFormat.class);
 *   -> TextInputFormat: reads input line by line.
 *   -> TextOutputFormat: writes output as plain text.
 *
 * FileInputFormat.addInputPath(job, new Path(args[0]));
 *   -> Sets the HDFS input path from command-line argument args[0].
 *
 * FileOutputFormat.setOutputPath(job, new Path(args[1]));
 *   -> Sets the HDFS output path from command-line argument args[1].
 *
 * OutputPath.getFileSystem(conf).delete(OutputPath);
 *   -> AUTOMATICALLY deletes the output directory if it already exists.
 *   -> This is a key difference from WordCount/Sales Country where you
 *      had to manually run: hdfs dfs -rm -r /output3000
 *
 * System.exit(job.waitForCompletion(true) ? 0 : 1);
 *   -> Submits the job and waits for it to finish.
 *   -> Returns 0 (success) or 1 (failure).
 *
 * ------------------------------------------------------------
 * FULL MapReduce FLOW FOR WEATHER DATA
 * ------------------------------------------------------------
 * INPUT (weather.txt in HDFS - fixed-width format):
 *   Each line = one day's weather record
 *   Position 6-14  = date
 *   Position 39-45 = max temp
 *   Position 47-53 = min temp
 *
 * AFTER MAP PHASE:
 *   ("The Day is Hot Day :20200115",  "32.5")   <- temp_Max > 30
 *   ("The Day is Cold Day :20200101", "-21.8")  <- temp_Min < 15
 *   ("The Day is Cold Day :20200102", "-23.4")
 *   ...
 *
 * AFTER SHUFFLE & SORT:
 *   ("The Day is Cold Day :20200101", ["-21.8"])
 *   ("The Day is Cold Day :20200102", ["-23.4"])
 *   ("The Day is Hot Day :20200115",  ["32.5"])
 *   ...
 *
 * AFTER REDUCE PHASE (final output):
 *   The Day is Cold Day :20200101    -21.8
 *   The Day is Cold Day :20200102    -23.4
 *   The Day is Hot Day  :20200115    32.5
 *   ...
 *
 * ------------------------------------------------------------
 * OLD API vs NEW API - KEY DIFFERENCE
 * ------------------------------------------------------------
 * OLD API (org.apache.hadoop.mapred) - used in WordCount, Sales Country:
 *   - Mapper implements Mapper interface
 *   - Uses OutputCollector to emit: output.collect(key, value)
 *   - Uses Reporter for progress
 *   - Separate Driver/Runner class needed
 *   - throws IOException only
 *
 * NEW API (org.apache.hadoop.mapreduce) - used in Weather Data:
 *   - Mapper extends Mapper class
 *   - Uses Context to emit: context.write(key, value)
 *   - Driver can be inside main() of same class
 *   - throws IOException, InterruptedException
 *   - More flexible and recommended for new code
 *
 * ------------------------------------------------------------
 * KEY CLASSES AND IMPORTS USED
 * ------------------------------------------------------------
 * Mapper<LongWritable,Text,Text,Text>  : New API Mapper base class.
 * Reducer<Text,Text,Text,Text>         : New API Reducer base class.
 * Configuration                        : Holds Hadoop configuration settings.
 * Job                                  : Represents a MapReduce job.
 * FileInputFormat / FileOutputFormat   : Set input/output paths for the job.
 * TextInputFormat                      : Reads input file line by line.
 * TextOutputFormat                     : Writes output as plain text.
 * Path                                 : Represents a file/directory path in HDFS.
 * Context                              : Used to emit (key, value) in new API.
 * Float.parseFloat()                   : Converts String to float.
 * String.valueOf()                     : Converts float to String.
 * line.substring(start, end)           : Extracts characters by position.
 * .trim()                              : Removes leading/trailing whitespace.
 *
 * ------------------------------------------------------------
 * KEY HDFS COMMANDS USED
 * ------------------------------------------------------------
 *   start-dfs.sh / stop-dfs.sh           : Start/Stop HDFS
 *   start-yarn.sh / stop-yarn.sh          : Start/Stop YARN
 *   jps                                   : Check running Hadoop daemons
 *   hdfs dfs -mkdir /input3000            : Create input directory in HDFS
 *   hdfs dfs -put weather.txt /input3000  : Upload weather file to HDFS
 *   hdfs dfs -ls /input3000               : List files in HDFS
 *   hdfs dfs -cat /output3000/part-00000  : View output result
 *   hdfs dfs -rm -r /output3000           : Delete output dir (if needed)
 *   javac -classpath $(hadoop classpath) -d . MyMaxMin.java : Compile
 *   jar -cvf weather.jar -C . .           : Create JAR file
 *   hadoop jar weather.jar MyMaxMin /input3000 /output3000  : Run job
 *
 * NOTE: Output directory is auto-deleted by the code itself:
 *   OutputPath.getFileSystem(conf).delete(OutputPath);
 *   So you usually do NOT need to manually delete it before re-running.
 *
 * ------------------------------------------------------------
 * KEY TERMS TO REMEMBER
 * ------------------------------------------------------------
 * NCDC           : National Climatic Data Center - source of weather data.
 * Fixed-width    : Data format where each field is at a fixed character position.
 * substring()    : Java method to extract part of a string by position.
 * trim()         : Removes whitespace from start and end of a string.
 * Float.parseFloat(): Converts a String to a float number.
 * String.valueOf(): Converts a number to a String.
 * context.write(): New API method to emit (key, value) from Mapper/Reducer.
 * Configuration  : Hadoop object holding job settings.
 * Job            : Represents and configures a MapReduce job.
 * Path           : Represents a file/directory path in HDFS.
 * TextInputFormat: Reads input file line by line (default input format).
 * TextOutputFormat: Writes output as plain text (default output format).
 * args[0]        : First command-line argument = input path.
 * args[1]        : Second command-line argument = output path.
 * MISSING = 9999 : Constant for missing temperature in NCDC format.
 * HOT DAY        : Day where max temperature > 30.0 degrees.
 * COLD DAY       : Day where min temperature < 15 degrees.
 *
 * ------------------------------------------------------------
 * POSSIBLE ORAL EXAM QUESTIONS AND ANSWERS
 * ------------------------------------------------------------
 * Q: What is this practical about?
 * A: It processes NCDC weather data using Hadoop MapReduce to classify
 *    days as Hot (max temp > 30) or Cold (min temp < 15) and outputs
 *    the date and temperature for each such day.
 *
 * Q: What is the difference between this practical and WordCount?
 * A: WordCount uses the OLD MapReduce API (org.apache.hadoop.mapred),
 *    uses OutputCollector to emit output, and has a separate Driver class.
 *    Weather Data uses the NEW API (org.apache.hadoop.mapreduce),
 *    uses Context to emit output, and the Driver is inside main().
 *
 * Q: What does the Mapper do in this practical?
 * A: It reads each line of weather data, extracts the date (chars 6-14),
 *    max temp (chars 39-45), and min temp (chars 47-53).
 *    If max temp > 30, emits (Hot Day label, temp_Max).
 *    If min temp < 15, emits (Cold Day label, temp_Min).
 *
 * Q: What does the Reducer do?
 * A: It receives the day label and temperature, and simply writes them
 *    to the output. Since each date is unique, there is only one value per key.
 *
 * Q: What is substring() and how is it used here?
 * A: substring(start, end) extracts characters from a String.
 *    start is inclusive, end is exclusive.
 *    Used to extract date, max temp, and min temp from fixed positions
 *    in each line of the weather data file.
 *
 * Q: What is the difference between context.write() and output.collect()?
 * A: output.collect() is used in the OLD MapReduce API (org.apache.hadoop.mapred).
 *    context.write() is used in the NEW MapReduce API (org.apache.hadoop.mapreduce).
 *    Both do the same thing - emit a (key, value) pair as output.
 *
 * Q: What does OutputPath.getFileSystem(conf).delete(OutputPath) do?
 * A: It automatically deletes the output directory in HDFS before the job runs.
 *    This prevents the "output directory already exists" error on re-runs.
 *    WordCount and Sales Country require manual deletion with hdfs dfs -rm -r.
 *
 * Q: What is NCDC weather data format?
 * A: It is a fixed-width text format where each field is at a specific
 *    character position in each line. Date is at positions 6-14,
 *    max temp at 39-45, min temp at 47-53.
 *
 * Q: What does Float.parseFloat() do?
 * A: It converts a String to a float (decimal) number.
 *    Used to convert the extracted temperature strings to numbers
 *    so we can compare them with thresholds (> 30, < 15).
 *
 * Q: What is the role of Configuration and Job classes?
 * A: Configuration holds Hadoop settings (cluster config, etc.).
 *    Job represents the MapReduce job - it is configured with the
 *    Mapper, Reducer, input/output paths, and key/value types,
 *    then submitted to the Hadoop cluster for execution.
 *
 * Q: What are args[0] and args[1] in the main() method?
 * A: They are command-line arguments passed when running the job.
 *    args[0] = input path in HDFS (e.g., /input3000)
 *    args[1] = output path in HDFS (e.g., /output3000)
 *    Example: hadoop jar weather.jar MyMaxMin /input3000 /output3000
 * ============================================================
 */

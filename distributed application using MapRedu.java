package SalesCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");
        output.collect(new Text(SingleCountryData[7]), one);
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
 * STEP 3 - Download the Sales dataset (Salesjan2009.csv)
 * -------------------------------------------------------
 *   (Place Salesjan2009.csv in your local system)
 *
 * STEP 4 - Upload input CSV to HDFS
 * -----------------------------------
 *   hdfs dfs -mkdir /inputfiles
 *   hdfs dfs -put Salesjan2009.csv /inputfiles
 *   hdfs dfs -ls /inputfiles
 *
 * STEP 5 - Create working directory and place all Java files
 * -----------------------------------------------------------
 *   mkdir ~/salescountry
 *   cd ~/salescountry
 *   (Place SalesMapper.java, SalesReducer.java, SalesCountryDriver.java here)
 *
 * STEP 6 - Compile all Java files
 * ---------------------------------
 *   javac -classpath $(hadoop classpath) -d ~/salescountry ~/salescountry/SalesMapper.java ~/salescountry/SalesReducer.java ~/salescountry/SalesCountryDriver.java
 *
 * STEP 7 - Create JAR file
 * -------------------------
 *   cd ~/salescountry
 *   jar -cvf salescountry.jar -C . .
 *
 * STEP 8 - Run the MapReduce job
 * --------------------------------
 *   hadoop jar ~/salescountry/salescountry.jar SalesCountry.SalesCountryDriver /inputfiles /outputfiles
 *
 * STEP 9 - View the output
 * -------------------------
 *   hdfs dfs -cat /outputfiles/part-00000
 *
 * EXPECTED OUTPUT:
 *   (Country name and count of sales per country from Salesjan2009.csv)
 *   Australia       ???
 *   Bahrain         ???
 *   ...
 *
 * NOTE - If output directory already exists (re-run error):
 * ----------------------------------------------------------
 *   hdfs dfs -rm -r /outputfiles
 *   (Then run Step 8 again)
 *
 * STEP 10 - Stop Hadoop when done
 * ---------------------------------
 *   stop-dfs.sh
 *   stop-yarn.sh
 *
 * ============================================================
 *
 * BROWSE HDFS DIRECTORY via Web UI:
 * -----------------------------------
 *   Open browser and go to: http://localhost:50070
 *   Navigate to: Utilities > Browse the file system
 *   Check /inputfiles and /outputfiles directories
 *
 * ============================================================
 * /*
 * ============================================================
 * IMPORTANT NOTES FOR ORAL EXAM
 * DISTRIBUTED APPLICATION USING MapReduce - SALES COUNTRY
 * ============================================================
 *
 * ------------------------------------------------------------
 * WHAT IS THIS PRACTICAL ABOUT?
 * ------------------------------------------------------------
 * This practical counts the number of sales per country from
 * a CSV file (Salesjan2009.csv) using Hadoop MapReduce.
 * It is a distributed application - the processing is spread
 * across multiple nodes in a Hadoop cluster.
 *
 * Files in this practical:
 *   SalesMapper.java        - The Mapper class
 *   SalesReducer.java       - The Reducer class (not shown here)
 *   SalesCountryDriver.java - The Driver class (not shown here)
 *
 * ------------------------------------------------------------
 * WHAT IS A DISTRIBUTED APPLICATION?
 * ------------------------------------------------------------
 * A distributed application runs across multiple computers
 * (nodes) simultaneously, sharing the workload.
 * Hadoop MapReduce is a framework for writing distributed apps.
 * Instead of one machine processing all data, many machines
 * process parts of the data in parallel -> much faster.
 *
 * ------------------------------------------------------------
 * DATASET USED - Salesjan2009.csv
 * ------------------------------------------------------------
 * A CSV (Comma Separated Values) file with sales transaction data.
 * Each row = one sales transaction.
 * Column index 7 (8th column) = Country of sale.
 * Goal: Count how many sales happened in each country.
 *
 * ------------------------------------------------------------
 * CODE EXPLANATION - SalesMapper
 * ------------------------------------------------------------
 * package SalesCountry;
 *   - Package name groups related classes together.
 *
 * implements Mapper<LongWritable, Text, Text, IntWritable>
 *   - Input Key   : LongWritable - byte offset of the line in file.
 *   - Input Value : Text         - one full line (row) of the CSV.
 *   - Output Key  : Text         - country name (column 7).
 *   - Output Value: IntWritable  - always 1 (one sale).
 *
 * private final static IntWritable one = new IntWritable(1);
 *   - A constant value of 1 (reused for every output pair).
 *   - Avoids creating a new object for every word -> efficient.
 *
 * map() method:
 *   String valueString = value.toString();
 *     - Converts the Hadoop Text object to a regular Java String.
 *
 *   String[] SingleCountryData = valueString.split(",");
 *     - Splits the CSV line by comma into an array of fields.
 *     - Example: "1,John,Male,...,USA,..." -> ["1","John","Male",...,"USA",...]
 *
 *   output.collect(new Text(SingleCountryData[7]), one);
 *     - SingleCountryData[7] = the country name (8th column, index 7).
 *     - Emits (country_name, 1) as the output key-value pair.
 *     - output.collect() sends this pair to the Reducer.
 *
 * ------------------------------------------------------------
 * WHAT DOES SalesReducer DO? (not shown but important)
 * ------------------------------------------------------------
 * - Receives: (country_name, [1, 1, 1, 1, ...])
 *   All 1s for the same country are grouped together.
 * - Sums up all the 1s.
 * - Emits: (country_name, total_sales_count)
 * - Example: ("Australia", [1,1,1]) -> ("Australia", 3)
 *
 * ------------------------------------------------------------
 * WHAT DOES SalesCountryDriver DO? (not shown but important)
 * ------------------------------------------------------------
 * The Driver class configures and submits the MapReduce job:
 *   - Sets input path  : /inputfiles (where CSV is in HDFS)
 *   - Sets output path : /outputfiles (where result goes)
 *   - Sets Mapper class : SalesMapper
 *   - Sets Reducer class: SalesReducer
 *   - Sets output key type  : Text
 *   - Sets output value type: IntWritable
 *   - Submits the job to Hadoop cluster
 *
 * ------------------------------------------------------------
 * FULL MapReduce FLOW FOR THIS PRACTICAL
 * ------------------------------------------------------------
 * INPUT (Salesjan2009.csv in HDFS):
 *   1,John,Male,25,Visa,100,Electronics,USA
 *   2,Jane,Female,30,Cash,200,Clothing,Australia
 *   3,Bob,Male,22,Visa,150,Electronics,USA
 *   ...
 *
 * AFTER MAP PHASE (intermediate output):
 *   (USA, 1)
 *   (Australia, 1)
 *   (USA, 1)
 *   ...
 *
 * AFTER SHUFFLE & SORT (grouped by key):
 *   (Australia, [1])
 *   (USA, [1, 1])
 *   ...
 *
 * AFTER REDUCE PHASE (final output):
 *   Australia    1
 *   USA          2
 *   ...
 *
 * ------------------------------------------------------------
 * KEY CLASSES AND IMPORTS USED
 * ------------------------------------------------------------
 * IntWritable   : Hadoop's serializable integer. Used for count = 1.
 * LongWritable  : Hadoop's serializable long. Used for line byte offset.
 * Text          : Hadoop's serializable String. Used for country name.
 * MapReduceBase : Base class that Mapper and Reducer extend (old API).
 * Mapper        : Interface defining the map() method signature.
 * OutputCollector: Collects (key, value) output from Mapper/Reducer.
 * Reporter      : Reports job progress and status to Hadoop framework.
 *
 * WHY Hadoop types (IntWritable, Text) instead of Java types (int, String)?
 *   - Hadoop needs to SERIALIZE data to send it across the network.
 *   - Hadoop types implement Writable interface for efficient serialization.
 *   - Regular Java types (int, String) cannot be directly serialized by Hadoop.
 *
 * ------------------------------------------------------------
 * DIFFERENCE FROM WORDCOUNT PRACTICAL
 * ------------------------------------------------------------
 * WordCount:
 *   - Input: plain text file.
 *   - Splits each line into words using StringTokenizer.
 *   - Emits (word, 1) for each word.
 *   - Output: count of each word.
 *
 * Sales Country:
 *   - Input: CSV file with structured data.
 *   - Splits each line by comma using split(",").
 *   - Extracts specific column (index 7 = country).
 *   - Emits (country, 1) for each row.
 *   - Output: count of sales per country.
 *
 * Both use the same MapReduce pattern - only the data extraction differs.
 *
 * ------------------------------------------------------------
 * KEY HDFS COMMANDS USED
 * ------------------------------------------------------------
 *   hdfs dfs -mkdir /inputfiles       : Create input directory in HDFS
 *   hdfs dfs -put Salesjan2009.csv /inputfiles : Upload CSV to HDFS
 *   hdfs dfs -ls /inputfiles          : List files in HDFS directory
 *   hdfs dfs -cat /outputfiles/part-00000 : View output result
 *   hdfs dfs -rm -r /outputfiles      : Delete output dir (before re-run)
 *   hadoop jar salescountry.jar SalesCountry.SalesCountryDriver /inputfiles /outputfiles
 *
 * Web UI to browse HDFS:
 *   http://localhost:50070 -> Utilities -> Browse the file system
 *
 * ------------------------------------------------------------
 * KEY TERMS TO REMEMBER
 * ------------------------------------------------------------
 * Distributed App  : Application that runs across multiple machines.
 * MapReduce        : Map phase + Shuffle/Sort + Reduce phase.
 * Mapper           : Processes input lines, emits (key, value) pairs.
 * Reducer          : Aggregates values for each key, emits final output.
 * Driver           : Configures and submits the MapReduce job.
 * CSV              : Comma Separated Values - structured text file format.
 * split(",")       : Java String method to split by comma delimiter.
 * SingleCountryData[7] : 8th column (index 7) = Country name.
 * output.collect() : Sends (key, value) pair to the Reducer.
 * Shuffle & Sort   : Groups all values for the same key before Reduce.
 * Serialization    : Converting data to bytes for network transmission.
 * Writable         : Hadoop interface for serializable data types.
 * JAR file         : Java Archive - packages all compiled classes.
 * part-00000       : Default name of the output file from Reducer.
 *
 * ------------------------------------------------------------
 * POSSIBLE ORAL EXAM QUESTIONS AND ANSWERS
 * ------------------------------------------------------------
 * Q: What is this practical about?
 * A: It counts the number of sales per country from a CSV file
 *    (Salesjan2009.csv) using Hadoop MapReduce distributed processing.
 *
 * Q: What does the Mapper do in this practical?
 * A: It reads each line of the CSV, splits it by comma, extracts the
 *    country name from column index 7, and emits (country_name, 1).
 *
 * Q: What does the Reducer do?
 * A: It receives all 1s for each country, sums them up, and emits
 *    (country_name, total_sales_count) as the final output.
 *
 * Q: Why do we use split(",") here instead of StringTokenizer?
 * A: Because the input is a CSV file with comma-separated fields.
 *    split(",") splits by comma to get individual columns.
 *    StringTokenizer splits by whitespace (used for plain text like WordCount).
 *
 * Q: What is SingleCountryData[7]?
 * A: After splitting the CSV line by comma, SingleCountryData is an array
 *    of all fields. Index 7 (8th column) contains the country name.
 *
 * Q: Why do we use IntWritable instead of int?
 * A: Hadoop needs to serialize data to send it across the network between
 *    nodes. IntWritable implements the Writable interface for serialization.
 *    Regular Java int cannot be directly serialized by Hadoop.
 *
 * Q: What is the difference between this practical and WordCount?
 * A: Both use the same MapReduce pattern.
 *    WordCount processes plain text and counts word occurrences.
 *    Sales Country processes CSV data and counts sales per country.
 *    The key difference is how data is extracted: StringTokenizer vs split(",").
 *
 * Q: What is the Driver class?
 * A: The Driver (SalesCountryDriver) configures the MapReduce job:
 *    sets input/output paths, Mapper, Reducer, key/value types, and submits it.
 *
 * Q: What does output.collect() do?
 * A: It sends the (key, value) pair from the Mapper to the framework,
 *    which then routes it to the appropriate Reducer after shuffle & sort.
 *
 * Q: How do you view the output after the job runs?
 * A: hdfs dfs -cat /outputfiles/part-00000
 *    Or via Web UI: http://localhost:50070 -> Browse file system.
 * ============================================================
 */
 */

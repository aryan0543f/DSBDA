package com.javatpoint;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output,
            Reporter reporter) throws IOException{
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            output.collect(word, one);
        }
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
 * STEP 3 - Create input file
 * ---------------------------
 *   mkdir ~/wordcount_input
 *   echo "Hadoop is a storage and processing tool. Hadoop is a unit of MapReduce. HDFS is a part of Hadoop." > ~/wordcount_input/input.txt
 *
 * STEP 4 - Upload input to HDFS
 * ------------------------------
 *   hdfs dfs -mkdir /input
 *   hdfs dfs -put ~/wordcount_input/input.txt /input
 *   hdfs dfs -ls /input
 *
 * STEP 5 - Create working directory and place all Java files
 * -----------------------------------------------------------
 *   mkdir ~/wordcount
 *   cd ~/wordcount
 *   (Place WC_Mapper.java, WC_Reducer.java, WC_Runner.java here)
 *
 * STEP 6 - Compile all Java files
 * ---------------------------------
 *   javac -classpath $(hadoop classpath) -d ~/wordcount ~/wordcount/WC_Mapper.java ~/wordcount/WC_Reducer.java ~/wordcount/WC_Runner.java
 *
 * STEP 7 - Create JAR file
 * -------------------------
 *   cd ~/wordcount
 *   jar -cvf wordcount.jar -C . .
 *
 * STEP 8 - Run the MapReduce job
 * --------------------------------
 *   hadoop jar ~/wordcount/wordcount.jar com.javatpoint.WC_Runner /input /r_output
 *
 * STEP 9 - View the output
 * -------------------------
 *   hdfs dfs -cat /r_output/part-00000
 *
 * EXPECTED OUTPUT:
 *   HDFS        1
 *   Hadoop      2
 *   MapReduce   1
 *   a           2
 *   is          2
 *   of          2
 *   processing  1
 *   storage     1
 *   tool        1
 *   unit        1
 *
 * NOTE - If output directory already exists (re-run error):
 * ----------------------------------------------------------
 *   hdfs dfs -rm -r /r_output
 *   (Then run Step 8 again)
 *
 * STEP 10 - Stop Hadoop when done
 * ---------------------------------
 *   stop-dfs.sh
 *   stop-yarn.sh
 *
 * ============================================================
 * 
 * 
 */



/*
 * ============================================================
 * IMPORTANT NOTES FOR ORAL EXAM
 * WORDCOUNT APPLICATION USING MapReduce
 * ============================================================
 *
 * ------------------------------------------------------------
 * WHAT IS THIS PRACTICAL ABOUT?
 * ------------------------------------------------------------
 * This practical counts how many times each word appears in a
 * text file using Hadoop MapReduce.
 * It is the "Hello World" of Hadoop - the most basic and classic
 * MapReduce example used to understand the framework.
 *
 * Files in this practical:
 *   WC_Mapper.java  - The Mapper class  (shown here)
 *   WC_Reducer.java - The Reducer class
 *   WC_Runner.java  - The Driver/Runner class
 *
 * ------------------------------------------------------------
 * WHAT IS HADOOP?
 * ------------------------------------------------------------
 * Hadoop is an open-source framework for storing and processing
 * large amounts of data (Big Data) across a cluster of computers.
 * Two main components:
 *   1. HDFS (Hadoop Distributed File System) - for STORAGE
 *   2. MapReduce                             - for PROCESSING
 *
 * ------------------------------------------------------------
 * WHAT IS HDFS?
 * ------------------------------------------------------------
 * HDFS = Hadoop Distributed File System
 * - Stores large files by splitting them into BLOCKS (default 128 MB).
 * - Each block is replicated across multiple machines (default = 3 copies).
 * - Two main nodes:
 *     NameNode : Master. Stores metadata (file names, block locations).
 *                Does NOT store actual data.
 *     DataNode : Slave. Stores the actual data blocks.
 * - Designed for write-once, read-many access.
 *
 * ------------------------------------------------------------
 * WHAT IS MapReduce?
 * ------------------------------------------------------------
 * MapReduce is a programming model for processing large datasets
 * in parallel across a cluster. THREE phases:
 *
 *   1. MAP PHASE:
 *      - Mapper reads (key, value) pairs and emits intermediate (key, value) pairs.
 *      - WordCount: reads (line_offset, line_text), emits (word, 1) for each word.
 *
 *   2. SHUFFLE AND SORT (automatic):
 *      - Groups all values for the same key together.
 *      - All (word, 1) pairs with the same word go to the same Reducer.
 *      - Sorted alphabetically by key.
 *
 *   3. REDUCE PHASE:
 *      - Reducer receives (key, list_of_values) and produces final output.
 *      - WordCount: receives (word, [1,1,1,...]), sums them -> (word, count).
 *
 * ------------------------------------------------------------
 * CODE EXPLANATION - WC_Mapper
 * ------------------------------------------------------------
 * implements Mapper<LongWritable, Text, Text, IntWritable>
 *   - Four type parameters: <InputKey, InputValue, OutputKey, OutputValue>
 *   - Input Key   : LongWritable - byte offset of the line in the file.
 *   - Input Value : Text         - one full line of text.
 *   - Output Key  : Text         - a single word.
 *   - Output Value: IntWritable  - always 1 (one occurrence).
 *
 * private final static IntWritable one = new IntWritable(1);
 *   - A constant value of 1 shared across all map() calls.
 *   - static and final to avoid creating a new object every time -> efficient.
 *
 * private Text word = new Text();
 *   - Reusable Text object to hold each word.
 *   - Reusing the same object is more memory efficient.
 *
 * map() method step by step:
 *   String line = value.toString();
 *     -> Converts Hadoop Text to a regular Java String.
 *
 *   StringTokenizer tokenizer = new StringTokenizer(line);
 *     -> Splits the line into words by whitespace (space, tab, newline).
 *
 *   while (tokenizer.hasMoreTokens()) {
 *     -> Loops through each word in the line.
 *
 *     word.set(tokenizer.nextToken());
 *     -> Gets the next word and stores it in the reusable Text object.
 *
 *     output.collect(word, one);
 *     -> Emits (word, 1) as an intermediate key-value pair.
 *   }
 *
 * ------------------------------------------------------------
 * WC_Reducer (not shown but important)
 * ------------------------------------------------------------
 * - Receives: (word, [1, 1, 1, ...]) - all 1s for the same word.
 * - Sums up all the 1s.
 * - Emits: (word, total_count)
 * - Example: ("Hadoop", [1, 1]) -> ("Hadoop", 2)
 *
 * ------------------------------------------------------------
 * WC_Runner / Driver (not shown but important)
 * ------------------------------------------------------------
 * - Sets input path  : /input  (text file in HDFS)
 * - Sets output path : /r_output (result in HDFS)
 * - Sets Mapper class : WC_Mapper
 * - Sets Reducer class: WC_Reducer
 * - Sets output key/value types and submits the job.
 *
 * ------------------------------------------------------------
 * FULL MapReduce FLOW FOR WORDCOUNT
 * ------------------------------------------------------------
 * INPUT: "Hadoop is a storage tool. Hadoop is a unit of MapReduce."
 *
 * AFTER MAP:
 *   (Hadoop,1), (is,1), (a,1), (storage,1), (tool,1)
 *   (Hadoop,1), (is,1), (a,1), (unit,1), (of,1), (MapReduce,1)
 *
 * AFTER SHUFFLE & SORT:
 *   (Hadoop,    [1, 1])
 *   (MapReduce, [1])
 *   (a,         [1, 1])
 *   (is,        [1, 1])
 *   ...
 *
 * AFTER REDUCE:
 *   Hadoop      2
 *   MapReduce   1
 *   a           2
 *   is          2
 *   ...
 *
 * ------------------------------------------------------------
 * HADOOP DAEMONS - checked with jps
 * ------------------------------------------------------------
 *   NameNode         : Master of HDFS. Manages file system metadata.
 *   DataNode         : Stores actual data blocks on each slave machine.
 *   ResourceManager  : Master of YARN. Manages cluster resources.
 *   NodeManager      : Manages resources on each slave node.
 *   SecondaryNameNode: Takes periodic checkpoints of NameNode metadata.
 *                      (NOT a backup NameNode!)
 *
 * ------------------------------------------------------------
 * KEY CLASSES USED
 * ------------------------------------------------------------
 * LongWritable   : Hadoop serializable long. Used for line byte offset.
 * IntWritable    : Hadoop serializable int. Used for count = 1.
 * Text           : Hadoop serializable String. Used for words.
 * MapReduceBase  : Base class for Mapper and Reducer (old API).
 * Mapper         : Interface defining the map() method signature.
 * OutputCollector: Collects (key, value) output from Mapper/Reducer.
 * Reporter       : Reports job progress to Hadoop framework.
 * StringTokenizer: Java class to split a string into words by whitespace.
 *
 * WHY Hadoop types instead of Java types?
 *   Hadoop needs to SERIALIZE data to send it across the network.
 *   Hadoop types implement Writable interface for serialization.
 *   Regular Java int/String cannot be directly serialized by Hadoop.
 *
 * ------------------------------------------------------------
 * KEY HDFS COMMANDS
 * ------------------------------------------------------------
 *   start-dfs.sh / stop-dfs.sh           : Start/Stop HDFS
 *   start-yarn.sh / stop-yarn.sh          : Start/Stop YARN
 *   jps                                   : Check running Hadoop daemons
 *   hdfs dfs -mkdir /input                : Create directory in HDFS
 *   hdfs dfs -put input.txt /input        : Upload file to HDFS
 *   hdfs dfs -ls /input                   : List files in HDFS
 *   hdfs dfs -cat /r_output/part-00000    : View output result
 *   hdfs dfs -rm -r /r_output             : Delete output dir (before re-run)
 *   javac -classpath $(hadoop classpath) -d . *.java  : Compile
 *   jar -cvf wordcount.jar -C . .         : Create JAR file
 *   hadoop jar wordcount.jar com.javatpoint.WC_Runner /input /r_output : Run
 *
 * ------------------------------------------------------------
 * KEY TERMS TO REMEMBER
 * ------------------------------------------------------------
 * Hadoop         : Framework for distributed storage and processing of Big Data.
 * HDFS           : Hadoop Distributed File System - stores data in blocks.
 * MapReduce      : Map + Shuffle/Sort + Reduce phases.
 * NameNode       : Master of HDFS - stores metadata only.
 * DataNode       : Slave of HDFS - stores actual data blocks.
 * ResourceManager: Master of YARN - manages cluster resources.
 * NodeManager    : Slave of YARN - manages resources on each node.
 * Mapper         : Processes input lines, emits (key, value) pairs.
 * Reducer        : Aggregates values for each key, emits final output.
 * Driver/Runner  : Configures and submits the MapReduce job.
 * Shuffle & Sort : Groups all values for the same key before Reduce.
 * StringTokenizer: Splits a string into words by whitespace.
 * output.collect(): Sends (key, value) pair to the Reducer.
 * Serialization  : Converting data to bytes for network transmission.
 * Writable       : Hadoop interface for serializable data types.
 * JAR file       : Java Archive - packages compiled classes for deployment.
 * YARN           : Yet Another Resource Negotiator - manages cluster resources.
 * Block          : Unit of storage in HDFS (default 128 MB).
 * Replication    : Storing multiple copies of each block (default = 3).
 * jps            : Java Process Status - shows running Java processes.
 * part-00000     : Default name of the output file from Reducer.
 *
 * ------------------------------------------------------------
 * POSSIBLE ORAL EXAM QUESTIONS AND ANSWERS
 * ------------------------------------------------------------
 * Q: What is Hadoop?
 * A: Hadoop is an open-source framework for distributed storage and processing
 *    of large datasets (Big Data) across a cluster of computers.
 *    Two main components: HDFS (storage) and MapReduce (processing).
 *
 * Q: What is HDFS?
 * A: HDFS stores large files by splitting them into blocks (128 MB each)
 *    distributed across DataNodes. NameNode manages metadata; DataNodes store data.
 *
 * Q: What is MapReduce?
 * A: A programming model for processing large data in parallel.
 *    Map phase emits (key, value) pairs.
 *    Shuffle & Sort groups values by key.
 *    Reduce phase aggregates values to produce final output.
 *
 * Q: What does the Mapper do in WordCount?
 * A: Reads each line, splits it into words using StringTokenizer,
 *    and emits (word, 1) for every word found.
 *
 * Q: What does the Reducer do in WordCount?
 * A: Receives (word, [1,1,1,...]) for each word, sums all the 1s,
 *    and emits (word, total_count) as the final output.
 *
 * Q: What is Shuffle and Sort?
 * A: The automatic phase between Map and Reduce.
 *    All intermediate (key, value) pairs are sorted by key and
 *    all values for the same key are grouped and sent to the same Reducer.
 *
 * Q: What is the difference between NameNode and DataNode?
 * A: NameNode is the master - stores metadata (file names, block locations)
 *    but NOT the actual data.
 *    DataNode is the slave - stores the actual data blocks.
 *
 * Q: Why do we use IntWritable instead of int?
 * A: Hadoop needs to serialize data to send it across the network.
 *    IntWritable implements the Writable interface for serialization.
 *    Regular Java int cannot be directly serialized by Hadoop.
 *
 * Q: What is StringTokenizer?
 * A: A Java class that splits a string into tokens (words) by whitespace.
 *    Used in WordCount to split each line into individual words.
 *
 * Q: What does jps show?
 * A: Shows all running Java processes (Hadoop daemons).
 *    Expected: NameNode, DataNode, ResourceManager, NodeManager, SecondaryNameNode.
 *
 * Q: What is the purpose of the Runner/Driver class?
 * A: Configures and submits the MapReduce job - sets input/output paths,
 *    Mapper class, Reducer class, output key/value types, then runs the job.
 *
 * Q: What is YARN?
 * A: Yet Another Resource Negotiator - the resource management layer of Hadoop.
 *    ResourceManager allocates cluster resources; NodeManager manages each node.
 *
 * Q: What is the expected output of WordCount?
 * A: Each unique word followed by its count, sorted alphabetically.
 *    Example: HDFS=1, Hadoop=2, MapReduce=1, a=2, is=2, of=2, etc.
 * ============================================================
 */

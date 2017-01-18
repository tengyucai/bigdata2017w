package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;


public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  public static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();

    public enum MyCounter { LINE_COUNTER };

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int numWords = 0;
      for (String word : Tokenizer.tokenize(value.toString())) {
        WORD.set(word);
        context.write(WORD, ONE);
        numWords++;
        if (numWords >= 40) break;
      }

      Counter counter = context.getCounter(MyCounter.LINE_COUNTER);
      counter.increment(1L);
    }
  }

  // Reducer: sums up all the counts.
  public static final class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  public static final class MySecondMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final PairOfStrings PAIR = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int numWords = 0;
      Set<String> set = new HashSet<String>();
      for (String word : Tokenizer.tokenize(value.toString())) {
        set.add(word);
        numWords++;
        if (numWords >= 40) break;
      }

      String[] words = new String[set.size()];
      words = set.toArray(words);

      for (int i = 0; i < words.length; i++) {
        for (int j = i + 1; j < words.length; j++) {
          PAIR.set(words[i], words[j]);
          context.write(PAIR, ONE);
          PAIR.set(words[j], words[i]);
          context.write(PAIR, ONE);
        }
      }
    }
  }

  public static final class MySecondCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  public static final class MySecondReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {
    // private static final DoubleWritable PMI = new DoubleWritable();
    // private static final DoubleWritable SUM = new DoubleWritable();
    private static final PairOfFloatInt PMI = new PairOfFloatInt();
    private static final Map<String, Integer> wordCount = new HashMap<String, Integer>();

    private static long numLines;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      numLines = conf.getLong("counter", 0L);

      FileSystem fs = FileSystem.get(conf);
      FileStatus[] status = fs.listStatus(new Path("tmp/"));
      for (int i = 0; i < 5; i++) {
        Path sideDataPath = new Path("tmp/part-r-0000" + Integer.toString(i));
        FSDataInputStream is = fs.open(sideDataPath);
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line = br.readLine();
        while (line != null) {
          String[] data = line.split("\\s+");
          if (data.length == 2) {
            wordCount.put(data[0], Integer.parseInt(data[1]));
          }
          line = br.readLine();
        }
        br.close();
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }

      Configuration conf = context.getConfiguration();
      int threshold = conf.getInt("threshold", 0);

      if (sum >= threshold) {
        String left = key.getLeftElement();
        String right = key.getRightElement();

        float pmi = (float) Math.log10((double)(sum * numLines) / (double)(wordCount.get(left) * wordCount.get(right)));
        PMI.set(pmi, sum);
        context.write(key, PMI);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
    int threshold = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    String sideDataPath = "tmp/";

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
    conf.set("sideDataPath", sideDataPath);
    conf.set("threshold", Integer.toString(args.threshold));
    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName() + "job1");
    job.setJarByClass(PairsPMI.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(sideDataPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(sideDataPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    // Second Job
    long count = job.getCounters().findCounter(MyMapper.MyCounter.LINE_COUNTER).getValue();
    conf.setLong("counter", count);
    Job secondJob = Job.getInstance(conf);
    secondJob.setJobName(PairsPMI.class.getSimpleName() + "job2");
    secondJob.setJarByClass(PairsPMI.class);

    secondJob.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(secondJob, new Path(args.input));
    FileOutputFormat.setOutputPath(secondJob, new Path(args.output));

    secondJob.setMapOutputKeyClass(PairOfStrings.class);
    secondJob.setMapOutputValueClass(IntWritable.class);
    secondJob.setOutputKeyClass(PairOfStrings.class);
    secondJob.setOutputValueClass(PairOfFloatInt.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);

    secondJob.setMapperClass(MySecondMapper.class);
    secondJob.setCombinerClass(MySecondCombiner.class);
    secondJob.setReducerClass(MySecondReducer.class);

    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}

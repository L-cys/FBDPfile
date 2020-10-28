import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;

public class WordCount2 {
    public static class TockenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        static enum CounterEnum { INPUT_WORDS }
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file' " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = (caseSensitive) ? value.toString().toLowerCase() : value.toString();
            String num = "[0-9]+";
            String p = "\\pP";
            /*for (String pattern : patternToSkip) {
                line = line.replaceAll(pattern,"");
            }*/
            line = line.replaceAll(num,"");
            line = line.replaceAll(p,"");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                boolean founded = false;
                String s = itr.nextToken();
                for (String pattern : patternToSkip) {
                    if (s.equals(pattern)) {
                        founded = true;
                        break;
                    }
                }


                if (s.length() >= 3 && !founded) {
                    word.set(s);
                    context.write(word, one);
                    /*Counter counter = context.getCounter(CounterEnum.class.getName(), CounterEnum.INPUT_WORDS.toString());
                    counter.increment(1);*/
                }
                /*word.set(itr.nextToken());
                context.write(word, one);
                Counter counter = context.getCounter(CounterEnum.class.getName(), CounterEnum.INPUT_WORDS.toString());
                counter.increment(1);*/
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, IntWritable,Text> {
        private Text result = new Text();
        private IntWritable ct = new IntWritable();
        int count = 0;
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                if (count >= 100) {
                    break;
                }
                count ++;
                ct.set(count);
                String s = val.toString() + " times: " + key.toString();
                result.set(s);
                context.write(ct,result);

            }
        }
    }


    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();
        Path tempDir = new Path("wordcount-temp-" + Integer.toString(
                new Random().nextInt(Integer.MAX_VALUE)));
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TockenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns",true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, tempDir);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);

        Job sortjob = new Job(conf, "sort");
        FileInputFormat.addInputPath(sortjob, tempDir);
        sortjob.setInputFormatClass(SequenceFileInputFormat.class);
        sortjob.setOutputFormatClass(TextOutputFormat.class);
        sortjob.setMapperClass(InverseMapper.class);
        sortjob.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(sortjob,
                new Path(otherArgs.get(1)));
        sortjob.setOutputKeyClass(IntWritable.class);
        sortjob.setOutputValueClass(Text.class);
        sortjob.setReducerClass(SortReducer.class);
        sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);

        sortjob.waitForCompletion(true);

        FileSystem.get(conf).deleteOnExit(tempDir);
        System.exit(0);
    }

}

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

/**
 * A project to find shared friends from input list.
 * @author chenyuanshan
 */
public class sharedfriends {
    /**
     * First mapper to make input to be [friend, person].
     * Inverse from the original key-value pairs.
     */
    public static class sharedfriendsMapperStepOne extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] record = line.split(",");
            String person = record[0];
            String[] friends = record[1].split("\\s+");
            ArrayList<String> strList = new ArrayList<String>();
            for (int i = 0; i < friends.length; i++) {
                strList.add(friends[i]);
            }
            while (strList.remove(null));
            while (strList.remove(""));
            String strArrLast[] = strList.toArray(new String[strList.size()]);
            for (String friend : strArrLast) {
                k.set(friend);
                v.set(person);
                context.write(k,v);
            }
        }
    }

    /**
     * First Reducer to get [friend, person1, person2, ...].
     */
    public static class sharedfriendsReducerStepOne extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuffer s = new StringBuffer();
            for (Text person : persons) {
                if (s.length() != 0) {
                    s.append(",");
                }
                s.append(person);
            }
            context.write(friend, new Text(s.toString()));
        }
    }

    /**
     * Second mapper to combine two persons into key, make the friend tobe value.
     * Finally get [person1,person2: friend].
     */
    public static class sharedfriendsMapperStepTwo extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] record = line.split("\t");
            String friend = record[0];
            String[] persons = record[1].split(",");
            // To prevent redundancy.
            Arrays.sort(persons);
            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    k = new Text("[" + persons[i] + "," + persons[j] + "]");
                    v = new Text(friend);
                    context.write(k, v);
                }
            }
        }
    }

    /**
     * Second Reducer to gain all the same pair of [person1,person2] and some up the friends.
     */

    public static class sharedfriendsReducerStepTwo extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text pair, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
            StringBuffer s = new StringBuffer();
            s.append("[");
            for (Text friend : friends) {
                s.append(friend);
                s.append(",");
            }
            s.deleteCharAt(s.length()-1);
            s.append("]");
            context.write(pair,new Text(s.toString()));
        }
    }


    /**
     * The main method to control files, input and two set of Map-Reduce.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: sharedfriends <in> [<in>...] <out>");
            System.exit(2);
        }
        /*Path tempDir = new Path("sharedfriends-" + Integer.toString(
                new Random().nextInt(Integer.MAX_VALUE)));*/
        Path tempDir = new Path(otherArgs[otherArgs.length-2]);

        Job job1 = Job.getInstance(conf,"Step one");
        job1.setJarByClass(sharedfriends.class);
        job1.setMapperClass(sharedfriendsMapperStepOne.class);
        job1.setReducerClass(sharedfriendsReducerStepOne.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 2; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job1, tempDir);
        job1.waitForCompletion(true);

        Job job2 = new Job(conf, "Step two");
        FileInputFormat.addInputPath(job2, tempDir);
        job2.setMapperClass(sharedfriendsMapperStepTwo.class);
        job2.setReducerClass(sharedfriendsReducerStepTwo.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2,
                new Path(otherArgs[otherArgs.length-1]));
        job2.waitForCompletion(true);
        //FileSystem.get(conf).deleteOnExit(tempDir);
        System.exit(0);
    }
}

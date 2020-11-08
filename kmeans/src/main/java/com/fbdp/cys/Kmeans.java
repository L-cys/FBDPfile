package com.fbdp.cys;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * The class for the Map-Reduce part.
 * @author chenyuanshan
 */

public class Kmeans {
    /**
     * Get the input and set the init centers randomly.
     * Calculate the distance between each record with center, choose the smallest one.
     */
    public static class kmeansMapperOne extends Mapper<LongWritable, Text, Text, Text> {
        Text ky = new Text();
        // Choose the first three records as initial centers.
        double[][] centers = {{86,43},{5,36},{16,58}};
        int[] kind = new int[2000];
        int count = 0;
        int k = 3;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Read each line from input.
            String line = value.toString();
            String[] index = line.split(",");
            double x = Double.parseDouble(index[0]);
            double y = Double.parseDouble(index[1]);
            double distance = 10000000.0;

            // Calculate the distance between record and each center. Find the smallest one.
            for (int i = 0; i < k; i++ ) {
                double temp = Math.pow(centers[i][0]-x,2) + Math.pow(centers[i][1]-y,2);
                if (temp < distance) {
                    distance = temp;
                    kind[count] = i;
                }
            }
            // Set the index of centers as Key, the coordinate as value.
            ky.set(String.valueOf(kind[count]));
            context.write(ky,value);
            count++;
        }
    }

    /**
     * Set the reducer of initial part. Gain all the point that were separated into the same center.
     * Give the average distance of one centers as output Key, all the point set to that center as Value.
     */
    public static class KmeansReducerOne extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double dis_x = 0.0;
            double dis_y = 0.0;
            int count = 0;
            String temp = "";
            for (Text value : values) {
                String[] v = value.toString().split(",");
                dis_x += Double.parseDouble(v[0]);
                dis_y += Double.parseDouble(v[1]);
                temp+=value.toString()+"#";
                count ++;
            }
            context.write(new Text(String.valueOf(dis_x/count)+","+String.valueOf(dis_y/count)),new Text(temp));
        }
    }

    /**
     * Set damping.
     */
    private static final double damping = 0.85;

    /**
     * Get the original average coordinate of each part as new centers.
     */
    public static class kmeansMapperTwo extends Mapper<LongWritable, Text, Text, Text> {
        int count = 0;
        int[] kind = new int[20000];
        Text ky = new Text();
        private Path[] localFiles;
        static double[][] centers = {{0.0,0.0},{0.0,0.0},{0.0,0.0}};

        /**
         * Get the original output, which would the average coordinate x and y of a cluster from context.
         * Set them as the new centers.
         * The only difference between two mapper would be how to set centers.
         */
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            URI[] URIs = Job.getInstance(conf).getCacheFiles();
            for (URI u : URIs) {
                Path p = new Path(u.getPath());
                String pFileName = p.getName().toString();
                String line;
                BufferedReader br = new BufferedReader(new FileReader(pFileName));
                while ((line = br.readLine()) != null) {
                    //String[] rcd = line.toString().split("\t");
                    double x=Double.valueOf(line.split("\t")[0].split(",")[0]);
                    double y=Double.valueOf(line.split("\t")[0].split(",")[1]);
                    centers[count][0] = x;
                    centers[count][1] = y;
                    count++;
                }
            }



//            localFiles = DistributedCache.getLocalCacheFiles(conf);
//            for (int i = 0; i < localFiles.length; i++) {
//                String line;
//                BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
//                while ((line = br.readLine()) != null) {
//                    String[] rcd = line.split("\t");
//                    double x = Double.parseDouble(rcd[0]);
//                    double y = Double.parseDouble(rcd[1]);
//                    centers[count][0] = x;
//                    centers[count][1] = y;
//                    count++;
//                }
//            }
            count = 0;
        }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] record = line.split(",");
            int k = 3;
            double x = Double.parseDouble(record[0]);
            double y = Double.parseDouble(record[1]);
            double distance = 1000000000.0;
            for (int i = 0;i < k; i++)
            {
                double temp = Math.pow((centers[i][0] - x),2) + Math.pow((centers[i][1] - y),2);

                if(temp < distance) {
                    distance = temp;
                    kind[count] = i;
                }
            }
            ky.set(String.valueOf(kind[count]));
            context.write(ky, value);
            count ++;
        }
    }
//    /**
//     * Final centers c.
//     */
//    static double[][] c = {{0.0,0.0},{0.0,0.0},{0.0,0.0}};
//    /**
//     * A mapper for display the final outcome.
//     * The only difference is how to set the centers.
//     */
//    public static class kmeansMapperThree extends Mapper<IntWritable, Text, Text, Text> {
//        int count = 0;
//        int[] kind=new int[20000];
//        Text ky = new Text();
//        private Path[] localFiles;
//        @Override
//        public void setup(Context context) throws IOException, InterruptedException {
//
//        }
//    }

    /**
     * A reducer for display the final outcome.
     */
    public static class kmeansReducerThree extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key,value);
            }
        }
    }

    /**
     * Main helper for first Map-Reduce section.
     */
    public static void mainHelpOne(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();
        if (remainingArgs.length < 2)  {
            System.err.println("Usage: Kmeans <in> <out>");
            System.exit(2);
        }
        FileSystem filesystem = FileSystem.get(new URI(args[0]),conf);
        Path outPath = new Path(args[1]);
        if(filesystem.exists(outPath)){
            filesystem.delete(outPath, true);
        }
        Job job1 = new Job(conf, "Graph Builder");
        job1.setJarByClass(Kmeans.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(kmeansMapperOne.class);
        job1.setReducerClass(KmeansReducerOne.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
    }

    /**
     * Main helper for second Map-Reduce section.
     * A lot of adjustment made here since file path mess.
     */
    public static void mainHelpTwo(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
//        String[] remainingArgs = optionsParser.getRemainingArgs();
        Job job2 = new Job(conf, "Iter");
        job2.addCacheFile(new Path(args[2]).toUri());


//        Configuration conf = new Configuration();
//        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
//        String[] remainingArgs = optionsParser.getRemainingArgs();
//        if (remainingArgs.length < 3)  {
//            System.err.println("Usage: Kmeans <in> <out>");
//            System.exit(2);
//        }
//        DistributedCache.addCacheFile(new URI(
//                args[2]), conf);
//        FileSystem filesystem = FileSystem.get(new URI(args[0]),conf);
//        Path outPath = new Path(args[1]);
//        if(filesystem.exists(outPath)){
//            filesystem.delete(outPath, true);
//        }

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(kmeansMapperTwo.class);
        job2.setReducerClass(KmeansReducerOne.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);

    }

    public static void mainHelpThree(String[] args) throws Exception {
        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new URI(
                args[2]), conf);
        final FileSystem filesystem = FileSystem.get(new URI(args[0]),conf);
        final Path outPath = new Path(args[1]);
        if(filesystem.exists(outPath)){
            filesystem.delete(outPath, true);
        }
        Job job3 = new Job(conf,"kmeans view");
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapperClass(kmeansMapperTwo.class);
        job3.setReducerClass(kmeansReducerThree.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        job3.waitForCompletion(true);

    }




    public static void main(String[] args) throws Exception {
        // Part one: first init.
        int times = 20;
        String[] MidOutput = { "", args[1] + "/Data0" };
        MidOutput[0] = args[0];
        mainHelpOne(MidOutput);
        //Part two: start iterate.
        String[] forItr = { "", "","" };
        for (int i = 0; i < times; i++) {
            //Input file.
            forItr[0] = args[0];
            //Output path.
            forItr[1] = args[1] + "/Data" + String.valueOf(i + 1);
            //Previous output, read by set up.
            forItr[2]=args[1]+"/Data"+i+"/part-r-00000";
            mainHelpTwo(forItr);
        }
        //Part three: to display the outcome.
        String[] fordis = { args[0],args[1] + "/FinalRank",args[1] + "/Data" + times+"/part-r-00000" };
        mainHelpThree(fordis);
    }
}

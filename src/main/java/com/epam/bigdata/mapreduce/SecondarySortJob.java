package com.epam.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by Ilya_Starushchanka on 9/7/2016.
 */
public class SecondarySortJob {

    public static class SortMapper extends Mapper<LongWritable, Text, CikWritable, Text> {

        private final CikWritable cikWritable = new CikWritable();

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split("\\s+");
            String iPinyouID = columns[2];
            Long timestamp = Long.parseLong(columns[1]);
            cikWritable.setiPinyouID(iPinyouID);
            cikWritable.setTimestamp(timestamp);
            context.write(cikWritable, new Text(line));
        }

    }

    public static class SortReduce extends Reducer<CikWritable, Text, CikWritable, Text>{

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(CikWritable key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(key, text);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: VisitsSpendsCount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Visits Spends count");
        job.setJarByClass(SecondarySortJob.class);
        job.setMapperClass(SortMapper.class);
        // job.setCombinerClass(LogsReducer.class);
        job.setReducerClass(SortReduce.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(CikWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(CikWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(CikPartition.class);
        job.setSortComparatorClass(CikKeyComparator.class);
        job.setGroupingComparatorClass(CikKeyGroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean result = job.waitForCompletion(true);

        /*Counters counters = job.getCounters();
        Counter maxValueCounter = counters.getGroup(StreamIdType.class.getCanonicalName()).findCounter(StreamIdType.SITEIMPRESSION.toString(), false);
        long maxValueCount = maxValueCounter.getValue();*/

        /*for (Counter counter : job.getCounters().getGroup(Browser.class.getCanonicalName())) {
            System.out.println(" - " + counter.getDisplayName() + ": " + counter.getValue());
        }*/
        System.exit(result ? 0 : 1);
    }
}

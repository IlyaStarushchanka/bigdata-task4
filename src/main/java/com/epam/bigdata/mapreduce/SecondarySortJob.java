package com.epam.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
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

    public static class SortReduce extends Reducer<CikWritable, Text, NullWritable, Text>{

        private int maxCounter = 0;
        private String maxIPinyouID;

        @Override
        protected void reduce(CikWritable key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            int tempCounter = 0;
            for (Text text : values) {
                context.write(NullWritable.get(), text);
                String[] columns = text.toString().split("\\s+");
                int streamId = Integer.parseInt(columns[columns.length - 1]);
                if (streamId == 1){
                    tempCounter++;
                }
            }

            if (maxCounter <= tempCounter){
                maxCounter = tempCounter;
                maxIPinyouID = key.getiPinyouID();
                //context.getCounter("DinamicCounter",key.getiPinyouID()).setValue(maxCounter);
                //context.getCounter("site-impression","1").setValue(maxCounter);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (null != maxIPinyouID) {
                context.getCounter("Site-impression", maxIPinyouID).setValue(maxCounter);
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
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(CikPartition.class);
        job.setSortComparatorClass(CikKeyComparator.class);
        job.setGroupingComparatorClass(CikKeyGroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean result = job.waitForCompletion(true);
        Counters counters = job.getCounters();

        //long maxSiteSmesh = counters.findCounter("Site-impression","1").getValue();
        //System.out.println("test1 " + maxSiteSmesh);
        long maxSiteSmesh = 0;
        String iPinyouID = null;
        for (Counter counter : counters.getGroup("Site-impression")) {
            if (maxSiteSmesh < counter.getValue()) {
                maxSiteSmesh = counter.getValue();
                iPinyouID = counter.getName();

            }
        }
        if (iPinyouID != null) {
            System.out.println("iPinyou ID: " + iPinyouID + ", the biggest amount of site impression: " + maxSiteSmesh);
        }
        System.exit(result ? 0 : 1);
    }
}

package com.epam.bigdata.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Ilya_Starushchanka on 9/7/2016.
 */
public class CikPartition extends Partitioner<CikWritable, NullWritable> {

    @Override
    public int getPartition(CikWritable cikWritable, NullWritable nullWritable, int numPartitions) {
        return cikWritable.getiPinyouID().hashCode() % numPartitions;
    }
}

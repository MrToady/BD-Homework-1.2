import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class BytesCountJob {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf, "Bytes count");
        job.setJarByClass(BytesCountJob.class);
        job.setMapperClass(BytesCountMapper.class);
        job.setReducerClass(BytesCountReducer.class);
        job.setCombinerClass(BytesCountCombiner.class);
        job.setSortComparatorClass(IntWritable.Comparator.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IPBytesWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(AverageBytesWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


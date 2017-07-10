import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;

public class SuffixArrayRun{
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
          System.err.printf("Usage: SuffixArrayRun <input> <output>\n");
          System.exit(-1);
        }


        Job job = new Job(new Configuration());
        Configuration conf = job.getConfiguration();
        // Specify various job-specific parameters     
        job.setJobName("Run SuffixArray for Bio Info (64) 160w CMS GC MGET Suffix");
        //job.setJobName("Run SuffixArray for Bio Info (32) 160W CMS AlwaysTenure NewRatio=5");

        job.setJarByClass(SuffixArrayRun.class);
     
        job.setMapperClass(BioMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        //job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(BioReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(BioPartitioner.class);
        job.setNumReduceTasks(64);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        //MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, BmpMapper.class);
        //MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, BmpMapper.class);
        //MultipleInputs.addInputPath(job, new Path(args[2]), SequenceFileInputFormat.class, BmpMapper.class);
        //MultipleInputs.addInputPath(job, new Path(args[3]), SequenceFileInputFormat.class, BmpMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job, then poll for progress until the job is complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

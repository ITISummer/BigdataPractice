package com.itis.mrpractice.grptopn_62;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class PracticeDriver implements Tool {
    private Configuration conf;

    /**
     * 在此填写Driver代码，并将运行结果（0或者1）返回。Job申请对象时使用成员变量conf
     * 将结果输出到output/practice文件夹，其他文件夹无效。
     * @param args org.apache.hadoop.shaded.com.and specific arguments.
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJobName("GrpTopN");

        job.setJarByClass(PracticeDriver.class);
        job.setMapperClass(PracticeMapper.class);
        job.setReducerClass(PracticeReducer.class);
        job.setCombinerClass(PracticeReducer.class);

        job.setCombinerKeyGroupingComparatorClass(UserIdGroupComparator.class);
        job.setGroupingComparatorClass(UserIdGroupComparator.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputKeyClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("input"));
        FileOutputFormat.setOutputPath(job,new Path("output/practice"));
        return job.waitForCompletion(true) ? 0 : 1;

    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}

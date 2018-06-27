c
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * using combiner
 */

public class EdgeAvgLen1 {



    public static class SumCountPairBean implements Writable{
        /**
         * SumCountPairBean class implements Writable interface
         * sum --> sum
         * cnt -->count
         */

        private Double sum;
        private Integer cnt;

        //non-para constructor for Non serializable
        public SumCountPairBean(){}
        //params constructor for Serializable
        public SumCountPairBean(Double sum, Integer cnt) {
            this.sum = sum;
            this.cnt = cnt;
        }

        //getter and setter
        public Double getSum() {
            return sum;
        }

        public void setSum(Double sum) {
            this.sum = sum;
        }

        public Integer getCnt() {
            return cnt;
        }

        public void setCnt(Integer cnt) {
            this.cnt = cnt;
        }

        // override write and readFields function

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(this.sum);
            dataOutput.writeInt(this.cnt);


        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            sum = dataInput.readDouble();
            cnt = dataInput.readInt();

        }
    }


    public  static class EdgeMapper extends Mapper<LongWritable,Text,IntWritable,SumCountPairBean>{

        /**
         * @param value string like "0 3466 937 9.25"
         * map : split string, extract incoming length of edges and node id
         * keyoutput :nodeid
         * transfer len of edge to (len,1), we dont accumulate the count of each nodeid
         * valueoutput: (sum , cnt=1)
         * when we finish one file then send to combiner or reducer
         */

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] val = line.split(" ");

            String nodeId = val[val.length-2];
            double lenOfIn = Double.parseDouble(val[val.length-1]);
            // key(nodeid) : (sum , cnt=1)   ----> to combiner
            context.write(new IntWritable(Integer.parseInt(nodeId)),  new SumCountPairBean(lenOfIn,1));
        }
    }




    public static class EdgeCombiner extends Reducer<IntWritable,SumCountPairBean,IntWritable,SumCountPairBean>{
        /**
         *accumulate len of each node
         */

        @Override
        protected void reduce(IntWritable key, Iterable<SumCountPairBean> values, Context context) throws IOException, InterruptedException {

            Double sum  = 0.0;
            Integer cnt = 0;
            // receive a iterable stream of key: (len,1)  ;key: (len,1)  ;key: (len,1)  ;......
            // combine all the vals as key; (sum_len,sum_cnt)
            for(SumCountPairBean val:values
                 ) {
                sum += val.getSum();
                cnt +=val.getCnt();
            }
            context.write(key,new SumCountPairBean(sum,cnt));

        }
    }


    public static class EdgeReducer extends Reducer<IntWritable ,SumCountPairBean,IntWritable,DoubleWritable> {
        /**
         *receive all (k,v) pairs,do final accumulation for each node
         */
        @Override
        protected void reduce(IntWritable key, Iterable<SumCountPairBean> values, Context context) throws IOException, InterruptedException {

            Double total_sum  = 0.0;
            Integer total_cnt = 0;
            for (SumCountPairBean val:values
                 ) {
                total_sum +=val.getSum();
                total_cnt +=val.getCnt();
            }

            DoubleWritable r = new DoubleWritable();
            r.set(total_sum / total_cnt);

            context.write(key,r);

        }


    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "combiner");

        job.setJarByClass(EdgeAvgLen1.class);

        job.setMapperClass(EdgeAvgLen1.EdgeMapper.class);
        job.setCombinerClass(EdgeAvgLen1.EdgeCombiner.class);
        job.setReducerClass(EdgeAvgLen1.EdgeReducer.class);

        // we need specify setMapOutputKeyClass and setMapOutputValueClass when we use combiner
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SumCountPairBean.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }










}

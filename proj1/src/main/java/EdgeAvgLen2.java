import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class EdgeAvgLen2 {


    public static class SumCountPairBean implements Writable{
        /**
         * same with EdgeAvgLen1
         */

        private Double sum;
        private Integer cnt;
        //constructor
        public SumCountPairBean(){}
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

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(sum);
            dataOutput.writeInt(cnt);

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            sum = dataInput.readDouble();
            cnt = dataInput.readInt();

        }
    }


    public static class EdgeMapper extends Mapper<LongWritable,Text,IntWritable,SumCountPairBean>{


        /**
         * setup function means to initialize a Map<String,SumCountPairBean>
         * map function do the accumulation work for exists or new nodes
         * cleanup function called when task completed and emit final key-value pairs
         */

        private  Map<String,SumCountPairBean> nodeAndEdge ;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // create a hashmap (nodeid---> set of length)
            nodeAndEdge = new HashMap<String, SumCountPairBean>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] val = line.split(" ");
            // key(node)
            String nodeId = val[val.length-2];
            // value(edge)
            Double lenOfEdge =Double.parseDouble(val[val.length-1]);

            // whether nodeAndEdge already exists node
            SumCountPairBean s ;
            if(nodeAndEdge.containsKey(nodeId)){
                s = new SumCountPairBean(nodeAndEdge.get(nodeId).getSum()+lenOfEdge,nodeAndEdge.get(nodeId).getCnt() +1);
                nodeAndEdge.put(nodeId,s);
            }else{
                nodeAndEdge.put(nodeId,new SumCountPairBean(lenOfEdge,1));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // send data to reducer until all theses files completed combination

            for (Map.Entry<String, SumCountPairBean> items : nodeAndEdge.entrySet()) {
                String sKey = items.getKey();
                SumCountPairBean svalue = items.getValue();
                context.write(new IntWritable(Integer.parseInt(sKey)), new SumCountPairBean(svalue.getSum(), svalue.getCnt()));
            }
        }
    }



    public static class EdgeReducer extends Reducer<IntWritable,SumCountPairBean,IntWritable,DoubleWritable>{

        /**
         * accumulation
         *same with EdgeAvgLen1
         */

        @Override
        protected void reduce(IntWritable key, Iterable<SumCountPairBean> values, Context context) throws IOException, InterruptedException {

            Double final_sum = 0.0;
            Integer final_cnt = 0;
            for (SumCountPairBean val:values
                 ) {
                final_sum +=val.getSum();
                final_cnt +=val.getCnt();
            }


            Double r;
            r = final_sum / (double)final_cnt;


            context.write(key,new DoubleWritable(r));


        }
    }



    public static void main(String[] args) throws Exception{


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "in_mapping");


        job.setJarByClass(EdgeAvgLen2.class);

        job.setMapperClass(EdgeAvgLen2.EdgeMapper.class);
        job.setReducerClass(EdgeAvgLen2.EdgeReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SumCountPairBean.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);




    }
}

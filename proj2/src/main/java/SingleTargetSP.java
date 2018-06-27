package comp9313.ass2;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;


public class SingleTargetSP {


    enum Counter{
		changed
	}


    private static String OUT = "output";
    private static String IN = "input";
    private static Integer TARGETID = 22937;
    private static String HOSTNAME = "hdfs://localhost:9000";


    public static class Node{


        // nodeid   distance|HashMap<Integer,Double> neighnodes|LinkedList<Integer> path
        private int nodeID;
        private String  distance;
        private HashMap<Integer,Double> neighnodes;
        private LinkedList<Integer> path;
        // using to check whether path had been changed
        private String temppath2 =null;
        private String temppath3 = null;


        // getter and setter

        public int getNodeID() {
            return nodeID;
        }
        public void setNodeID(int nodeID) {
            this.nodeID = nodeID;
        }
        public String getDistance() {
            return distance;
        }
        public void setDistance(String distance) {
            this.distance = distance;
        }
        public HashMap<Integer, Double> getNeighnodes() {
            return neighnodes;
        }
        public void setNeighnodes(HashMap<Integer, Double> neighnodes) {
            this.neighnodes = neighnodes;
        }
        public LinkedList<Integer> getPath() {
            return path;
        }
        public void setPath(LinkedList<Integer> path) {
            this.path = path;
        }


        public Node(){}



        public Node(String s){

//            0	0|{2=5.0, 4=2.0}|[0]
//            1	inf|{0=10.0, 2=3.0}|[]
//            2	inf|{1=3.0}|[]
//            3	inf|{1=1.0, 2=9.0, 4=6.0}|[]
//            4	inf|{2=2.0, 3=4.0}|[]

            String[] compns = s.split("\\|");
            String[] tid_distance = compns[0].split("\t");
            this.nodeID = Integer.parseInt(tid_distance[0].trim());
            this.distance = tid_distance[1].trim();
            this.neighnodes = Parser_neistr(compns[1]);
            this.path = Parser_pathstr(compns[2]);
        }

        //return the shortest distance
        private String DistanceCompare(String d1,String d2){
            if(d1.equals("inf")){
                return d2;
            }else if(d2.equals("inf")){
                return d1;
            }else {
                Double dd1 =Double.valueOf(d1);
                Double dd2 = Double.valueOf(d2);
                if(dd1 > dd2){
                    return d2;
                }else{
                    return d1;
                }
            }
        }


        /**
         *
         * @param s There are two types of input string s:
         *          type1. inf|{1:3.0}|[] which length equal to 3
         *          type2. 5|[0,2] which length equal to 2
         * @return  regardless of the order of type1 or type2
         *          this function always set Node attributes to the one has shortest distance
         *          the return value represent in the process, whether path has been changed
         *          if changed return true, otherwise false
         */
        public Boolean  update(String s){
            // inf|1:3.0|[]    5|0,2
            String[] compns = s.split("\\|");


            if(compns.length == 2){
                String _distance = compns[0];
                LinkedList<Integer> _path = Parser_pathstr(compns[1]);
                this.temppath2 = _path.toString().trim();
                // 5|0,2
                if(this.distance==null || _distance.equals(DistanceCompare(_distance,this.distance))){
                    //_distance is smaller
                    this.distance = _distance;
					this.path = _path;
                }
            }else {
                // inf|1:3.0|[]
                String _distance = compns[0];
                HashMap<Integer,Double> _neighbours= Parser_neistr(compns[1]);
                LinkedList<Integer> _path = Parser_pathstr(compns[2]);
                this.temppath3 = _path.toString().trim();
                if(this.distance==null || _distance.equals(DistanceCompare(_distance,this.distance))){
                    //_distance is smaller
                    this.distance = _distance;
                    this.neighnodes = _neighbours;
                    this.path=_path;

                }
                if(this.neighnodes ==null){
                    this.neighnodes = _neighbours;
                }
            }

            // using temppath2 and temppath3 to record the path change
            if(this.temppath3!=null && !this.path.toString().equals(this.temppath3)){
            	return true;
			}else {
            	return false;
			}

        }





        /**
         *
         * @param s parser the string '{2=5.0, 4=2.0}'
         * @return  HashMap<Integer,Double>  adjanceny list
         */

        public HashMap<Integer,Double> Parser_neistr(String s)
        {
            String compns_1 = s.trim();
            compns_1 = compns_1.substring(1,compns_1.length()-1);
            HashMap<Integer,Double> neighnodes = new HashMap<Integer, Double>();
            // isolated points
            if(compns_1.isEmpty()){
                return neighnodes;
            }


            String[] neighnodes_strs= compns_1.split( ",");

//            HashMap<Integer,Double> neighnodes = new HashMap<Integer, Double>();
            for (String neighnodes_str:neighnodes_strs
                    ) {
                Integer _k = Integer.valueOf(neighnodes_str.split("=")[0].trim());
                Double _v = Double.valueOf(neighnodes_str.split("=")[1].trim());
                neighnodes.put(_k,_v);
            }
            return neighnodes;
        }

        /**
         *
         * @param s parser the string '[0,4,6]'
         * @return LinkedList<Integer> path
         */

        public LinkedList<Integer> Parser_pathstr(String s){
            LinkedList<Integer> path = new LinkedList<Integer>();
            String compns_2 = s.trim();
            String[] path_strs = compns_2.substring(1,compns_2.length()-1).split(",");
            for (String path_str :path_strs
                    ) {
                if(!path_str.equals("")){
                    path.add(Integer.valueOf(path_str.trim()));
                }
            }
            return path;
        }



        public String valueToString(){


            // if the node has no coming edge, set as "[]" empty list
            if(this.neighnodes==null){
                HashMap<Integer,Double> n = new HashMap<Integer, Double>();
                this.neighnodes = n;
            }

            return this.distance + "|" + this.neighnodes.toString() +"|"+  this.path.toString();
        }

    }


    public static class STMapper extends Mapper<Object, Text, LongWritable, Text> {

        @Override
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

//            0,0|{2=5.0, 4=2.0}|[0]
//            1,inf|{0=10.0, 2=3.0}|[]
//            2,inf|{1=3.0}|[]
//            3,inf|{1=1.0, 2=9.0, 4=6.0}|[]
//            4,inf|{2=2.0, 3=4.0}|[]

            // YOUR JOB: map function


            /**
             *  0,0|{2=5.0, 4=2.0}|[0]
             * emit
             * 2,5|[0,2], 4,2|[0,4],0,0|{2=5.0, 4=2.0}|[0]
             */

            Node node= new Node(value.toString());

            if(node.distance.equals("inf")){
                context.write(new LongWritable(node.nodeID),new Text(node.valueToString()));
            }else{
                context.write(new LongWritable(node.nodeID),new Text(node.valueToString()));

                for (Map.Entry<Integer,Double> e:node.getNeighnodes().entrySet()
                     ) {
                    // 0,0|{2=5.0, 4=2.0}|[0] --- >  2,5|0,2
                    Integer neinode = e.getKey();
                    Double neidistance = e.getValue();
                    Double newdistance = Double.valueOf(node.distance) + neidistance;
                    LinkedList<Integer> newPath = (LinkedList<Integer>) node.getPath().clone();
                    newPath.add(neinode);
                    String value_string = newdistance.toString() + "|" + newPath.toString();
                    //text
                    context.write(new LongWritable(neinode),new Text(value_string));
                }
            }


        }

    }


    public static class STReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // YOUR JOB: reduce function
            // ... ...
//
//            0,0|2:5.0 4:2.0|0
//            1,inf|0:10.0 2:3.0|
//            2,inf|1:3.0|[]    5|0,2
//            3,inf|1:1.0 2:9.0 4:6.0|
//            4,inf|2:2.0 3:4.0|[]     2|0,4


            /**
             * 1. combine all values for each key
             * 2.if there have change of path in the process, add 1 to counter
             */

            Node node = new Node();
            node.setNodeID((int) key.get());
            Boolean b =false;
            for (Text str:values
                 ) {
                b= node.update(str.toString());
            }

			if(b.equals(true)){
            	context.getCounter(Counter.changed).increment(1);
			}

			context.write(key,new Text(node.valueToString()));




        }
    }



    public static class FLMapper extends Mapper<Object, Text, LongWritable, Text> {

        @Override
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            //0	0|{2=5.0, 4=2.0}|[0]
            //1	7.0|{0=10.0, 2=3.0}|[0, 4, 3, 1]
            //2	4.0|{1=3.0}|[0, 4, 2]
            //3	6.0|{1=1.0, 2=9.0, 4=6.0}|[0, 4, 3]
            //4	2.0|{2=2.0, 3=4.0}|[0, 4]
            Node e = new Node(value.toString());
            context.write(new LongWritable(e.nodeID),new Text(e.valueToString()));

        }

    }


    public static class FLReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //0	0|{2=5.0, 4=2.0}|[0]
            //1	7.0|{0=10.0, 2=3.0}|[0, 4, 3, 1]
            //2	4.0|{1=3.0}|[0, 4, 2]
            //3	6.0|{1=1.0, 2=9.0, 4=6.0}|[0, 4, 3]
            //4	2.0|{2=2.0, 3=4.0}|[0, 4]

            Node node = new Node();
            node.setNodeID((int) key.get());
            String final_string ="";
            String print_path="";
            Integer targetid=0;


            /**
             * 1.extract the final results
             * 2.remove target value from distance
             */
            for (Text str:values
                    ) {
                node.update(str.toString());
                LinkedList<Integer> l = node.path;

                while (l.size()>1){
                    print_path  = print_path +  String.valueOf(l.remove(l.size()-1)) + "->";
                }
                if(!l.isEmpty()){
                    targetid = l.remove(l.size()-1);
                    print_path += String.valueOf(targetid);
                }


                if(!node.distance.equals("inf")){

                    Double final_distance = Double.parseDouble(node.distance) - (double)targetid;
                    final_string = String.valueOf(final_distance) + "\t" + print_path;
                }

            }
            if(!final_string.isEmpty()){
                context.write(key,new Text(final_string));
            }




        }
    }




    public static class BGMapper extends Mapper<Object, Text, LongWritable, Text> {

        @Override
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

//            2 3466 8579 6.58
//            3 3466 10310 3.32
//            4 3466 15931 8.43
//            5 3466 17038 8.94
//            6 3466 18720 1.63
//            7 3466 19607 9.75
            String line=value.toString();
            String[] val = line.split(" ");
            Integer nodeid = Integer.valueOf(val[val.length-2]);
            String neig_lenofneig = val[val.length-3] + " " + val[val.length-1];
            context.write(new LongWritable(nodeid),new Text(neig_lenofneig));

        }

    }


    public static class BGReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Node e = new Node();
            e.setNodeID((int)key.get());

            HashMap<Integer,Double> adj = new HashMap<Integer, Double>();
            for (Text str:values
                 ) {
                String[] nei_len = str.toString().split(" ");
                adj.put(Integer.valueOf(nei_len[0]),Double.valueOf(nei_len[1]));
            }
            e.setNeighnodes(adj);
            Configuration conf = context.getConfiguration();
            String targetid = conf.get("TARGETID");
            LinkedList<Integer> path = new LinkedList<Integer>();


            if(String.valueOf(e.nodeID).equals(targetid)){
                e.setDistance(targetid);
                path.add(Integer.valueOf(targetid));
                e.setPath(path);

            }else {
                e.setDistance("inf");
                e.setPath(path);


            }


            context.write(key,new Text(e.valueToString()));

        }
    }






    public static void main(String[] args) throws Exception {



        IN = args[0];
        OUT = args[1];
        TARGETID = Integer.valueOf(args[2]);

        int iteration = 0;
        String input = IN;
        String output = OUT + iteration;
        Integer targetid = TARGETID;

	    // YOUR JOB: Convert the input file to the desired format for iteration, i.e., 
        //           create the adjacency list and initialize the distances

        Configuration conf = new Configuration();
        conf.set("TARGETID",args[2]);
        Job job;
        job= Job.getInstance(conf,"initialize");

        job.setJarByClass(SingleTargetSP.class);
        job.setMapperClass(SingleTargetSP.BGMapper.class);
        job.setReducerClass(SingleTargetSP.BGReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);


        input = output;
        iteration ++;
        output = OUT + iteration;



        boolean isdone = false;
		while (isdone == false) {


			conf = new Configuration();
			// YOUR JOB: Configure and run the MapReduce job
            job = Job.getInstance(conf,"Dijkstra");
            job.setJarByClass(SingleTargetSP.class);
            job.setMapperClass(SingleTargetSP.STMapper.class);
            job.setReducerClass(SingleTargetSP.STReducer.class);
//
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
//
            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);



			//You can consider to delete the output folder in the previous iteration to save disk space.
   //          File input_folder = new File(input);
   //          FileUtils.cleanDirectory(input_folder);
			// input_folder.delete();


           FileSystem hdfs = FileSystem.get(URI.create(HOSTNAME),conf);
           if(hdfs.exists(new Path(input)) && iteration>0){
               hdfs.delete(new Path(input),true);
           }


            input = output;
            iteration ++;
            output = OUT + iteration;


			// YOUR JOB: Check the termination criterion by utilizing the counter
            // ... ...

            /**
             *
             * when there has no changes in a whole iteration,
             * the counter value will be zero
             */
            Counters counter = job.getCounters();
			if(counter.findCounter(Counter.changed).getValue()==0){
				isdone=true;
			}
        }



        // YOUR JOB: Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
        // ... ...

        conf = new Configuration();
        // YOUR JOB: Configure and run the MapReduce job
        Job job2 = Job.getInstance(conf,"final");

        job2.setJarByClass(SingleTargetSP.class);
        job2.setMapperClass(SingleTargetSP.FLMapper.class);
        job2.setReducerClass(SingleTargetSP.FLReducer.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2,new Path(input));
        FileOutputFormat.setOutputPath(job2, new Path(output));
        job2.waitForCompletion(true);


       FileSystem hdfs = FileSystem.get(URI.create(HOSTNAME),conf);
       if(hdfs.exists(new Path(input)) && iteration>0){
           hdfs.delete(new Path(input),true);
       }


        // File input_folder = new File(input);
        // FileUtils.cleanDirectory(input_folder);
        // input_folder.delete();

    }


    /**
     * using graph to store edge-edge-distance relationship
     */

    public static class Graph {

        HashMap<Integer, HashMap<Integer,Double>> node_adj;

        public Graph(){
            node_adj = new HashMap<Integer, HashMap<Integer, Double>>();
        }


        public void addNeighbor(int u, int v, double d){

            HashMap<Integer,Double> adj = new HashMap<Integer, Double>();
            if(node_adj.containsKey(u)){
                node_adj.get(u).put(v,d);
            }else {
                adj.put(v,d);
                node_adj.put(u,adj);
            }

        }
    }



}


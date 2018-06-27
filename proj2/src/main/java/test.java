import java.util.*;

public class test {
//
//    public static class Node{
//
//        private int nodeID;
//        private String  distance;
//        private HashMap<Integer,Double> neighnodes;
//        private LinkedList<Integer> path;
//
//        public int getNodeID() {
//            return nodeID;
//        }
//
//        public void setNodeID(int nodeID) {
//            this.nodeID = nodeID;
//        }
//
//        public String getDistance() {
//            return distance;
//        }
//
//        public void setDistance(String distance) {
//            this.distance = distance;
//        }
//
//        public HashMap<Integer, Double> getNeighnodes() {
//            return neighnodes;
//        }
//
//        public void setNeighnodes(HashMap<Integer, Double> neighnodes) {
//            this.neighnodes = neighnodes;
//        }
//
//        public LinkedList<Integer> getPath() {
//            return path;
//        }
//
//        public void setPath(LinkedList<Integer> path) {
//            this.path = path;
//        }
//
//
//        public Node(){}
//
//
//
//        public Node(String s){
//
//            String[] compns = s.split("\\|");
//            String[] tid_distance = compns[0].split("\t");
//            this.nodeID = Integer.parseInt(tid_distance[0].trim());
//            this.distance = tid_distance[1].trim();
//            this.neighnodes = Parser_neistr(compns[1]);
//            this.path = Parser_pathstr(compns[2]);
//        }
//
//
//        private String DistanceCompare(String d1,String d2){
//            if(d1.equals("inf")){
//                return d2;
//            }else if(d2.equals("inf")){
//                return d1;
//            }else {
//                Double dd1 =Double.valueOf(d1);
//                Double dd2 = Double.valueOf(d2);
//                if(dd1 > dd2){
//                    return d2;
//                }else{
//                    return d1;
//                }
//            }
//        }
//
//        public void update(String s){
//            // inf|1:3.0|[]    5|0,2
//            String[] compns = s.split("\\|");
//
//
//            if(compns.length == 2){
//                String _distance = compns[0];
//                LinkedList<Integer> _path = Parser_pathstr(compns[1]);
//                // 5|0,2
//                if(this.distance==null || _distance.equals(DistanceCompare(_distance,this.distance))){
//                    //_distance is smaller
//                    this.distance = _distance;
//                    this.path = _path;
//                }
//            }else {
//                // inf|1:3.0|[]
//                String _distance = compns[0];
//                HashMap<Integer,Double> _neighbours= Parser_neistr(compns[1]);
//                LinkedList<Integer> _path = Parser_pathstr(compns[2]);
//                if(this.distance==null || _distance.equals(DistanceCompare(_distance,this.distance))){
//                    //_distance is smaller
//                    this.distance = _distance;
//                    this.neighnodes = _neighbours;
//                    this.path = _path;
//                }
//
//                if(this.neighnodes ==null){
//                    this.neighnodes = _neighbours;
//                }
//            }
//
//
//
//        }
//
//
//
//        private HashMap<Integer,Double> Parser_neistr(String s)
//        {
//            String compns_1 = s.trim();
//            compns_1 = compns_1.substring(1,compns_1.length()-1);
//            String[] neighnodes_strs= compns_1.split( ",");
//
//            HashMap<Integer,Double> neighnodes = new HashMap<Integer, Double>();
//            for (String neighnodes_str:neighnodes_strs
//                    ) {
//                Integer _k = Integer.valueOf(neighnodes_str.split("=")[0].trim());
//                Double _v = Double.valueOf(neighnodes_str.split("=")[1].trim());
//                neighnodes.put(_k,_v);
//            }
//            return neighnodes;
//        }
//
//        private LinkedList<Integer> Parser_pathstr(String s){
//            LinkedList<Integer> path = new LinkedList<Integer>();
//            String compns_2 = s.trim();
//            String[] path_strs = compns_2.substring(1,compns_2.length()-1).split(",");
//            for (String path_str :path_strs
//                    ) {
//                if(!path_str.equals("")){
//                    path.add(Integer.valueOf(path_str.trim()));
//                }
//            }
//            return path;
//        }
//
//
//        public String valueToString(){
//            return this.distance + "|" + this.neighnodes.toString() +"|"+  this.path.toString();
//        }
//
//    }
//
//
//
//

    public static void main(String[] args){
//
//
//        0 0|{2=5.0, 4=2.0}|[0]
//        1	inf|{0=10.0, 2=3.0}|[]
//        2	inf|{1=3.0}|[]
//        3	inf|{1=1.0, 2=9.0, 4=6.0}|[]
//        4	inf|{2=2.0, 3=4.0}|[]


//        LinkedList<Integer> l = new LinkedList<Integer>();
//
//        l.add(2);
//        l.add(3);
//        l.add(4);
//
//        String pp = "";
//
//
//        while (l.size()>1){
//            pp += String.valueOf(l.remove(l.size()-1)) + "->";
//        }
//        pp += String.valueOf(l.remove(l.size()-1));

//        String s = null;
//        System.out.println(s.toString());
//









    }

//
//
//
//

		}

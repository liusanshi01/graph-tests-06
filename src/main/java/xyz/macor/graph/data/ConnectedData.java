package xyz.macor.graph.data;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class ConnectedData {

    //private static List<List<String>> fromToName = new ArrayList<List<String>>();  // ftomMobile,toMobile,name
    //final private static long[] toMobiles = {15201922461l,15618387902l,13879290459l,17183450901l,17717037901l,};
    final private static String[][] tonameArray = {{"赫卡忒","Hecate","魔法的女神","幽灵的女神","夜之女神"},
            {"该亚","Gaea","大地女神","尤拉诺斯之母","创造了大地"},
            {"尤拉诺斯","Uranus","天之神","第一任神王","该亚的长子"},
            {"普罗米修斯","Prometheus","先知者","人类的创造者","泰坦爱泼特斯之子"},
            {"厄毗米修斯","Epimetheus","后知者","潘多拉的丈夫","普罗米修斯的兄弟"}};

    final private static String[][] nameArray = {
            {"宙斯","第三代神王zeus","雷神宙斯","宙斯1"},
            {"赫拉","天后赫拉", "天后赫拉","Hera"},
            {"雅典娜","Athene","智慧女神雅典娜","女神"}
            };
    final private static long[][] possibleMobiles = {{15201922990l,15618387902l,18593470202l,15201926868l},
            {17183450901l,18593470211l,13222089822l,15651078465l},
            {15201922461l,13789023467l,17718379400l,18891832346l}
            };

    private int relationshipCountPerNode = 200;
    private  Map<String,List<String>> possibleMobileMap = new HashMap<String, List<String>>();  // {"156",[7902,2990,]}  一个号码对应可能的号码
    private  Map<String,List<String>> possibleNameMap = new HashMap<String, List<String>>();  // {"156",["宙斯","雷神宙斯","宙斯1"]} 一个号码对应可能的名字
    //private static Map<String,List<String>> mobileRelationMap = new HashMap<String, List<String>>();  // {"156",[7902,2990,]}  跟这个号码有联系的号码

    //private static Map<String,List<List<String>>> phoneNameMap = new HashMap<String, List<List<String>>>();

    private List<String> relationList = new ArrayList<String>();

    //private Map<String,List<String>> mobileMap = new HashMap<String, List<String>>();

    // 1。 固定的几个号码和随机商城的200多个号码建立联系
    // 2。 固定号码与指定的几个号码建立联系
    // 3。 200多个号码再随机与其他两百多个号码以及第一步的号码集建立联系(名字列表)

    public static void generateFromRelationData(){
       /* String a = "15201922990"; // 宙斯
        for(long m:fromMobiles) {
            // md5(from)-to ## name | from
            Bytes.toBytes(a);

        }*/

    }

    public void setRelationshipCountPerNode(int relationshipCountPerNode) {
        this.relationshipCountPerNode = relationshipCountPerNode;
    }

    public List<String> generate400FromRelationship(String mp){
        List<String> mList =new ArrayList<String>(260);
        List<String> dataList = new ArrayList<String>(260);
        Random random = new Random();
        long raw =  15600009900L;
        for(int i = 0; i < 400;i++){
            StringBuffer sb = new StringBuffer();
            long m = raw + random.nextInt(1000);
            mList.add(String.valueOf(m));
            String rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes(mp));
            sb.append(rowKey + "-" + m + " ## ");
            sb.append(nameArray[random.nextInt(5)][random.nextInt(5)]);
            sb.append(" | ");
            sb.append(String.valueOf(m));

            dataList.add(sb.toString());   // Todo need to write to file.

            mList.add(String.valueOf(m));
        }

        return mList;
    }

    public List<String> generateRelations(){
        for(int n=0;n<3;n++){               // possibleMobileMap
            for(int m=0;m<4;m++){
                List<String> pmList = new ArrayList<String>();
                if(possibleMobileMap.containsKey(String.valueOf(possibleMobiles[n][m]))){
                    pmList = possibleMobileMap.get(String.valueOf(possibleMobiles[n][m]));
                }
                for(int i =0;i<4;i++){
                    if(m!=i){
                        String mb = String.valueOf(possibleMobiles[n][i]);
                        pmList.add(mb);
                    }
                }
                possibleMobileMap.put(String.valueOf(possibleMobiles[n][m]),pmList);
            }
        }

        for(int n=0;n<3;n++){           // possibleMobiles
            for(int m=0;m<4;m++){
                List<String> nameList = new ArrayList<String>();
                if(possibleNameMap.containsKey(String.valueOf(possibleMobiles[n][m]))){
                    nameList = possibleNameMap.get(String.valueOf(possibleMobiles[n][m]));
                }

                Random random = new Random();
                for(int i =0;i<3;i++){
                    nameList.add(String.valueOf(nameArray[n][random.nextInt(4)]));
                }
                possibleNameMap.put(String.valueOf(possibleMobiles[n][m]),nameList);
            }
        }

        Random random = new Random();
        long raw =  15600009900L;
        List<String> toList = new ArrayList<String>();
        for(String m:possibleNameMap.keySet()){
            for(int i=0;i<relationshipCountPerNode;i++){         // 产生从该节点出发的关系
                StringBuffer sb = new StringBuffer();
                List<String> nList = possibleNameMap.get(m);
                String mb = String.valueOf(raw + random.nextInt(10000));
                String rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(m));
                sb.append(rowkey + "-" + mb + " ## ");
                sb.append(tonameArray[random.nextInt(5)][random.nextInt(5)]);
                sb.append(" | ");
                sb.append(String.valueOf(m));
                relationList.add(sb.toString());

                for(int n=0;n<3;n++){          //   产生从toMobile开始的关系,建立与其它可能的手机号码之间的联系
                    StringBuffer s = new StringBuffer();
                    String tophone = possibleMobileMap.get(m).get(random.nextInt(3));
                    String name = possibleNameMap.get(tophone).get(random.nextInt(3));
                    String rowkey1 = MD5Hash.getMD5AsHex(Bytes.toBytes(mb));
                    s.append(rowkey1 + "-" + tophone + " ## ");
                    s.append(name);
                    s.append(" | ");
                    s.append(String.valueOf(mb));
                    sb.append("|");
                    relationList.add(s.toString());
                }
                for(int n=0;n<relationshipCountPerNode;n++){          //   产生从toMobile开始的关系
                    StringBuffer s = new StringBuffer();
                    String tophone = String.valueOf(raw + random.nextInt(100000));;
                    String name = tonameArray[random.nextInt(5)][random.nextInt(5)];
                    String rowkey1 = MD5Hash.getMD5AsHex(Bytes.toBytes(mb));
                    s.append(rowkey1 + "-" + tophone + " ## ");
                    s.append(name);
                    s.append(" | ");
                    s.append(String.valueOf(mb));
                    sb.append("|");
                    relationList.add(s.toString());
                }
                //toList.add(mb);
            }
        }

        return relationList;
    }

    public boolean savaRelationshipDataToFile(String path, String filename){
        String filePath = null;
        if(path.endsWith("/")){
           filePath = path  + filename + ".txt";
        }else{
            filePath = path  + "/" +filename + ".txt";
        }

        File file = new File(filePath);     //文件路径（路径+文件名）
        if (!file.exists()) {   //文件不存在则创建文件，先创建目录
            File dir = new File(file.getParent());
            dir.mkdirs();
            try {
                file.createNewFile();
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        FileWriter fw = null;
        try {
            fw = new FileWriter(file);
            for(String line:relationList){
                fw.write(line);
                fw.write("\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fw.close();
                System.out.println("Write  " + String.valueOf(relationList.size()) +" item relationship data to " + filePath + " done!");
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return false;
        }
    }

    /*public static void main(String[] args){
        *//*for (Map.Entry<String, List<String>> entry : possibleMobileMap.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue().toString());
        }

        for (Map.Entry<String, List<String>> entry : possibleNameMap.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue().toString());
        }*//*
        ConnectedData cdata = new ConnectedData();
        cdata.setRelationshipCountPerNode(200);
        cdata.generateRelations();
        cdata.savaRelationshipDataToFile(Configurations.getDataPath() + "edge/","complexrelationship");
        *//*for(String r:cdata.generateRelations()){
            System.out.println(r);
        }*//*
    }*/

}

package xyz.macor.graph.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.macor.graph.data.ConnectedData;
import xyz.macor.graph.data.DataGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class DataJobEntrance {
    private static Logger logger = LoggerFactory.getLogger(DataJobEntrance.class);
    private static Connection conn = null ;  //HBaseConnection.getConnection();

    public static List<String> qurryTableBatch(List<String> rowkeyList) throws IOException {
        List<Get> getList = new ArrayList();
        List<String> resultList = new ArrayList<String>();
        String tableName = Configurations.getEdgeTableName();
        if(null == conn){
            conn = HBaseConnection.getConnection();
        }

        Table table = conn.getTable(TableName.valueOf(tableName));// 获取表

        for (String rowkey : rowkeyList){           //把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }

        Result[] results = table.get(getList);          //批量查询getList<Get>
        for (Result result : results){                  //对返回的结果集进行操作
            //String r = Bytes.toString(result.getValue(Bytes.toBytes(Configurations.getEdgeCFName()),
             //       Bytes.toBytes(Configurations.getEdgeColumnName())));

            //resultList.add(r);
            String rowkey = Bytes.toString(result.getRow());

            for (Cell cell : result.rawCells()) {  // Or result.listCells();

                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                resultList.add(value);
            }
        }
        return resultList;
    }

    public static List<String> getDataBymobile(String tablename, String cf, String columnname,String mobile) {
        logger.info(String.format("Start scan table[%s] by column[%s:%s]", tablename,cf,columnname));
        //column就是代表frommobile：
        List<String> resultList = new ArrayList<String>();
        try{
            String tableName = tablename;//Configurations.getEdgeTableName();
            if(null == conn){
                conn = HBaseConnection.getConnection();
            }
            Table table = conn.getTable(TableName.valueOf(tableName));// 获取表

            try {
                byte[] columnB = Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(mobile)));

                Scan scan = new Scan(columnB);
                //String family =  cf;           //configProperties.getEdgeFamily();//info 列簇得意思
                scan.addFamily(Bytes.toBytes(cf));
                scan.setCaching(200);//这里批处理设置得是二百 设置高了防止宕机
                scan.setFilter(new PrefixFilter(columnB));
                scan.setCacheBlocks(false);

                ResultScanner results = table.getScanner(scan);

                Iterator<Result> iterator = results.iterator();
                while (iterator.hasNext()) {
                    Result result = iterator.next();
                    //userList.add(constructList(result));
                    String rowKey= Bytes.toString(result.getRow()).replaceAll(" ","");
                    byte[] valueByte = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(columnname));
                    String value = Bytes.toString(valueByte);
                    System.out.println(rowKey + "  :  " + value);
                    resultList.add(rowKey + "  :  " + value);
                }

            } catch (Exception e) {
                String errorMsg = String.format("Error when find by %s", mobile);
                logger.warn(errorMsg, e);
                throw new RuntimeException(errorMsg, e);
            } finally {
                try {
                    if (table != null) {
                        table.close();
                    }
                } catch (Exception e) {
                    logger.warn(String.format("Error when close table %s", tableName));
                }
            }

            logger.info(String.format("End scan [%s] by [%s]  ", tableName,mobile));
        }catch (Exception e){
            logger.error("Error in scan table." ,e.getMessage());
        }

        return resultList;
    }

    public static boolean put(String tableName, String columnFamily, String qualifier, String rowkey, String value) {
        try {
            if(null == conn){
                conn = HBaseConnection.getConnection();
            }
            HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowkey.getBytes());

            put.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            //System.out.println("put successfully！ " + rowkey + "," + columnFamily + "," + qualifier + "," + value);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean batchPut(String tablename, String cf, String column, Map<String,String> datamap) {
        try{
            if(null == conn){
                conn = HBaseConnection.getConnection();
            }
            HTable table = (HTable) conn.getTable(TableName.valueOf(tablename));

            List<Put> puts = new ArrayList<Put>(10000);
            Iterator<Entry<String, String>> entries = datamap.entrySet().iterator();
            int i = 0;

            while (entries.hasNext()) {
                Map.Entry<String, String> entry = entries.next();

                Put put = new Put(Bytes.toBytes(entry.getKey()));
                put.addImmutable(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(entry.getValue()));
                puts.add(put);
                i = i + 1;

                if (i % 10000 == 0 || i == datamap.size()) {
                    table.put(puts);
                    puts = new ArrayList<Put>(10000);
                }

            }

            return true;
        }catch (Exception e){
            logger.error(String.format("Batch put data into table %s failed! %s",tablename,e.getMessage()));
            e.printStackTrace();
            return false;
        }
    }

    public static List<String> listDir(String path){
        List<String> filelist = new ArrayList<String>();
        File p = new File(path);
        String pt = null;

        if(!p.exists()){
            logger.error("%s does not exists.",path);
            throw new RuntimeException(path + " does not exists.");
        }
        if(!path.endsWith("/")){
            pt = path +"/";
        }else{
            pt = path;
        }

        for(File f:p.listFiles()){
            if(!f.isDirectory()){
                filelist.add(pt + f.getName());
            }
        }

        return filelist;
    }

    public static void loadDataToHbase(String path, String tablename, String cf, String column){
        try{
            if(null == conn){
                conn = HBaseConnection.getConnection();
            }
            for(String f:listDir(path)){
                System.out.println(String.format("Begin load %s to hbase.",f));
                Map<String ,String> datamap = new HashMap<String, String>();
                FileReader reader = new FileReader(f);
                BufferedReader br = new BufferedReader(reader);
                int c = 0;

                String str = null;
                //String e = null;
                while((str = br.readLine()) != null) {
                    String[] kv = str.split("##");
                    if(kv.length >= 2){
                        // rowkey 有很多重复的
                        datamap.put(kv[0].trim(),kv[1].trim());
                    }
//                    }else{
//                        e = str;
//                    }
                    c = c +1;
                }
                //System.out.println(String.format("There are %s lines in %s",String.valueOf(c),f));
                logger.info(String.format("There are %s lines in %s",String.valueOf(c),f));

               boolean succeed = batchPut(tablename,cf,column,datamap);
                if(succeed){
                    //System.out.println(String.format("Load %s item data from %s to %s succeed.",String.valueOf(datamap.size()),f,tablename));
                    logger.info(String.format("Load %s item data from %s to %s succeed.",String.valueOf(datamap.size()),f,tablename));
                }else{
                    //System.out.println(String.format("Load data from %s to %s failed.",f,tablename));
                    logger.info(String.format("Load data from %s to %s failed.",f,tablename));
                }

                br.close();
                reader.close();
            }
        }catch (Exception e){
            e.printStackTrace();
            logger.error("Load data from %s to Hbase failed.",path);
        }
    }

    public static void main(String[] args){
        String nodepath = Configurations.getDataPath() + "node/";
        String nodetablename = Configurations.getNodeTableName();
        String nodecf = Configurations.getNodeCFName();
        String nodecolumn = Configurations.getNodeColumnName();

        String edgepath = Configurations.getDataPath() + "edge/";
        String edgetablename = Configurations.getEdgeTableName();
        String edgecf = Configurations.getEdgeCFName();
        String edgecolumn = Configurations.getEdgeColumnName();

        String jobType = "gennodedata";  // loadnodedata , genedgedata, loadedgedata
        if(args.length >0){
            jobType = args[0];
        }

        if(jobType.toLowerCase().contains("gennodedata")){
            DataGenerator nodeGenerator = new DataGenerator(nodepath,Configurations.getNodeAllCount());
            String r1 = nodeGenerator.generateNodeData();
            System.out.println(r1);
        }

        if(jobType.toLowerCase().contains("loadnodedata")){
            loadDataToHbase(nodepath,nodetablename,nodecf,nodecolumn);
        }

        if(jobType.toLowerCase().contains("genedgedata")){
            DataGenerator edgeGenerator = new DataGenerator(edgepath,Configurations.getEdgeAllCount());

            String r2 = edgeGenerator.generateEdgeData();
            System.out.println(r2);

            System.out.println("Generate connected edge data...");
            int edgecountpernode = Configurations.getEdgeCountPerNode();
            ConnectedData cdata = new ConnectedData();
            cdata.setRelationshipCountPerNode(edgecountpernode);
            cdata.generateRelations();
            cdata.savaRelationshipDataToFile(edgepath,"complexrelationship");
        }

        if(jobType.toLowerCase().contains("loadedgedata")){
            //loadDataToHbase(edgepath,edgetablename,edgecf,edgecolumn);
        }

        if(jobType.toLowerCase().contains("scan")){
            //loadDataToHbase(edgepath,edgetablename,edgecf,edgecolumn);
            String tablename = args[1];
            String cf = args[2];
            String columnname = args[3];
            String mobile = args[4];
            getDataBymobile(tablename,cf, columnname,mobile);
        }

//        int edgeCount = 10000;
//
//        String s1 = generateNodeData(nodeCount);
//        System.out.println(s1);
//
//        String s2 = generateEdgeData(edgeCount);
//        System.out.println(s2);

        //byte[] rowKeyB = Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes("15618387902")));
        //System.out.println(rowKeyB.toString());
        //System.out.println("MD5Hash.getMD5AsHex(Bytes.toBytes(\"15618387902\"))"+MD5Hash.getMD5AsHex(Bytes.toBytes("15618387902")));

        //loadDataToHbase(nodepath,nodetablename,nodecf,nodecolumn);

        if(null != conn){
            HBaseConnection.close();
        }
    }
}

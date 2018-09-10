package xyz.macor.graph.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Configurations implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(Configurations.class);
    private static Properties properties = new Properties();

    static {
        try {
            //InputStream in = new BufferedInputStream(new FileInputStream("classpath:config.properties"));
            //InputStream in = new BufferedInputStream(new FileInputStream("classpath:config.properties"));
            List<String> lines = loadConf("classpath:/config.properties");
            //List<String> lines = loadConf("/Users/daiyongtao/Code/graph-tests/src/main/resources/config.properties");


            for(String line:lines){
                if(null != line && line.length() >=3 && !line.startsWith("#") && line.contains("=")){
                    String[] kv = line.split("=");
                    properties.setProperty(kv[0].trim(),kv[1].trim());
                }
            }
            //properties.load(in);
        } catch (Exception e) {
            String errorMsg = String.format("Failed load property file [%s]","config.properties");
            logger.error(errorMsg);
            e.printStackTrace();
            throw new RuntimeException(errorMsg,e);
        }
    }

    public static List<String> loadConf(String path) throws Exception {
        BufferedReader reader = null;
        if (path.startsWith("classpath:")) {
            String parsedPath = path.replace("classpath:", "");
            InputStream is = Configurations.class.getResourceAsStream(parsedPath);
            InputStreamReader isr = new InputStreamReader(is, "utf-8");
            reader = new BufferedReader(isr);
        }else {
            reader = new BufferedReader(new FileReader(new File(path)));
        }
        List<String> lines = new ArrayList<String>();
        String line = reader.readLine();
        while (line != null) {
            lines.add(line);
            line = reader.readLine();
        }
        return lines;
    }

    public static String getQuorum(){
        return properties.getProperty("hbase.zookeeper.quorum","localhost");
    }

    public static String getZKPort(){
        return properties.getProperty("hbase.zookeeper.property.clientPort","2181");
    }

    public static int getRpcTimeout(){
        return Integer.valueOf(properties.getProperty("hbase.rpc.timeout","300000"));
    }

    public static int getClietTimeout(){
        return Integer.valueOf(properties.getProperty("hbase.client.operation.timeout","400000"));
    }

    public static int getScannerTimeout(){
        return Integer.valueOf(properties.getProperty("hbase.client.scanner.timeout.period","600000"));
    }

    public static String getNodeTableName(){
        return properties.getProperty("hbase.table.name.node","node");
    }

    public static String getNodeCFName(){
        return properties.getProperty("hbase.table.name.node.family","info");
    }

    public static String getNodeColumnName(){
        return properties.getProperty("hbase.table.name.node.columnname","c1");
    }

    public static String getEdgeTableName(){
        return properties.getProperty("hbase.table.name.edge","edge");
    }

    public static String getEdgeCFName(){
        return properties.getProperty("hbase.table.name.edge.family","info");
    }

    public static String getEdgeColumnName(){
        return properties.getProperty("hbase.table.name.edge.columnname","c1");
    }

    public static String getScanCache(){
        return properties.getProperty("hbase.table.scan.cache","1000");
    }

    public static String getDataPath(){
        //path + "/node/"
        String datadir = properties.getProperty("data.dir","/tmp/");
        if(datadir.endsWith("/"))
            return datadir;
        else
            return datadir + "/";
    }

    public static int getEdgeCountPerNode(){
        return Integer.valueOf(properties.getProperty("data.edge.countpernode","200"));
    }

    public static int getNodeAllCount(){
        return Integer.valueOf(properties.getProperty("data.node.allcount","10000000"));
    }

    public static int getEdgeAllCount(){
        return Integer.valueOf(properties.getProperty("data.edge.allcount","100000000"));
    }

}

package xyz.macor.graph.hbase;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.xerces.util.SynchronizedSymbolTable;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.io.Serializable;
import java.util.Properties;

public class HBaseConnection implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(HBaseConnection.class);

    private static Connection connection;
    private static Properties properties = new Properties();
    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum",Configurations.getQuorum());
            configuration.set("hbase.zookeeper.property.clientPort", Configurations.getZKPort());
            configuration.setInt("hbase.rpc.timeout", Configurations.getRpcTimeout());
            configuration.setInt("hbase.client.operation.timeout", Configurations.getClietTimeout());
            configuration.setInt("hbase.client.scanner.timeout.period", Configurations.getScannerTimeout());
            configuration.set("hbase.client.keyvalue.maxsize", "1048576000"); //1G

            connection = ConnectionFactory.createConnection(configuration);
            if(connection != null){
                System.out.println("Connect to Hbase succeed.");
            }
        } catch (Exception e) {
            String errorMsg = "Failed create hbase connection.";
            logger.error(errorMsg);
            e.printStackTrace();
            throw new RuntimeException(errorMsg,e);
        }
    }

    public static Connection getConnection() {
        return connection;
    }

    public static void close(){
        if(connection != null){
            try {
                connection.close();
                logger.info("Close Hbase connection succeed.");
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static byte[] getValueFromResult(Result result, String family, String column) {
        return result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
    }
}

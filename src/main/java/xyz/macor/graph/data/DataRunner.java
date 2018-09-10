package xyz.macor.graph.data;

import static xyz.macor.graph.data.DataType.Edge;
import static xyz.macor.graph.data.DataType.Node;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

public class DataRunner implements Callable<String> {
    final private static String[][] nameArray = {{"赫卡忒","Hecate","魔法的女神","幽灵的女神","夜之女神"},
                                            {"该亚","Gaea","大地女神","尤拉诺斯之母","创造了大地"},
                                            {"尤拉诺斯","Uranus","天之神","第一任神王","该亚的长子"},
                                            {"普罗米修斯","Prometheus","先知者","人类的创造者","泰坦爱泼特斯之子"},
                                            {"厄毗米修斯","Epimetheus","后知者","潘多拉的丈夫","普罗米修斯的兄弟"}};
    final private static String[][] edgeNameArray = {{"赫卡忒","赫卡忒Hecate","女神赫卡忒","幽灵女神赫卡忒","赫卡忒Hecate女神"},
            {"该亚","该亚Gaea","该亚大地女神","Gaea该亚","大地女神该亚"},
            {"尤拉诺斯","尤拉诺斯Uranus","天之神尤拉诺斯","神王尤拉诺斯","尤拉诺斯神"},
            {"普罗米修斯","普罗米修斯Prometheus","先知者普罗米修斯","创造者普罗米修斯","普罗米修斯"},
            {"厄毗米修斯","厄毗米修斯Epimetheus","后知者厄毗米修斯","厄毗米修斯2","厄毗米修斯1"}};
    private int startIndex;
    private int endIndex;
    private String name;
    private String path;
    private DataType dataType = Edge;

    public DataRunner(String name, int startIndex, int endIndex, String path, DataType datatype) {
        this.name = name;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.path = path;
        this.dataType = datatype;
    }

    private String genetePredictName(int ind){
        Random random = new Random();
        int len = random.nextInt(5 ) + 1;
        StringBuffer sb = new StringBuffer();
        //sb.append("[");
        for(int i=0; i < len; i++){
            sb.append(nameArray[ind][i]);
            sb.append(":" + (50 - i - 2*ind));

            if(i < len - 1){
                sb.append(";");
            }
        }
        //sb.append("]");

        return sb.toString();
    }

    public String call(){
        String ret = null;
        switch (dataType){
            case Edge:
                ret = generateEdgeData();
                break;
            case Node:
                ret = generateNodeData();
                break;
            default:
                ret = generateEdgeData();
        }

        return ret;
    }

    private String generateName(int i){
        int m = i % 5;
        Random random = new Random();
        int ind = random.nextInt(5);

        return nameArray[m][ind];
    }

    private String generateNodeData(){
        // USR_NAME_PREDICT
        // put 'node','52c38e1164336eb4657295d319e0eb65','info:family','name->张三|mobile->13222085838|isCustom->1|userId->123|SenCod->3|appellation->张三 12;张总 8;上海张三 7'
        String filePath = path  + name + ".txt";

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

        /**
         CLT_NBR(客户号)
         CLT_MOB_TEL1(主手机号)
         CLT_CIR_FLG(流通标示)
         CLT_AGE(年龄)
         CLT_NAM(姓名)
         SEN_COD(敏感客户标示)
         CLT_EDU(学历)
         CLT_SEX(性别)
         CLT_MAR_STS(婚姻)
         CLT_CC_COD(CC职业等级)
         CLT_ADR_ID(主地址标示)
         CLT_STR_DTE(成为会员日期)
         USR_NAM_PREDICT(名字统计)
         */

        // 'CLT_NAM -> 张三 | CLT_MOB_TEL1 -> 13222085838 | CLT_AGE -> 28 | isCustom -> 1 | userId -> 123 |
        // SEN_COD -> 3 | USR_NAM_PREDICT -> "name1 10;name2 8;name3 6"
        FileWriter fw = null;
        long[] raw_phone_list = {13183080000l,13873980000l,15618200000l,17128350900l,18873450000l,15103450000l};

        try {
            fw = new FileWriter(file);

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            String date = format.format(new Date());
            Random random = new Random();
            //Map<String,String> dataMap = new HashMap<String, String>();
            for (int i = startIndex; i < endIndex; i++) {
                StringBuffer sb = new StringBuffer();
                //long mb = 15600000000L + i;
                long mb = raw_phone_list[random.nextInt(raw_phone_list.length)] + random.nextInt(1000000);

                //byte[] rowKey = Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(mb)));
                String rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes(mb));

                sb.append(rowKey);
                sb.append(" ## ");

                sb.append("CLT_NAM -> ");
                sb.append(generateName(i));
                sb.append(" | ");

                sb.append("CLT_MOB_TEL1 -> ");
                sb.append(mb);
                sb.append(" | ");

                sb.append("CLT_AGE -> ");
                sb.append(random.nextInt(50));
                sb.append(" | ");

               /* sb.append("isCustom -> ");
                sb.append(random.nextInt(2));
                sb.append(" | ");
                //CLT_NBR为空表示非招行客户
                String isCustom;
                if (StringUtils.isBlank(userId)) {
                    isCustom = "0";
                } else {
                    isCustom = "1";
                }
                */

                sb.append("CLT_STR_DTE -> ");
                sb.append(date);
                sb.append(" | ");

                sb.append("CLT_NBR -> ");
                sb.append(i);
                sb.append(" | ");

                sb.append("SEN_COD -> ");
                sb.append(random.nextInt(8));
                sb.append(" | ");

                sb.append("USR_NAM_PREDICT -> ");
                sb.append(genetePredictName(random.nextInt(5)));
                sb.append("|");
                sb.append("\n");

                fw.write(sb.toString());
            }
            //fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println(filePath + " done!");
        return name + " done!";
    }

    private String generateEdgeData(){
        // row-key:  md5(fromMobile.toByte)-13618279035
        // column:  info:detail   name | toMobile
        // put 'edge','52c38e1164336eb4657295d319e0eb65-13222085848','info:mixB','李四|13222085838'
        // put 'edge','52c38e1164336eb4657295d319e0eb65-13222085849','info:mixB','王二|13222085838'
        // put 'edge','52c38e1164336eb4657295d319e0eb65-13222085850','info:mixB','小明|13222085838'
        // put 'edge','d5b8e2edaa3e5f6c329524933ac37aa0-13222085850','info:mixB','小明|13222085850'
        String filePath = path  + name + ".txt";
        long[] raw_phone_list = {13183080000l,13873980000l,15618200000l,17128350900l,18873450000l,15103450000l};
        final int edgeCount = 240;

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

            Random random = new Random();
            //Map<String,String> dataMap = new HashMap<String, String>();
            for (int i = startIndex; i < endIndex; i++) {
                //long fm = 15600000000L + i;
                long fm = raw_phone_list[random.nextInt(raw_phone_list.length)] + i;

                for(int j=0;j<edgeCount;j++){
                    //long tm = 13873980000L + random.nextInt(100000);
                    long tm = raw_phone_list[random.nextInt(raw_phone_list.length)] + random.nextInt(1000000);

                    StringBuffer sb = new StringBuffer();

                    String rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fm));
                    sb.append(rowKey + "-" + tm + " ## ");

                    sb.append(generateEdgeName(random.nextInt(5)));
                    sb.append(" | ");
                    sb.append(fm);
                    sb.append("|");
                    sb.append("\n");

                    fw.write(sb.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println(filePath + " done!");
        return name + " done!";

    }

    private String generateEdgeName(int i){
        int m = i % 5;
        Random random = new Random();
        int ind = random.nextInt(5);

        return edgeNameArray[m][ind];
    }

    public static String geneteStringNew(List<String> strs,String sep) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strs.size(); i++) {
            sb.append(strs.get(i));
            if (i != strs.size() - 1) {
                sb.append(sep);
            }
        }
        sb.append("\n");
        return sb.toString();
    }

}

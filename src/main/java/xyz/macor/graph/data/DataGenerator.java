package xyz.macor.graph.data;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DataGenerator {

    private String path;
    private int allCount;

    public DataGenerator(String path, int allCount) {
        this.path = path;
        this.allCount = allCount;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setAllCount(int allCount) {
        this.allCount = allCount;
    }

    public String getPath() {
        return path;
    }

    public int getAllCount() {
        return allCount;
    }

    /*public static void main(String[] args){
//        int nodeCount = 1000000;
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

    }*/

    public String generateNodeData(){
        //String path = "/Users/daiyongtao/Desktop/cmb-graph/testdata";

        final int threadNum = 20;
        final int COUNT_PER = allCount / threadNum;
        final int COUNT_LAST = COUNT_PER + allCount % threadNum;

        //2000万行内节点数据生成：200个线程：每一个生成10w条记录。
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        List<Future<String>> resultList = new ArrayList<Future<String>>();

        for (int i = 0; i < threadNum; i++) {
            DataRunner runnable = null;
            if(i < threadNum -1){
                runnable = new DataRunner("file" + i, i * COUNT_PER, (i + 1) * COUNT_PER ,path, DataType.Node);
            }else{
                runnable = new DataRunner("file" + i, i * COUNT_PER, i * COUNT_PER + COUNT_LAST ,path, DataType.Node);
            }
            Future<String> future = cachedThreadPool.submit(runnable);
            resultList.add(future);
        }

        Assert.assertEquals(resultList.size(), threadNum);

        if (cachedThreadPool.isTerminated()) {
            cachedThreadPool.shutdownNow();
            //cachedThreadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
        //return "Succeed generate node data.";
        return path + " done.";
    }

    public String generateEdgeData(){
//        String path = "/Users/daiyongtao/Desktop/cmb-graph/testdata";

        final int threadNum = 20;
        final int COUNT_PER = allCount / threadNum;
        final int COUNT_LAST = COUNT_PER + allCount % threadNum;

        //2000万行内节点数据生成：200个线程：每一个生成10w条记录。
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        List<Future<String>> resultList = new ArrayList<Future<String>>();

        for (int i = 0; i < threadNum; i++) {
            DataRunner runnable = null;
            if(i < threadNum -1){
                runnable = new DataRunner("file" + i, i * COUNT_PER, (i + 1) * COUNT_PER ,path, DataType.Edge);
            }else{
                runnable = new DataRunner("file" + i, i * COUNT_PER, i * COUNT_PER + COUNT_LAST ,path, DataType.Edge);
            }
            Future<String> future = cachedThreadPool.submit(runnable);
            resultList.add(future);
        }

        Assert.assertEquals(resultList.size(), threadNum);

        if (cachedThreadPool.isTerminated()) {
            cachedThreadPool.shutdownNow();
        }
        //return "Succeed generate edge data.";
        return path + " done";
    }
}

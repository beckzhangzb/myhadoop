import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * 测试实例
 * @author zhangzb
 * @since 2019/1/29 17:03
 */
public class HbaseTest {

    /**
     * 配置ss
     */
    private static Configuration config     = null;
    private   static      Connection    connection = null;
    private   static    Table         table      = null;

    /**
     * 前初始化
     * @throws Exception
     */
    public static void init() throws Exception {
        //如果配置了环境变量就可以省略之
        System.setProperty("hadoop.home.dir", "D:\\ins_soft\\works\\hadoop-2.7.7");
        config = HBaseConfiguration.create();
        //与 hbase-site-xml里面的配置信息 zookeeper.znode.parent 一致
        config.set("zookeeper.znode.parent", "/hbase");
        config.set("hbase.zookeeper.quorum", "192.168.67.128");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf("test"));
    }

    public static void main(String[] args) throws Exception {
        init();
        HbaseTest hbaseTest = new HbaseTest();
        hbaseTest.scanDataStep1();
        hbaseTest.close();
    }

    /**
     * 创建数据库表dept，并增加列族info和subdept
     *
     * @throws Exception
     */
    public void createTable() throws Exception {
        // 创建表管理类
        System.out.println("创建表成功！");
    }

    /**
     * 向hbase中插入前三行网络部、开发部、测试部的相关数据，
     * 即加入表中的前三条数据
     *
     * @throws Exception
     */
    @SuppressWarnings({"deprecation", "resource"})
    public void insertData() throws Exception {
        /*table.setAutoFlushTo(false);
        table.setWriteBufferSize(534534534);
        ArrayList<Put> arrayList = new ArrayList<Put>();

        Put put = new Put(Bytes.toBytes("0_1"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("网络部"));
        put.add(Bytes.toBytes("subdept"), Bytes.toBytes("subdept1"), Bytes.toBytes("1_1"));
        put.add(Bytes.toBytes("subdept"), Bytes.toBytes("subdept2"), Bytes.toBytes("1_2"));
        arrayList.add(put);

        Put put1 = new Put(Bytes.toBytes("1_1"));
        put1.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("开发部"));
        put1.add(Bytes.toBytes("info"), Bytes.toBytes("f_pid"), Bytes.toBytes("0_1"));

        Put put2 = new Put(Bytes.toBytes("1_2"));
        put2.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("测试部"));
        put2.add(Bytes.toBytes("info"), Bytes.toBytes("f_pid"), Bytes.toBytes("0_1"));

        for (int i = 1; i <= 100; i++) {

            put1.add(Bytes.toBytes("subdept"), Bytes.toBytes("subdept" + i), Bytes.toBytes("2_" + i));
            put2.add(Bytes.toBytes("subdept"), Bytes.toBytes("subdept" + i), Bytes.toBytes("3_" + i));
        }
        arrayList.add(put1);
        arrayList.add(put2);
        //插入数据
        table.put(arrayList);
        //提交
        table.flushCommits();*/
        System.out.println("数据插入成功！");
    }

    /**
     * 查询所有一级部门(没有上级部门的部门)
     * @throws Exception
     */
    public void scanDataStep1() throws Exception {

        // 创建全表扫描的scan
        Scan scan = new Scan();
        System.out.println("查询到的所有如下：");
        // 打印结果集
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println("cell=>" + cell);
                System.out.println("value=>" + Bytes.toString(cell.getValueArray()));
            }
        }
    }

    /**
     * 已知rowkey，查询该部门的所有(直接)子部门信息 rowkey=1_1
     * @throws Exception
     */
    public void scanDataStep2() throws Exception {
       /* Get g = new Get("1_1".getBytes());
        g.addFamily("subdept".getBytes());
        // 打印结果集
        Result result = table.get(g);
        for (KeyValue kv : result.raw()) {
            Get g1 = new Get(kv.getValue());
            Result result1 = table.get(g1);
            for (KeyValue kv1 : result1.raw()) {
                System.out.print(new String(kv1.getRow()) + " ");
                System.out.print(new String(kv1.getFamily()) + ":");
                System.out.print(new String(kv1.getQualifier()) + " = ");
                System.out.print(new String(kv1.getValue()));
                System.out.print(" timestamp = " + kv1.getTimestamp() + "\n");
            }
        }*/
    }

    /**
     * 已知rowkey，向该部门增加一个子部门
     * rowkey:0_1
     * 增加的部门名：我增加的部门
     * @throws Exception
     */
    public void scanDataStep3() throws Exception {
        /*//新增一个部门
        Put put = new Put(Bytes.toBytes("4_1"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("我增加的部门"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("f_pid"), Bytes.toBytes("0_1"));
        //插入数据
        table.put(put);
        //提交
        table.flushCommits();

        //更新网络部
        Put put1 = new Put(Bytes.toBytes("0_1"));
        put1.add(Bytes.toBytes("subdept"), Bytes.toBytes("subdept3"), Bytes.toBytes("4_1"));
        //插入数据
        table.put(put1);
        //提交
        table.flushCommits();*/
    }

    public void close() throws Exception {
        table.close();
        connection.close();
    }

}
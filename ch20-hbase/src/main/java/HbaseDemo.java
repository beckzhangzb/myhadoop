import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试实例
 * @author zhangzb
 * @since 2019/1/29 17:03
 */
public class HbaseDemo {

    /**
     * 配置ss
     */
    private static Configuration config     = null;
    private static Connection    connection = null;
    private static Table         table      = null;

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

    /**
     * 执行主方法
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        init();
        HbaseDemo hbaseDemo = new HbaseDemo();
        createSingleColumnTable("demo", "user");
        createMultiColumnTable("mtable", new String[] {"user", "address"});
        hbaseDemo.scanDataStep1();
        hbaseDemo.close();
    }

    /**
     * 创建只有一个列簇的表
     * @throws IOException
     */

    public static void createSingleColumnTable(String tabName, String columnFamily) throws IOException {
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tabName))) {
            TableName tableName = TableName.valueOf(tabName);
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述起构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.
                    newBuilder(Bytes.toBytes(columnFamily));
            //获得列描述起
            ColumnFamilyDescriptor cfd = cdb.build();
            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            //admin.addColumnFamily(tableName, cfd); //给表添加列族
            admin.createTable(td);
            System.out.println("建表完成, 开始初始化部分数据");
            insertSingle(tabName, columnFamily);
        } else {
            System.out.println("表已存在");
        }
        //关闭链接
    }

    /**
     * 创建表（包含多个列簇）
     * @tableName 表名
     *
     * @family 列族列表
     */
    public static void createMultiColumnTable(String tabName, String[] columnFamilys) throws IOException {
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tabName))) {
            TableName tableName = TableName.valueOf(tabName);
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述器构造器
            ColumnFamilyDescriptorBuilder cdb;
            //获得列描述器
            ColumnFamilyDescriptor cfd;
            for (String columnFamily : columnFamilys) {
                cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                cfd = cdb.build();
                //添加列族
                tdb.setColumnFamily(cfd);
            }
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
            System.out.println("建表成功！");
        } else {
            System.out.println("表已存在！");
        }
        //关闭链接
    }

    /**
     * 添加数据（一个rowKey,一个列簇）
     * @throws IOException
     */
    public static void insertSingle(String tabName, String columnFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));

        Put put = new Put(Bytes.toBytes("rowKey1"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("age"), Bytes.toBytes("22"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("weight"), Bytes.toBytes("88kg"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("sex"), Bytes.toBytes("男"));

        table.put(put);
        table.close();
    }

    /**
     * 添加数据（多个rowKey，多个列簇）
     * @throws IOException
     */
    public static void insertMany(String tabName, String[] columnFamilys) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        List<Put> puts = new ArrayList<Put>();

        for (int i = 0; i < columnFamilys.length; i++) {
            Put put = new Put(Bytes.toBytes("rowKey" + (i + 1)));
            put.addColumn(Bytes.toBytes(columnFamilys[i]), Bytes.toBytes("name" + i), Bytes.toBytes("value" + i));
            put.addColumn(Bytes.toBytes(columnFamilys[i]), Bytes.toBytes("name" + (i + 1)),
                    Bytes.toBytes("value" + (i + 1)));
            puts.add(put);
        }

        table.put(puts);
        table.close();
    }

    /**
     * 根据RowKey，列簇，列名修改值
     * @param tabName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param columnValue
     * @throws IOException
     */
    public static void updateData(String tabName, String rowKey, String columnFamily, String columnName,
                                  String columnValue) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        Put put1 = new Put(Bytes.toBytes(rowKey));
        put1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        table.put(put1);
        table.close();
    }

    /**
     * 删除某一行的某一个列簇内容
     * @param tabName
     * @param rowKey
     * @param columnFamily
     * @throws IOException
     */
    public static void deleteData(String tabName, String rowKey, String columnFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
        table.close();
    }

    /**
     *根据rowKey查询数据
     * @throws IOException
     */
    public static void getResult(String tabName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        //获得一行
        Get get = new Get(Bytes.toBytes(rowKey));
        Result set = table.get(get);
        Cell[] cells = set.rawCells();
        for (Cell cell : cells) {
            System.out.println(
                    Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                            + "::" +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        table.close();
    }

    /**
     * 查询表所有记录
     * @throws Exception
     */
    public void scanTableData(String tabName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tabName));
        // 创建全表扫描的scan
        Scan scan = new Scan();
        System.out.println("查询到的所有如下：");
        // 打印结果集
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String rowkey = Bytes.toString(result.getRow());
            System.out.println("result :" + result);
            System.out.println("row key :" + rowkey);
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                                + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                cell.getQualifierLength()) + "::" +
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                System.out.println("value=>" + Bytes.toString(cell.getValueArray()));
            }
        }
    }

    //过滤器 LESS <  LESS_OR_EQUAL <=  EQUAL =   NOT_EQUAL <>  GREATER_OR_EQUAL >=   GREATER >   NO_OP 排除所有

    /**
     * rowKey过滤器
     * @param tabName
     * @throws IOException
     */
    public static void rowkeyFilter(String tabName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(CompareOperator.EQUAL,
                new RegexStringComparator("Key1$")); //str$ 末尾匹配，相当于sql中的 %str  ^str开头匹配，相当于sql中的str%
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                                + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                cell.getQualifierLength()) + "::" +
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 列值过滤器
     * @throws IOException
     */
    public static void singColumnFilter(String tabName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        Scan scan = new Scan();
        //下列参数分别为，列族，列名，比较符号，值
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("author"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, Bytes.toBytes("spark"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                                + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                cell.getQualifierLength()) + "::" +
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 列名前缀过滤器
     * @throws IOException
     */
    public static void columnPrefixFilter(String tabName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                                + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                cell.getQualifierLength()) + "::" +
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 过滤器集合
     * @throws IOException
     */
    public static void filterSet(String tabName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        Scan scan = new Scan();
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("author"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, Bytes.toBytes("spark"));
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("name"));
        list.addFilter(filter1);
        list.addFilter(filter2);

        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                        Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                                + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                cell.getQualifierLength()) + "::" +
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
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

    /**
     * 关闭
     * @throws Exception
     */
    public void close() throws Exception {
        table.close();
        connection.close();
    }

}

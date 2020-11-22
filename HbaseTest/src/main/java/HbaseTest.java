import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.ws.rs.core.Response;
import java.io.IOException;
public class HbaseTest {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
    static {
        configuration = HBaseConfiguration.create();
        //configuration.set("hbase.zookeeper.quorum","localhost");
        //configuration.set("hbase.zookeeper.property.clientPort","2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a new table.
     * @param tableName the name of target table.
     * @param cFamily a set of all column families.
     */
    public static void createTable(String tableName, String[] cFamily) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table exists!");
            return;
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String c : cFamily) {
            tableDescriptor.addFamily(new HColumnDescriptor(c));
        }
        admin.createTable(tableDescriptor);
        System.out.println("create table success!");
    }

    /**
     * Drop a table.
     * @param tableName the target table to be deleted.
     */
    public static void dropTable(String tableName) throws IOException {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + " not exits.");
        }
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("deletable " + tableName + " done.");
    }

    /**
     * To insert a record with current table structure.
     * @param tableName target table.
     * @param rowKey the row num.
     * @param family the column family name.
     * @param column the column under the family.
     * @param value the value tobe added.
     * @throws IOException
     */
    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("Insert record " + rowKey + " to table " + tableName + " done.");
    }

    /**
     * To delete a record( row)
     * @param tableName target table.
     * @param rowKey the row num of the record tobe deleted.
     */
    public static void delete(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    /**
     * Print the specif record according to target row key and the family.
     * @param tableName target table.
     * @param rowKey the row tobe searched.
     * @param family specific family.
     * @throws IOException
     */
    public static void search(String tableName, String rowKey, String family) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Get get = new Get(rowKey.getBytes());
            get.addFamily(family.getBytes());
            Result result = table.get(get);

//            for (KeyValue kv : result) {
//                System.out.println("Return search result: " + kv);
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Add a new column family to target table.
     * @param tableName target table.
     * @param cFamily new column family.
     */
    public static void addCFamily(String tableName, String cFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        admin.disableTable(TableName.valueOf(tableName));
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("cFamily");
        tableDescriptor.addFamily(columnDescriptor);
        admin.addColumn(TableName.valueOf(tableName),columnDescriptor);
        admin.enableTableAsync(TableName.valueOf(tableName));
    }

    /**
     * Return all the record of target table.
     * @param tableName target table.
     */
    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Scan scan = new Scan();
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                System.out.println(result);
            }
            results.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        // Create table and insert record.
        String[] cFamilies = {"Description", "Courses", "Home"};
        System.out.println("Here!");
        createTable("students",cFamilies);
        // Row 1.
        insert("students","1","Description","Name","Li Lei");
        insert("students","1","Description","Height","176");
        insert("students","1","Courses","Chinese","80");
        insert("students","1","Courses","Math","90");
        insert("students","1","Courses","Physics","95");
        insert("students","1","Home","Province","Zhejiang");
        // Row 2.
        insert("students","2","Description","Name","Han Meimei");
        insert("students","2","Description","Height","183");
        insert("students","2","Courses","Chinese","88");
        insert("students","2","Courses","Math","77");
        insert("students","2","Courses","Physics","66");
        insert("students","2","Home","Province","Beijing");
        // Row 3.
        insert("students","3","Description","Name","Xiao Ming");
        insert("students","3","Description","Height","162");
        insert("students","3","Courses","Chinese","90");
        insert("students","3","Courses","Math","90");
        insert("students","3","Courses","Physics","90");
        insert("students","3","Home","Province","Shanghai");

        // scan table.
        System.out.println("====Scan table.====");
        scanTable("students");

        // Search students' home.
        System.out.println("====Search students' home.====");
        search("students","1","Home");
        search("students","2","Home");
        search("students","3","Home");

        // Add new column.
        insert("students","1","Courses","English","95");
        insert("students","2","Courses","English","85");
        insert("students","3","Courses","English","98");
        // Add new column family.
        addCFamily("students","Contact");
        insert("students","1","Contact","Email","lilei@qq.com");
        insert("students","2","Contact","Email","hanmeimei@qq.com");
        insert("students","2","Contact","Email","xiaoming@qq.com");
    }
}

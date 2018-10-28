package ir.milux.metalmarks.core;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DB {
    private static Logger logger = Logger.getRootLogger();
    private static final TableName TABLE_NAME = TableName.valueOf(CONSTANTS.MOB_TABLE_NAME);
    private static final Connection connection = getConnection();

    private static Connection getConnection () {
        Connection connection = null;

        logger.info("hbase.zookeeper.quorum : "+CONSTANTS.HBASE_ZK_QUORUM);
        logger.info("zookeeper.session.timeout : " + CONSTANTS.ZK_TIMEOUT);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", CONSTANTS.HBASE_ZK_QUORUM);
        conf.set("zookeeper.session.timeout", CONSTANTS.ZK_TIMEOUT);

        try {
            connection = ConnectionFactory.createConnection(conf);
            logger.info("hbase connection created.");
        } catch (IOException e) {
            e.printStackTrace();
            logger.info(e.getMessage());
        }
        return connection;
    }

    private static Table getTable () {
        Table table = null;
        try {
            table = connection.getTable(TABLE_NAME);
            logger.info("get table");
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(101);
        }
        return table;
    }

    public static boolean put (MOB mob) {
        Table table = getTable();
        Put p = new Put(Bytes.toBytes(mob.getName()));
        p.addColumn(CONSTANTS.MOB_CF_NAME.getBytes(), CONSTANTS.MOB_QF_NAME.getBytes(), MOB.serialize(mob));

        try {
            table.put(p);
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }
    public static MOB get(String key) throws IOException {
        Table table = getTable();
        Get get = new Get(key.getBytes());

        Result result = table.get(get);
        byte[] bytes = result.getValue(CONSTANTS.MOB_CF_NAME.getBytes(), CONSTANTS.MOB_QF_NAME.getBytes());
        if (bytes == null)
            return null;
        return MOB.deserialize(bytes);
    }


}








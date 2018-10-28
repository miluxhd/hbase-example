package ir.milux.metalmarks.core;

/**
 * Created by milux on 10/27/18.
 */
public class CONSTANTS {
    public static final String INPUT_FILE_LIST = Properties.getProperty("sourcefile");
    public static final String PASSWORD = Properties.getProperty("rabbit.password");
    public static final String USER_NAME = Properties.getProperty("rabbit.username");
    public static final String INPUT_QUEUE_NAME = Properties.getProperty("rabbit.queue.inputaddr") ;
    public static final String OUTPUT_QUEUE_NAME = Properties.getProperty("rabbit.queue.mob");
    public static final String RABBIT_HOST = Properties.getProperty("rabbit.hostname") ;
    public static final String MOB_TABLE_NAME = Properties.getProperty("hbase.mob.tablename");
    public static final String MOB_CF_NAME = Properties.getProperty("hbase.mob.cf");
    public static final String MOB_QF_NAME = Properties.getProperty("hbase.mob.qf") ;
    public static final String HBASE_ZK_QUORUM = Properties.getProperty("hbase.zk.quorum");
    public static final String ZK_TIMEOUT = Properties.getProperty("hbase.zk.timeout");
    public static final int REST_PORT = Integer.parseInt(Properties.getProperty("http.port"));
    public static final String CCH_BASE = Properties.getProperty("fs.cache.path");
    public static final int RABBIT_QOS = Integer.parseInt(Properties.getProperty("rabbit.qos"));
}

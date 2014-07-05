package com.transwarp.titan.hbase;


import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 6/23/14
 * Time  : 12:17 PM
 * Email : xuezy90@gmail.com
 */
public class GraphConfig {

    private Log log = LogFactory.getLog(GraphConfig.class);
    private static final String INDEX_NAME = "search";
    private static final String DEFAULT_PATH = "/home/xue/titanTest";
    public static Configuration create(String dir)
    {
        BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        // configuring local backend
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hbase");
        storage.setProperty("hbase.tablename","titan_transwarp_test");
        storage.setProperty(GraphDatabaseConfiguration.HOSTNAME_KEY,"tw-31,transwarp-tmp2");
        storage.setProperty(GraphDatabaseConfiguration.METRICS_ENABLED,"true");
        storage.setProperty(GraphDatabaseConfiguration.BASIC_METRICS,"true");
        storage.setProperty(GraphDatabaseConfiguration.METRICS_CONSOLE_INTERVAL_KEY,"100");
        storage.setProperty(GraphDatabaseConfiguration.METRICS_CSV_INTERVAL_KEY,"100");
        storage.setProperty(GraphDatabaseConfiguration.METRICS_CSV_DIR_KEY,"/home/xue/titanTest");
//        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY,dir);
//        // configuring elastic search index
//        Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
//        index.setProperty(INDEX_BACKEND_KEY, "elasticsearch");
//        index.setProperty("local-mode", true);
//        index.setProperty("client-only", false);
//        index.setProperty(STORAGE_DIRECTORY_KEY, dir + File.separator + "es");
        return config;
    }

    public static Configuration create()
    {
        return create(DEFAULT_PATH);
    }
}

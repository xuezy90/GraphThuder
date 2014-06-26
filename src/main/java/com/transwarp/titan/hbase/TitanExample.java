package com.transwarp.titan.hbase;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.File;
import java.io.IOException;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;


/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 6/21/14
 * Time  : 11:01 AM
 * Email : xuezy90@gmail.com
 */
public class TitanExample {

    private static Log log = LogFactory.getLog(TitanExample.class);
    public static final String INDEX_NAME = "search";

    public static BaseConfiguration getConfig(String dir)
    {
        BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        // configuring local backend
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hbase");
//        storage.setProperty("storage.tablename","titan_transwarp_test");
        storage.setProperty("storage.hostname","tw-31,transwarp-tmp2");
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY,dir);
        // configuring elastic search index
        Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
        index.setProperty(INDEX_BACKEND_KEY, "elasticsearch");
        index.setProperty("local-mode", true);
        index.setProperty("client-only", false);
        index.setProperty(STORAGE_DIRECTORY_KEY, dir + File.separator + "es");
        return config;
    }

    private static void clearHbase(String titan) throws IOException {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        HBaseAdmin ha = new HBaseAdmin(conf);
        if(ha.tableExists(titan)){
            if(!ha.isTableDisabled(titan))
                ha.disableTable(titan);
            ha.deleteTable(titan);
        }
    }

    private static void clearFolder(String dir) throws IOException {
        for(File f:new File(dir).listFiles())
        {
            if(f.exists()){
                if(f.isFile())
                {
                    f.delete();
                }
                else if(f.isDirectory())
                {
                    clearFolder(f.getAbsolutePath());
                }
            }
            f.delete();
        }
    }

    public static TitanGraph getGraph(String str)
    {
        try {
            clearFolder(str);
            clearHbase("titan");
        } catch (IOException e) {
            log.info("clear hbase failed!");
            e.printStackTrace();
        }
        return TitanFactory.open(getConfig(str));
    }
    public static void query()
    {
        TitanGraph g2 = getGraph("/home/xue/titanTest");
        log.info("=============================================================================================");
        long st = System.currentTimeMillis();
        Iterable<Vertex> vertices = g2.query().vertices();
        for(Vertex v:vertices)
        {
            log.info(v.getProperty("name")+"  "+v.getProperty("age")+"  "+v.getProperty("type")
                     +"  "+v.getProperty("place")+"  "+
                            v.query().has("name", Query.Compare.EQUAL,"hercules").count());
        }
        long et = System.currentTimeMillis();
        log.info("Cost: " +(et-st)+" ms!");
    }
    public static void pureExample(String dir){
        GraphOfTheGodsFactory.create(dir);
    }
    public void create(String dir)
    {
        BaseConfiguration config = new BaseConfiguration();
        Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        // configuring local backend
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "hbase");
        storage.setProperty("storage.tablename","titan_transwarp_test");
        storage.setProperty("storage.hostname","transwarp-perf1,transwarp-perf2,transwarp-perf3,transwarp-perf4");
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY,dir);
        // configuring elastic search index
        Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
        index.setProperty(INDEX_BACKEND_KEY, "elasticsearch");
        index.setProperty("local-mode", true);
        index.setProperty("client-only", false);
        index.setProperty(STORAGE_DIRECTORY_KEY, dir + File.separator + "es"+System.currentTimeMillis());

        TitanGraph graph = TitanFactory.open(config);
        GraphOfTheGodsFactory.load(graph);
    }
    public static void main(String[] args) throws IOException {
//          pureExample("/home/xue/titanTest");
//        createGraph();
//        create("/home/xue/titanTest");
        clearFolder("/home/xue/titanTest");
    }
}

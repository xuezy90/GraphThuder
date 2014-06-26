package com.transwarp.titan.hbase;


import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 6/19/14
 * Time  : 8:35 PM
 * Email : xuezy90@gmail.com
 */
public class test {
    private Log log = LogFactory.getLog(test.class);
    public void createTable(String tablename,String colfamily)
    {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hAdmin;
        try {
            hAdmin = new HBaseAdmin(conf);
            if(hAdmin.tableExists(tablename.getBytes()))
            {
                log.info(tablename+" already exists!");
            }
            else
            {
                HTableDescriptor ht = new HTableDescriptor(tablename);
                ht.addFamily(new HColumnDescriptor(colfamily));
                hAdmin.createTable(ht);
                log.info(tablename+" create successfully!");
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void graphInsert()
    {
        TitanGraph g = TitanFactory.open("conf/tian.properties");
        Vertex juno = g.addVertex(null);
        juno.setProperty("name","juno");
        Vertex jupiter = g.addVertex(null);
        jupiter.setProperty("name","jupiter");
        Edge married = g.addEdge(null,juno,jupiter,"married");
    }
    public static void main(String[] args) {
        test te = new test();
        te.graphInsert();
    }
}
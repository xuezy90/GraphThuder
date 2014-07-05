package com.transwarp.titan.hbase;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import org.apache.commons.configuration.Configuration;

/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 7/3/14
 * Time  : 4:32 PM
 * Email : xuezy90@gmail.com
 */
public class TransactionTest {

    public static TitanGraph getGraph()
    {
        TitanGraph tg = TitanFactory.open(GraphConfig.create());
        return tg;
    }

    public static void test()
    {
        TitanGraph tg = getGraph();
        tg.buildTransaction().setCacheSize(12500).start();


    }

    public static void main(String[] args) {

    }
}

package com.transwarp.titan.hbase;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 6/24/14
 * Time  : 11:21 AM
 * Email : xuezy90@gmail.com
 */
public class QueryTest {
    private static Log log = LogFactory.getLog(QueryTest.class);
    public static TitanGraph tg;

    public static void openGraph()
    {
        tg = TitanFactory.open(GraphConfig.create());
    }

    private void openGraph(String str) {
        tg = TitanFactory.open(GraphConfig.create(str));
    }

    public QueryTest()
    {
        if(tg==null||!tg.isOpen()) openGraph();
    }

    public QueryTest(String str)
    {
        if(tg==null||!tg.isOpen()) openGraph(str);
    }

    public void vertexCounter()
    {
        Iterable<Vertex> vertices = tg.query().vertices();
        int counter = 0;
        for(Vertex v:vertices)
        {
            counter++;
            if(counter%10000==0)
            log.info(counter+"lines,current vertex name: "+v.getProperty("name"));
            log.info(v.getId()+"  "+v.getProperty("name"));
        }
        log.info("Total: "+counter+" lines");
    }
    public void edgeCounter()
    {
        Iterable<Edge> edges = tg.query().edges();
        int counter = 0;
        for(Edge e:edges)
        {
            counter++;
            if(counter%10000==0)
                log.info(counter + "lines,current edge name: " + e.getLabel());
//            log.info(e.getLabel()+"  "+e.getVertex(Direction.IN).getProperty("name")+"  "+e.getVertex(Direction.OUT).getProperty("name")+"  "+e.getPropertyKeys());
        }
        log.info("Total: "+counter+" lines");
    }

    public static void main(String[] args) {
        String str = "/home/xue/titanTest";
        QueryTest qt = new QueryTest();
        qt.edgeCounter();
    }
}

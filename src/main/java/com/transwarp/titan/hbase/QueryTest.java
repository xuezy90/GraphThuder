package com.transwarp.titan.hbase;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanMultiVertexQuery;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.*;


/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 6/24/14
 * Time  : 11:21 AM
 * Email : xuezy90@gmail.com
 */
public class QueryTest {
    private Log log = LogFactory.getLog(QueryTest.class);
    public  TitanGraph tg;
    private Set<String> ch = new HashSet<String>();
    private Set<String> fa = new HashSet<String>();
    private List<String> idlist = new ArrayList<String>();
    private List<TitanVertex> vlist = new ArrayList<TitanVertex>();
    public int froundCounter;
    public int cNumCounter;

    public void openGraph()
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

    public QueryTest(TitanGraph g)
    {
        this.tg = g;
    }

    public QueryTest(String str,String logFilePath) throws IOException {
        if(tg==null||!tg.isOpen()) openGraph(str);
        getAllVertex();
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

    public void getAllVertex()
    {
        Iterable<Vertex> vertices = tg.query().vertices();
        for(Vertex v:vertices)
        {
            vlist.add((TitanVertex)v);
        }
        log.info(vlist.size());
    }

    public List<TitanVertex> getVertexList(int num)
    {
        List<TitanVertex> ls = new ArrayList<TitanVertex>();
        Random rnd = new Random((long)(Math.random()*1000));
        while(--num>0)
        {
            ls.add(vlist.get(Math.abs(rnd.nextInt()%10000)));
        }
        log.info("Total query: "+ls.size()+" lines");
        return ls;
    }
    private void getAllIds(String path) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(path)));
        String str;
        while((str = br.readLine()) != null)
        {
            idlist.add(str);
        }
        br.close();
    }
    public List<TitanVertex> getVertexList(String path,int num) throws IOException {
        getAllIds(path);
        List<TitanVertex> ls = new ArrayList<TitanVertex>();
        Random and = new Random();
        while(num-->0)
        {
            ls.add((TitanVertex) tg.getVertex(idlist.get(and.nextInt())));
        }
        return ls;
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

    public List<TitanVertex> deepTraversals(TitanMultiVertexQuery mq,List<TitanVertex> vls)
    {
        froundCounter++;
        mq = tg.multiQuery();
        mq.direction(Direction.IN).labels(ConstValue.PID);
        for(TitanVertex v:vls)
        {
            mq.addVertex(v);
        }
        Map<TitanVertex,Iterable<TitanVertex>> resultls = mq.vertices();
        List<TitanVertex> merres = mergeRes(resultls);
//        log.info(merres);
        if(merres.size() == 1) return merres;
        deepTraversals(mq, merres);
        return merres;
    }

    public void getAllChildren(TitanMultiVertexQuery tmq,List<TitanVertex> tx)
    {
        tmq = tg.multiQuery();
        tmq.direction(Direction.OUT).labels(ConstValue.PID);
        for(TitanVertex v:tx)
        {
            tmq.addVertex(v);
        }
        Map<TitanVertex,Iterable<TitanVertex>> resultls = tmq.vertices();
        List<TitanVertex> merres = getChildren(resultls);
//        log.info(merres);
        if(merres.size() == 0) return;
        deepTraversals(tmq, merres);
    }

    private List<TitanVertex> getChildren(Map<TitanVertex, Iterable<TitanVertex>> resultls) {
        List<TitanVertex> newls = new ArrayList<TitanVertex>();
        for(TitanVertex tv:resultls.keySet())
        {
            Iterator<TitanVertex> it = resultls.get(tv).iterator();
            while(it.hasNext())
            {
                newls.add(it.next());
            }
        }
        return newls;
    }

    private List<TitanVertex> mergeRes(Map<TitanVertex,Iterable<TitanVertex>> resultls) {
        List<TitanVertex> newls = new ArrayList<TitanVertex>();
        Set<String> ids = new HashSet<String>();
        for(TitanVertex tv:resultls.keySet())
        {
            Iterator<TitanVertex> it = resultls.get(tv).iterator();
            while(it.hasNext())
            {
                TitanVertex temp = it.next();
//                System.out.print(temp.getID()+"  ");
                if(!ids.contains(temp.getID()+""))
                {
                    cNumCounter++;
                    newls.add(temp);
                    ids.add(temp.getID()+"");
                }
            }
        }
//        System.out.println();
        return newls;
    }

    public static void main(String[] args) throws IOException {
        com.codahale.metrics.MetricRegistry register = MetricManager.INSTANCE.getRegistry();
        String str = "/home/xue/titanTest";
        String idPath = "/home/xue/titanTest/leafvertexid";
        int queryNum = 1000;
        QueryTest qt = new QueryTest(str,idPath);
        TitanMultiVertexQuery mq = null;
        TitanMultiVertexQuery mq1 = null;
        List<TitanVertex> vvv;
//        log.info("-------------------------------------------------------------------------------------");
        for(int j = 0;j<3;j++){
            if(j == 0) queryNum = 10;
            else if(j == 1) queryNum = 100;
            else if(j == 2) queryNum = 1000;
            else break;
            for(int i = 0;i<5;i++)
            {
                qt.froundCounter = 0;
                qt.cNumCounter = 0;
                long stime = System.currentTimeMillis();
                vvv = qt.deepTraversals(mq,qt.getVertexList(queryNum));
//                log.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                long etime = System.currentTimeMillis();
                qt.getAllChildren(mq1,vvv);
                long cetime = System.currentTimeMillis();
//                log.info("Round : " + qt.froundCounter);
//                log.info("Children Num : " + qt.cNumCounter);
//                log.info("Total Cost, get a father cost: " + (etime - stime)+" ms, get all children cost: " + (cetime - etime) + " ms.");
//                log.info("-------------------------------------------------------------------------------------");
            }
        }
    }
}

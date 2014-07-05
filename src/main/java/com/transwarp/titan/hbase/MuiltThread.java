package com.transwarp.titan.hbase;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanMultiVertexQuery;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 7/2/14
 * Time  : 8:12 PM
 * Email : xuezy90@gmail.com
 */
public class MuiltThread implements Runnable {
    private static Log log = LogFactory.getLog(MuiltThread.class);
    private static List<TitanVertex> vlist = new ArrayList<TitanVertex>();
    public static TitanGraph tg = null;
    private static final String INDEX_PATH = "/home/xue/titanTest";
    public static int froundCounter;
    public static int cNumCounter;

    static
    {
        log.info("INITIALIZING...");
        openGraph(INDEX_PATH);
        getAllVertex();
    }

    public static void getAllVertex()
    {
        Iterable<Vertex> vertices = tg.query().vertices();
        for(Vertex v:vertices)
        {
            vlist.add((TitanVertex)v);
        }
        log.info("Vertex size: " + vlist.size());
    }

    private static void openGraph(String str) {
        tg = TitanFactory.open(GraphConfig.create(str));
    }

    public void muiltQuery(int num)
    {
        while(--num >= 0)
        {
            MuiltThread mt = new MuiltThread();
            new Thread(mt).start();
        }
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

    @Override
    public void run() {
        int queryNum = 1000;
        QueryTest qt = new QueryTest(tg);
        TitanMultiVertexQuery mq = null;
        TitanMultiVertexQuery mq1 = null;
        List<TitanVertex> vvv;
        log.info("-------------------------------------------------------------------------------------");
        for(int j = 0;j<3;j++){
            if(j == 0) queryNum = 10;
            else if(j == 1) queryNum = 100;
            else if(j == 2) queryNum = 1000;
            else break;
            for(int i = 0;i<6;i++)
            {
                qt.froundCounter = 0;
                qt.cNumCounter = 0;
                long stime = System.currentTimeMillis();
                vvv = qt.deepTraversals(mq,getVertexList(queryNum));
                log.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                long etime = System.currentTimeMillis();
                qt.getAllChildren(mq1,vvv);
                long cetime = System.currentTimeMillis();
                log.info("Round : " + qt.froundCounter);
                log.info("Children Num : " + qt.cNumCounter);
                log.info("Total Cost, get a father cost: " + (etime - stime)+" ms, get all children cost: " + (cetime - etime) + " ms.");
                log.info("-------------------------------------------------------------------------------------");
            }
        }
    }
    public static void main(String[] args) {
        MuiltThread mt = new MuiltThread();
        mt.muiltQuery(2);
    }
}

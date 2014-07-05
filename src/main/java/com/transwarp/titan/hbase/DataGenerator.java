package com.transwarp.titan.hbase;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 6/23/14
 * Time  : 12:15 PM
 * Email : xuezy90@gmail.com
 */
public class DataGenerator {
    private static Log log = LogFactory.getLog(DataGenerator.class);
    private DecimalFormat df = new DecimalFormat("0.0");
    private final int GRAPH_SIZE = 10000000;
    private final int DEVICE_KINDS = 100;//hundreds
    private final int MAX_CHILDS = 50;
    private final int MAX_FlOORS = 50;
    private final int POINTS_PER_ROUND = 100;
    private static int currPonints = 0;
    private static Set<TitanVertex> leafNode = new HashSet<TitanVertex>();
    public void generrator(String str)
    {
        TitanGraph g = TitanFactory.open(GraphConfig.create());
        try{
        this.load(g);
        }
        catch (Exception e){
            log.info("type alredy exists");
        }
        Vertex root;
        root = g.addVertex(null);
        ElementHelper.setProperties(root,ConstValue.ID,currPonints+"",ConstValue.PB_ID,currPonints+"_"+MD5Hash.getMD5AsHex((currPonints+"").getBytes()),
                ConstValue.DVC_TYPE,df.format(Math.random() * DEVICE_KINDS)+" ");
        currPonints++;
        int maxfloors = MAX_FlOORS;
        this.floorBuilder(g,root,maxfloors);
        g.commit();
    }

    private void floorBuilder(TitanGraph g, Vertex father,int maxfloor) {
        if(maxfloor <= 0) leafNode.add((TitanVertex)father);
        int childNum = (int)(Math.random()*MAX_CHILDS+1);
        Vertex v;
        if(currPonints%POINTS_PER_ROUND==0 ) {
            g.commit();
            log.info(currPonints+" has complete! id :"+father.getId());
        }
        log.debug(childNum);
        if((currPonints+childNum) < this.GRAPH_SIZE && maxfloor > 0)
        {
            while(childNum-- > 0)
            {
                v = g.addVertex(null);
                ElementHelper.setProperties(v,ConstValue.ID,currPonints+"",ConstValue.PB_ID,currPonints+"_"+MD5Hash.getMD5AsHex((currPonints+"").getBytes()),
                        ConstValue.DVC_TYPE,df.format(Math.random() * DEVICE_KINDS)+" ");
                log.debug(father.getProperty(ConstValue.ID) + "----------------->" + v.getProperty(ConstValue.ID));
                father.addEdge(ConstValue.PID, v);
//                v.addEdge("son",father);
                currPonints++;
                this.floorBuilder(g,v,maxfloor-1);
            }

//            log.info(father.getProperty("id_c")+" all "+childNum+" childnodes already created!");
        }

    }

    public void load(final TitanGraph g)
    {
//        g.makeKey("link").dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.ID).dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.SID).dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.PB_ID).dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.DVC_TYPE).dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.DVC_NAME).dataType(String.class).indexed(Vertex.class).make();

        g.makeLabel(ConstValue.PID).manyToMany().make();

        g.commit();
    }
    public void recordLeaf(String path) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
        for(TitanVertex tx:leafNode)
        {
            bw.write(tx.getId().toString() + "\n");
        }
        bw.flush();
        bw.close();
    }
    public static void main(String[] args) {
        DataGenerator dg = new DataGenerator();
        dg.generrator("/home/xue/titanTest");
//        try {
//            dg.recordLeaf("/home/xue/titanTest/leafvertexid");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}

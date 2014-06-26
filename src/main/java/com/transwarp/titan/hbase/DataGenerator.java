package com.transwarp.titan.hbase;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.text.DecimalFormat;

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
    private final int GRAPH_SIZE = 1000000;
    private final int DEVICE_KINDS = 100;//hundreds
    private final int MAX_CHILDS = 50;
    private final int MAX_FlOORS = 10;
    private final int POINTS_PER_ROUND = 100;
    private static int currPonints = 0;
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
        ElementHelper.setProperties(root,"id_c",currPonints+"","name",currPonints+"_"+MD5Hash.getMD5AsHex((currPonints+"").getBytes()),
                "type",df.format(Math.random() * DEVICE_KINDS)+" ");
        currPonints++;
        int maxfloors = MAX_FlOORS;
        this.floorBuilder(g,root,maxfloors);
        g.commit();
    }

    private void floorBuilder(TitanGraph g, Vertex father,int maxfloor) {
        int childNum = (int)(Math.random()*MAX_CHILDS+1);
        Vertex v;
        if(currPonints%POINTS_PER_ROUND==0 ) {
            g.commit();
            log.info(currPonints+" has complete! id_c:"+father.getProperty("id_c")+" name"+father.getProperty("name")+" type");
        }
        log.debug(childNum);
        if((currPonints+childNum) < this.GRAPH_SIZE && maxfloor > 0)
        {
            while(childNum-- > 0)
            {
                v = g.addVertex(null);
                ElementHelper.setProperties(v,"id_c",currPonints+"","name",currPonints+"_"+MD5Hash.getMD5AsHex((currPonints+"").getBytes()),
                        "type",df.format(Math.random() * DEVICE_KINDS)+" ");
                log.debug(father.getProperty("id_c") + "----------------->" + v.getProperty("id_c"));
                father.addEdge("father", v);
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
        g.makeKey("id_c").dataType(String.class).indexed(Vertex.class).make();
        g.makeKey("name").dataType(String.class).indexed(Vertex.class).make();
        g.makeKey("type").dataType(String.class).indexed(Vertex.class).make();

        g.makeLabel("father").manyToMany().make();
        g.makeLabel("son").oneToMany().make();
        g.makeLabel("brother").manyToMany().make();

        g.commit();

    }
    public static void main(String[] args) {
        DataGenerator dg = new DataGenerator();
        dg.generrator("/home/xue/titanTest");
    }
}

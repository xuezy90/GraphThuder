package com.transwarp.titan.hbase;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 6/30/14
 * Time  : 4:18 PM
 * Email : xuezy90@gmail.com
 */
public class RealDataFormat {
    private static Log log = LogFactory.getLog(RealDataFormat.class);
    private static String path = null;
    private static int counter = 0;
    private static int tempcounter = 0;
    public RealDataFormat(String path) {
        this.path = path;
    }

    public void initGraph(final TitanGraph g)
    {
        g.makeKey(ConstValue.ID).dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.SID).dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.PB_ID).dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.DVC_TYPE).dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.DVC_NAME).dataType(String.class).indexed(Vertex.class).make();
        g.makeKey(ConstValue.FATHER_PATH).dataType(String.class).indexed(Vertex.class).make();

        g.makeLabel(ConstValue.PID).manyToMany().make();

        g.commit();

    }

    public void buildGraph(TitanGraph g) throws SQLException, IOException {
        Vertex root = g.addVertex(null);
        StorageHandler dh = new StorageHandler(this.path);
        GraphItem gi = dh.getRoot(ConstValue.ROOT_FLAG);
        counter++;
        ElementHelper.setProperties(root,ConstValue.ID,gi.getId(),ConstValue.PB_ID,gi.getPb_id(),ConstValue.SID,gi.getSid(),
                ConstValue.DVC_TYPE,gi.getDvc_type(),ConstValue.DVC_NAME,gi.getDvc_name());
        addChildren(g, dh, root);
    }

    private void addChildren(TitanGraph g, StorageHandler dh, Vertex root) throws SQLException {
        List<GraphItem> ls = dh.getChildren(root.getProperty(ConstValue.SID).toString(),false);
        log.info(root.getProperty(ConstValue.ID)+" Children number:" + ls.size());
        if(ls == null || ls.size() == 0) return;
        for(GraphItem gi:ls)
        {
            Vertex v = g.addVertex(null);
            ElementHelper.setProperties(v,ConstValue.ID,gi.getId(),ConstValue.PB_ID,gi.getPb_id(),
                    ConstValue.SID,gi.getSid(),ConstValue.DVC_TYPE,gi.getDvc_type(),
                    ConstValue.DVC_NAME,gi.getDvc_name(),ConstValue.FATHER_PATH,gi.getFather_path()+" "+gi.getSid());
            root.addEdge(ConstValue.PID, v);
            counter++;
            if(counter%ConstValue.BATCH_SIZE == 0){
                log.info(counter+" lines has completed! tempcounter :" + tempcounter);
                g.commit();
            }
            this.addChildren(g,dh,v);
        }
        tempcounter++;
   } 



    public static void main(String[] args) {
        String path = "/home/xue/Downloads/suyuangaoke/data.txt";
        RealDataFormat rdf = new RealDataFormat(path);
        // StandardTitanGraph g = new StandardTitanGraph(GraphConfig.create());
        TitanGraph g = TitanFactory.open(GraphConfig.create());
//        g.shutdown();
//        TitanCleanup.clear(g);
        rdf.initGraph(g);
        try {
            rdf.buildGraph(g);
        } catch (SQLException e) {
            log.error(e.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

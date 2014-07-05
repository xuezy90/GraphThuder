package com.transwarp.titan.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 6/30/14
 * Time  : 8:25 PM
 * Email : xuezy90@gmail.com
 */
public class StorageHandler {
    private static final String DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static final String INCEPTOR_IP = "172.16.1.32";
    private static final int PORT = 10000;
    private static final String DB_NAME = "default";
    private static final String TABLE_NAME = "suyuan_graph_clean";
    private static Log log = LogFactory.getLog(StorageHandler.class);
    private Statement sm = null;

    private BufferedReader br;
    private Map<String,List<GraphItem>> childls  =new HashMap<String,List<GraphItem>>();

    public BufferedReader getFileReader(String path) throws FileNotFoundException {

        if(br == null) br = new BufferedReader(new FileReader(new File(path)));
        return br;
    }

    public StorageHandler() throws SQLException {
        dbInit();
    }

    public StorageHandler(String path) throws IOException, SQLException {
        dbInit();
        getFileReader(path);
        graphBuildFromTxt();
    }

    public void closeReader() throws IOException {
        if(br != null) br.close();
    }

    public void graphBuildFromTxt() throws IOException {

        String str;
        List<GraphItem> ll;
        while((str = br.readLine()) != null)
        {
            GraphItem gi = new GraphItem(str.replace("\"", ""));
            if((ll=childls.get(gi.getPid())) == null) ll = new ArrayList<GraphItem>();
            ll.add(gi);
            childls.put(gi.getPid(),ll);
        }
        log.info("Total: "+ childls.size());
    }

    public void showData(String str) throws IOException {
        getFileReader(str);
        graphBuildFromTxt();
    }

    public void dbInit() throws SQLException {

        try{
            Class.forName(DRIVER_NAME);
        }catch(ClassNotFoundException e){
            log.error(e.toString());
            System.exit(1);
        }
        //172.16.1.110 is the IP address of Inceptor Server
        Connection connection = DriverManager.getConnection("jdbc:transwarp://" + INCEPTOR_IP + ":" + PORT + "/" + DB_NAME, "", "");
        log.info("jdbc:transwarp://" + INCEPTOR_IP + ":" + PORT + "/" + DB_NAME);
        sm = connection.createStatement();
    }

    public GraphItem getRoot(String root) throws SQLException {
        String sql = "select * from " + TABLE_NAME +" where dvc_type = '" + root + "'";
        log.info(sql);
        ResultSet rs = sm.executeQuery(sql);
        List<GraphItem> ls = getGraphItem(rs);
        return ls.get(0);
    }

    public List<GraphItem> getChildren(String pid) throws SQLException {
        String sql = "select * from " + TABLE_NAME +" where pid = " + pid;
        return getGraphItem(sm.executeQuery(sql));
    }

    private List<GraphItem> getGraphItem(ResultSet resultSet) throws SQLException {
        log.info("IN getGraphItem ...");
        List<GraphItem> ls = new ArrayList<GraphItem>();
        while(resultSet.next())
        {
            GraphItem gi = new GraphItem();
            gi.setId(resultSet.getString(1));
            gi.setSid(resultSet.getString(2));
            gi.setPid(resultSet.getString(3));
            gi.setDvc_type(resultSet.getString(4));
            gi.setDvc_name(resultSet.getString(5));
            gi.setPb_id(resultSet.getString(6));
            ls.add(gi);
        }
        log.info(ls.size());
        return ls;
    }

    public static void main(String[] args) throws SQLException, IOException {
        String path = "/home/xue/Downloads/suyuangaoke/data.txt";
        StorageHandler dbh = new StorageHandler(path);
        dbh.getRoot(ConstValue.ROOT_FLAG);
//        for(String s:dbh.childls.keySet())
//        {
//            log.info(dbh.childls.get(s));
//        }
    }

    public List<GraphItem> getChildren(String s, boolean b) {
        return childls.get(s);
    }
}

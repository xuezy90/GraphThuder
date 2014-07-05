package com.transwarp.titan.hbase;

/**
 * Created by Intellij IDEA.
 * Author: Allen.Xue
 * Date  : 6/30/14
 * Time  : 8:25 PM
 * Email : xuezy90@gmail.com
 */
public class GraphItem {
    private String id;
    private String sid;
    private String pid;
    private String dvc_type;
    private String dvc_name;
    private String pb_id;
    private String father_path;

    public GraphItem(){}
    public GraphItem(String replace) {
        String[] ss = replace.split(",");
        this.id = ss[0];
        this.sid = ss[1];
        this.pid = ss[2];
        this.dvc_type = ss[3];
        this.dvc_name = ss[4];
        this.pb_id = ss[5];
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getDvc_type() {
        return dvc_type;
    }

    public void setDvc_type(String dvc_type) {
        this.dvc_type = dvc_type;
    }

    public String getPb_id() {
        return pb_id;
    }

    public void setPb_id(String pb_id) {
        this.pb_id = pb_id;
    }

    public String getDvc_name() {
        return dvc_name;
    }

    public void setDvc_name(String dvc_name) {
        this.dvc_name = dvc_name;
    }

    public String getFather_path() {
        return father_path;
    }

    public void setFather_path(String father_path) {
        this.father_path = father_path;
    }
}

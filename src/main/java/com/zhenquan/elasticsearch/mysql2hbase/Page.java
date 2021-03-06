package com.zhenquan.elasticsearch.mysql2hbase;

import java.util.ArrayList;
import java.util.List;


/**
 * 保存页面详细信息
 * Created by dajiangtai on 2016-10-01
 */
public class Page {
    //电视剧id
    //Created by dajiangtai on 2016-10-12
    private String tvId;

    //电视剧名称

    private String tvname;

    // 页面的url

    private String url;

    //总播放量

    private String allnumber;

    //每天播放增量

    private String daynumber;

    //评论数

    private String commentnumber;

    //收藏数

    private String collectnumber;

    //赞

    private String supportnumber;

    //踩

    private String againstnumber;

    //每集数据
    private String episodenumber;

    // 网页内容
    private String content;

    //存储电视剧url（包含列表url和详情url）Created by dajiangtai on 2016-10-05
    private List<String> urlList = new ArrayList<String>();

    public String getTvId() {
        return tvId;
    }

    public void setTvId(String tvId) {
        this.tvId = tvId;
    }

    public String getAllnumber() {
        return allnumber;
    }

    public void setAllnumber(String allnumber) {
        this.allnumber = allnumber;
    }

    public String getDaynumber() {
        return daynumber;
    }

    public void setDaynumber(String daynumber) {
        this.daynumber = daynumber;
    }

    public String getCommentnumber() {
        return commentnumber;
    }

    public void setCommentnumber(String commentnumber) {
        this.commentnumber = commentnumber;
    }

    public String getCollectnumber() {
        return collectnumber;
    }

    public void setCollectnumber(String collectnumber) {
        this.collectnumber = collectnumber;
    }

    public String getSupportnumber() {
        return supportnumber;
    }

    public void setSupportnumber(String supportnumber) {
        this.supportnumber = supportnumber;
    }

    public String getEpisodenumber() {
        return episodenumber;
    }

    public void setEpisodenumber(String episodenumber) {
        this.episodenumber = episodenumber;
    }

    public String getAgainstnumber() {
        return againstnumber;
    }

    public void setAgainstnumber(String againstnumber) {
        this.againstnumber = againstnumber;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<String> getUrlList() {
        return urlList;
    }

    public void addUrl(String url) {
        this.urlList.add(url);
    }

    public String getTvname() {
        return tvname;
    }

    public void setTvname(String tvname) {
        this.tvname = tvname;
    }

}

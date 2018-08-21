package com.zhenquan.elasticsearch.mysql2hbase;

/**
 * TVCount 实体类
 */
public class TVCount {
    private String tvname;
    private String director;
    private String actor;
    private String allnumber;
    private String tvtype;
    private String description;
    private String tvid;
    private String alias;
    private String tvshow;
    private String present;
    private String score;
    private String zone;
    private String commentnumber;
    private String supportnumber;
    private String pic;


    public String getPic() {
        return pic;
    }

    public void setPic(String pic) {
        this.pic = pic;
    }

    public String getTvname() {
        return tvname;
    }

    public void setTvname(String tvname) {
        this.tvname = tvname;
    }

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public String getActor() {
        return actor;
    }

    public void setActor(String actor) {
        this.actor = actor;
    }

    public String getAllnumber() {
        return allnumber;
    }

    public void setAllnumber(String allnumber) {
        this.allnumber = allnumber;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getTvid() {
        return tvid;
    }

    public void setTvid(String tvid) {
        this.tvid = tvid;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getPresent() {
        return present;
    }

    public String getTvtype() {
        return tvtype;
    }

    public void setTvtype(String tvtype) {
        this.tvtype = tvtype;
    }

    public String getTvshow() {
        return tvshow;
    }

    public void setTvshow(String tvshow) {
        this.tvshow = tvshow;
    }

    public void setPresent(String present) {
        this.present = present;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getCommentnumber() {
        return commentnumber;
    }

    public void setCommentnumber(String commentnumber) {
        this.commentnumber = commentnumber;
    }

    public String getSupportnumber() {
        return supportnumber;
    }

    public void setSupportnumber(String supportnumber) {
        this.supportnumber = supportnumber;
    }

    @Override
    public String toString() {
        return "TVCount{" +
                "tvname='" + tvname + '\'' +
                ", director='" + director + '\'' +
                ", actor='" + actor + '\'' +
                ", allnumber='" + allnumber + '\'' +
                ", tvtype='" + tvtype + '\'' +
                ", description='" + description + '\'' +
                ", tvid='" + tvid + '\'' +
                ", alias='" + alias + '\'' +
                ", tvshow='" + tvshow + '\'' +
                ", present='" + present + '\'' +
                ", score='" + score + '\'' +
                ", zone='" + zone + '\'' +
                ", commentnumber='" + commentnumber + '\'' +
                ", supportnumber='" + supportnumber + '\'' +
                ", pic='" + pic + '\'' +
                '}';
    }
}

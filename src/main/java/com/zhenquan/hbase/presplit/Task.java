package com.zhenquan.hbase.presplit;
/**
 * 学习任务实体类
 * @author dajiangtai
 *
 */
public class Task {
	public String uid;
	public String systaskid;
	public String taskid;
	public String name;
	public String type ;
	public String state;
	public String starttime;
	public String finishtime;
	public String receivedate ;
	public String actualfinishtime;
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getSystaskid() {
		return systaskid;
	}
	public void setSystaskid(String systaskid) {
		this.systaskid = systaskid;
	}
	public String getTaskid() {
		return taskid;
	}
	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getStarttime() {
		return starttime;
	}
	public void setStarttime(String starttime) {
		this.starttime = starttime;
	}
	public String getFinishtime() {
		return finishtime;
	}
	public void setFinishtime(String finishtime) {
		this.finishtime = finishtime;
	}
	public String getReceivedate() {
		return receivedate;
	}
	public void setReceivedate(String receivedate) {
		this.receivedate = receivedate;
	}
	public String getActualfinishtime() {
		return actualfinishtime;
	}
	public void setActualfinishtime(String actualfinishtime) {
		this.actualfinishtime = actualfinishtime;
	}
	

	
}

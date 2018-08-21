package com.zhenquan.elasticsearch;

import java.util.Date;

/**
 * user bean
 * @author 大讲台
 *
 */
public class User {
	private String user;
	private Date postDate;
	private String message;
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public Date getPostDate() {
		return postDate;
	}
	public void setPostDate(Date postDate) {
		this.postDate = postDate;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	
}

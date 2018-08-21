package com.zhenquan.elasticsearch;

public class Acount {
	private String phone;
	private String name;
	private String sex;
	private int age;
	public Acount(String phone,String name,String sex,int age){
		this.phone = phone;
		this.name = name;
		this.sex = sex;
		this.age = age;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSex() {
		return sex;
	}
	public void setSex(String sex) {
		this.sex = sex;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	
}

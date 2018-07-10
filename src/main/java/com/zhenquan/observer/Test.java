package com.zhenquan.observer;

import java.util.ArrayList;

public class Test {
    public static void main(String [] args){
        WechatServer wechatServer = new WechatServer();
        Observer o1 = new User("张三");
        Observer o2 = new User("lisi");
        Observer o3 = new User("wangwu");
        Observer o4 = new User("liyan");
        ArrayList<Observer> objects = new ArrayList<>();
        objects.add(o1);
        objects.add(o2);
        objects.add(o3);
        objects.add(o4);

        for (int i = 0; i < objects.size(); i++) {
            Observer observer = objects.get(i);
            wechatServer.registObserver(observer);
        }
        wechatServer.setInfo("aaaaaaaaaaaaaaaaaaaaaaa");

        System.out.println("--------------------------------------");
        wechatServer.removeObserver(o2);
        wechatServer.setInfo("bbbbbbbbbbbbbbbbbbbbbbbbb");

    }
}

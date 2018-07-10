package com.zhenquan.observer;

import java.util.ArrayList;
import java.util.List;

public class WechatServer implements Observerable {
    private List<Observer> list;
    private String message;

    public WechatServer() {
        this.list = new ArrayList<>();
    }

    @Override
    public void registObserver(Observer observer) {
        list.add(observer);
    }

    @Override
    public void removeObserver(Observer observer) {
        list.remove(observer);
    }

    @Override
    public void notifyObserver() {
        for (int i = 0; i < list.size(); i++) {
            Observer observer = list.get(i);
            observer.update(message);
        }
    }

    public void setInfo(String s) {
        this.message = s;
        System.out.println("微信服务更新消息：" + s);
        notifyObserver();
    }
}

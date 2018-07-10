package com.zhenquan.observer;

import java.util.Observable;

public class User implements Observer {
    private String name;
    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public User() {
    }

    public User(String name) {
        this.name = name;
    }

    public User(String name, String message) {
        this.name = name;
        this.message = message;
    }

    @Override
    public void update(String message) {
        this.message = message;
        System.out.println(name + "收到推送消息:" + message);
    }
}

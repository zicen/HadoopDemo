package com.zhenquan.observer;


/**
 * 被观察者
 */
public interface Observerable {

    public void registObserver(Observer observer);

    public void removeObserver(Observer observer);

    public void notifyObserver();

}

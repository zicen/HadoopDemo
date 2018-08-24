package com.zhenquan.flume;

import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.simple.JSONArray;

import java.util.List;

public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String tmpStr = new String(event.getBody(),
                Charsets.UTF_8);

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        return null;
    }

    @Override
    public void close() {

    }
}

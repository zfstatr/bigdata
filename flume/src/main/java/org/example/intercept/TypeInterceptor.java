package org.example.intercept;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {

    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        addHeaderEvents = new ArrayList<>();
    }

    //单个事件拦截
    @Override
    public Event intercept(Event event) {
        //1.获取事件的头信息
        Map<String, String> headers = event.getHeaders();

        //2.获取事件的body信息
        String body = new String(event.getBody());

        //3.根据body中是否有hello决定添加怎样的头信息
        if (body.contains("hello")){
            headers.put("type", "zf");
        }else {
            headers.put("type", "others");
        }
        return event;
    }
    //批量事件拦截
    @Override
    public List<Event> intercept(List<Event> events) {
        //1.清空集合
        addHeaderEvents.clear();

        //2.遍历events给每个事件添加头信息
        for (Event event : events) {
            addHeaderEvents.add(intercept(event));
        }
        //3.返回结果
        return addHeaderEvents;

    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

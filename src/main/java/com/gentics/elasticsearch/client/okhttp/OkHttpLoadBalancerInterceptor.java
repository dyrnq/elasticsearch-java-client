package com.gentics.elasticsearch.client.okhttp;


import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.Request;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class OkHttpLoadBalancerInterceptor implements Interceptor {

    private final List<Server> allServerList = new ArrayList<>();
    // url size
    private final int size;
    // 计数：下次轮询服务下标
    private final AtomicInteger nextUrlCyclicCounter;

    /**
     * 初始化拦截器
     */
    public OkHttpLoadBalancerInterceptor(String schema, List<String> urls) {
        for (String url : urls) {
            String[] hostAndPort = url.split(":");
            allServerList.add(new Server(schema, hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        }
        this.size = this.allServerList.size();
        this.nextUrlCyclicCounter = new AtomicInteger(0);
    }
    // 定时器执行ping服务

    /**
     * 拦截器切入执行
     */
    @Override
    public okhttp3.Response intercept(Chain chain) throws IOException {
        // 获取原始的originalRequest
        Request originalRequest = chain.request();
        // 不需要负载均衡
        if (size < 2) {
            return chain.proceed(originalRequest);
        }
        // 获取老的url
        HttpUrl oldUrl = originalRequest.url();
        // 获取originalRequest的创建者builder
        Request.Builder builder = originalRequest.newBuilder();
        // 重建新的HttpUrl，需要重新设置的url部分
        HttpUrl newHttpUrl = getHttpUrl(oldUrl);
        // 没有可用地址

        if (newHttpUrl == null) {
            return chain.proceed(originalRequest);
        }

        // 获取新newRequest
        Request newRequest = builder.url(newHttpUrl).build();
        // 请求
        return proceedRequest(chain, newRequest, 0);
    }

    /**
     * 递归切换有问题的url
     */
    public okhttp3.Response proceedRequest(Chain chain, Request request, int retryCount) throws IOException {

        try {
            return chain.proceed(request);
        } catch (IOException e) {
            // 防止切换到无效的地址、防止同一请求轮询到同一无效地址
            if (retryCount++ < 10) {
                HttpUrl oldUrl = request.url();
                // 切换url
                HttpUrl httpUrl = getHttpUrl(oldUrl);
                // 没有可用服务地址
                if (httpUrl == null) {
                    throw new ConnectException("no useful address");
                }
                // 新建request
                Request newRequest = chain.request().newBuilder().url(httpUrl).build();
                return proceedRequest(chain, newRequest, retryCount);
            } else {
                throw new ConnectException("no useful address");
            }
        }
    }

    /**
     * 重建新的HttpUrl，需要重新设置的url部分
     */
    public HttpUrl getHttpUrl(HttpUrl oldUrl) {
        // 获取server
        Server server = chooseServer();
        if (server == null) {
            return null;
        }
        // 获取新的url

        return oldUrl.newBuilder()
                .scheme(server.getScheme())// http协议如：http或者https
                .host(server.getHost())// 主机地址
                .port(server.getPort())// 端口
                .build();
    }

    /**
     * 选择可用服务
     */
    public Server chooseServer() {
        int nextServerIndex = incrementAndGetModulo();
        Server server = allServerList.get(nextServerIndex);
        return server;
    }

    /**
     * 多线程轮询获取
     */
    public int incrementAndGetModulo() {
        for (; ; ) {
            int current = nextUrlCyclicCounter.get();
            int next = (current + 1) % size;
            if (nextUrlCyclicCounter.compareAndSet(current, next)) {
                return next;
            }
        }
    }


    class Server {
        String host;
        String scheme;
        int port;

        public Server(String scheme, String host, int port) {
            this.host = host;
            this.scheme = scheme;
            this.port = port;
        }

        public Server(String host, int port) {
            this("http", host, port);
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getScheme() {
            return scheme;
        }

        public void setScheme(String scheme) {
            this.scheme = scheme;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }


    }


}


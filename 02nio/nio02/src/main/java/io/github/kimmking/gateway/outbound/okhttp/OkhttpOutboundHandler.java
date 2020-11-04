package io.github.kimmking.gateway.outbound.okhttp;

import io.github.kimmking.gateway.outbound.httpclient4.NamedThreadFactory;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.protocol.HTTP;

import java.io.IOException;
import java.util.concurrent.*;

import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class OkhttpOutboundHandler {
    private CloseableHttpAsyncClient httpclient;
    private ExecutorService proxyService;
    private String backendUrl;
    private OkHttpClient client;

    public OkhttpOutboundHandler(String backendUrl){
        this.backendUrl = backendUrl.endsWith("/")?backendUrl.substring(0,backendUrl.length()-1):backendUrl;
        int cores = Runtime.getRuntime().availableProcessors() * 2;
        long keepAliveTime = 1000;
        int queueSize = 2048;
        RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();//.DiscardPolicy();
        proxyService = new ThreadPoolExecutor(cores, cores,
                keepAliveTime, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueSize),
                new NamedThreadFactory("proxyService"), handler);
    }

    public void handle(final FullHttpRequest fullRequest, final ChannelHandlerContext ctx) {
        final String url = this.backendUrl + fullRequest.uri();
        proxyService.submit(()->fetchGet(fullRequest, ctx, url));
    }

    private void fetchGet(final FullHttpRequest inbound, final ChannelHandlerContext ctx, final String url) {
        client = new OkHttpClient.Builder()
                .addInterceptor(new HandleRespInterceptor(inbound, ctx))
                .build();

        Request request = new Request.Builder()
                .header(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE)
                .url(url)
                .build();
        try {
            Response response = client.newCall(request).execute();
            System.out.println(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    class HandleRespInterceptor implements Interceptor {
        FullHttpResponse response = null;
        FullHttpRequest inbound;
        ChannelHandlerContext ctx;

        public HandleRespInterceptor(FullHttpRequest inbound, ChannelHandlerContext ctx) {
            this.ctx = ctx;
            this.inbound = inbound;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            Response okHttpResponse = chain.proceed(request);
            try {
                byte[] respBytes = okHttpResponse.body().bytes();
                response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(respBytes));
                response.headers().set("Content-Type", "application/json");
                response.headers().setInt("Content-Length", Integer.parseInt(okHttpResponse.header("Content-Length")));
            }catch (Exception e) {
                e.printStackTrace();
                response = new DefaultFullHttpResponse(HTTP_1_1, NO_CONTENT);
                exceptionCaught(ctx, e);
            } finally {
                if (inbound != null) {
                    if (!HttpUtil.isKeepAlive(inbound)) {
                        ctx.write(response).addListener(ChannelFutureListener.CLOSE);
                    } else {
                        ctx.write(response);
                    }
                }
                ctx.flush();
            }
            return okHttpResponse;
        }

    }
}

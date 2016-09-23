package pl.codewise.amazon.client.http;

import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;
import pl.codewise.amazon.client.auth.Operation;

class RequestSender implements FutureListener<Channel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestSender.class);

    private final String s3Location;
    private final HandlerDemultiplexer demultiplexer;
    private final ChannelPool channelPool;

    private final Request requestData;
    private final SubscriptionCompletionHandler completionHandler;

    RequestSender(String s3Location, Request requestData, SubscriptionCompletionHandler completionHandler, HandlerDemultiplexer demultiplexer, ChannelPool channelPool) {
        this.s3Location = s3Location;
        this.requestData = requestData;
        this.completionHandler = completionHandler;
        this.demultiplexer = demultiplexer;
        this.channelPool = channelPool;
    }

    @Override
    public void operationComplete(Future<Channel> future) throws Exception {
        if (!future.isSuccess()) {
            completionHandler.onError(future.cause());
        } else {
            try {
                executeRequest(future.getNow(), requestData);
            } catch (Exception e) {
                completionHandler.onError(e);
            }
        }
    }

    private void executeRequest(Channel channel, Request requestData) throws Exception {
        DefaultFullHttpRequest request;
        if (requestData.getOperation().equals(Operation.PUT)) {
            request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, requestData.getOperation().getHttpMethod(), requestData.getUrl(), requestData.getBody());
        } else {
            request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, requestData.getOperation().getHttpMethod(), requestData.getUrl());
        }

        if (requestData.getBody() != null && !(requestData.getBody() instanceof UnpooledHeapByteBuf)) {
            LOGGER.warn("Pooled buffer used {} for method {} on url {}",
                    requestData.getBody().getClass(), requestData.getOperation(), requestData.getUrl());
        }

        request.headers().set(HttpHeaderNames.HOST, requestData.getBucketName() + "." + s3Location);
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        request.headers().set(HttpHeaderNames.CONTENT_TYPE, requestData.getContentType());
        request.headers().set(HttpHeaderNames.CONTENT_LENGTH, requestData.getContentLength());
        request.headers().set(HttpHeaderNames.CONTENT_MD5, requestData.getMd5());

        requestData.getSignatureCalculatorFactory().getSignatureCalculator()
                .calculateAndAddSignature(request.headers(), requestData);

        demultiplexer.setAttributeValue(channel, new HttpClientHandler(channelPool, completionHandler));
        channel.writeAndFlush(request);
    }
}

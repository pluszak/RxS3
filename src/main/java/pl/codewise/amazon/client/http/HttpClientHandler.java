package pl.codewise.amazon.client.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.codec.http.*;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.codewise.amazon.client.SubscriptionCompletionHandler;

public class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientHandler.class);

    public static final AttributeKey<SubscriptionCompletionHandler> HANDLER_ATTRIBUTE_KEY = AttributeKey.valueOf("handler");
    public static final AttributeKey<ByteBuf> BUFFER_ATTRIBUTE_KEY = AttributeKey.valueOf("buffer");

    private final ChannelPool channelPool;

    private HttpResponseStatus status;
    private boolean keepAlive;

    public HttpClientHandler(ChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        Attribute<ByteBuf> bufferAttribute = ctx.channel().attr(BUFFER_ATTRIBUTE_KEY);
        ByteBuf result = bufferAttribute.get();

        if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            keepAlive = HttpHeaders.isKeepAlive(response);
            status = response.getStatus();

            if (result != null && result.refCnt() == 1) {
                ReferenceCountUtil.release(result);
            }

            result = ctx.alloc().buffer();
            bufferAttribute.set(result);
        }

        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;

            result.writeBytes(content.content());
            if (content instanceof LastHttpContent) {
                Attribute<SubscriptionCompletionHandler> handlerAttribute = ctx.channel().attr(HANDLER_ATTRIBUTE_KEY);
                SubscriptionCompletionHandler handler = handlerAttribute.getAndRemove();

                if (!keepAlive) {
                    ctx.close();
                }

                channelPool.release(ctx.channel());

                handler.onNext(Pair.of(status, result));
                handler.onCompleted();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Attribute<SubscriptionCompletionHandler> handlerAttribute = ctx.channel().attr(HANDLER_ATTRIBUTE_KEY);
        SubscriptionCompletionHandler handler = handlerAttribute.getAndRemove();

        if (handler == null) {
            LOGGER.warn("Exception caught but handler is null", cause);
            channelPool.release(ctx.channel());
        } else {
            handler.onError(cause);
        }
    }
}

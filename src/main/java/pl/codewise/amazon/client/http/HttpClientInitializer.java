package pl.codewise.amazon.client.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;

public class HttpClientInitializer {

    public void initChannel(ChannelPool channelPool, Channel ch) {
        ChannelPipeline p = ch.pipeline();

        p.addLast(new HttpClientCodec());
        p.addLast(new HttpContentDecompressor());
        p.addLast(new HttpClientHandler<>(channelPool));
    }
}

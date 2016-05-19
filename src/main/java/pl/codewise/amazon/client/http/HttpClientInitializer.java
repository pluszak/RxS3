package pl.codewise.amazon.client.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import pl.codewise.amazon.client.InactiveConnectionsHandler;

public class HttpClientInitializer {

    public static final int BYTES_IN_MEGABYTE = 1024 * 1024;
    public static final int MAX_REQUEST_SIZE = 1000 * BYTES_IN_MEGABYTE;

    public void initChannel(Channel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new HttpClientCodec());
        p.addLast(new HttpContentDecompressor());
        p.addLast(new HttpObjectAggregator(MAX_REQUEST_SIZE));
        p.addLast(InactiveConnectionsHandler.NAME, new InactiveConnectionsHandler());
    }
}

package ru.serce;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import ru.serce.ChatProtocol.Message;
import ru.serce.ChatProtocol.Message.Type;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author serce
 * @since 20.11.15.
 */
public class ChannelBench {

    public static final Message MESSAGE = Message.newBuilder()
            .setType(Type.MESSAGE)
            .setAuthor(Thread.currentThread().getName())
            .addText("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
            .build();
    private static int COUNT = 10;
    private static int PER_CONN = 10;
    private static CountDownLatch cdl = new CountDownLatch(COUNT * PER_CONN * (COUNT - 1));

    public static void main(String[] args) throws IOException, InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();

                            p.addLast(new ProtobufVarint32FrameDecoder());
                            p.addLast(new ProtobufDecoder(ChatProtocol.Message.getDefaultInstance()));

                            p.addLast(new ProtobufVarint32LengthFieldPrepender());
                            p.addLast(new ProtobufEncoder());

                            p.addLast(new SimpleChannelInboundHandler<ChatProtocol.Message>() {
                                @Override
                                protected void messageReceived(ChannelHandlerContext ctx, ChatProtocol.Message msg) throws Exception {
                                    cdl.countDown();
                                }
                            });
                        }
                    });


            CountDownLatch connected = new CountDownLatch(COUNT);
            ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
            for (int i = 0; i < COUNT; i++) {
                b.connect("localhost", 20026)
                        .addListener(new GenericFutureListener<ChannelFuture>() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (!future.isSuccess()) {
                                    System.out.println("ERROR " + future.cause().getMessage());
                                    System.exit(1);
                                } else {
                                    Channel ch = future.channel();
                                    channels.add(ch);
                                    connected.countDown();
                                }
                            }
                        });
            }
            connected.await();
            long start = System.nanoTime();
            for (int i = 0; i < PER_CONN; i++) {
                channels.writeAndFlush(MESSAGE);
            }
            cdl.await();
            long end = System.nanoTime();
            System.out.println("TIME " + TimeUnit.NANOSECONDS.toMillis(end - start) + " ms");
        } finally {
            group.shutdownGracefully();
        }
    }
}

package ru.serce;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import ru.serce.ChatProtocol.Message;
import ru.serce.ChatProtocol.Message.Type;

import java.io.IOException;
import java.util.Arrays;
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
    public static final byte[] coded = new byte[]{16,2,26,36,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,65,34,4,109,97,105,110};
    private static int COUNT = 1300;
    private static int PER_CONN = 10;
    private static final int NUM = COUNT * PER_CONN * (COUNT - 1);
    private static final AtomicInteger res = new AtomicInteger(NUM);
    private static CountDownLatch cdl = new CountDownLatch(NUM);

    static {
        Thread t = new Thread(() -> {
            while (true) {
                System.out.println("GET " + res.get());
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                            cause.printStackTrace();
                            ctx.close();
                        }

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();

                            p.addLast(new ProtobufVarint32FrameDecoder());

                            p.addLast(new ProtobufVarint32LengthFieldPrepender());
                            p.addLast(new ProtobufEncoder());

                            p.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                                    if(res.get() == 0) {
                                        // handle + 1
                                    } else {
                                        if (!Arrays.equals(coded, msg.array())) {
                                            throw new RuntimeException("UNEQUAL!");
                                        }
                                        int len = msg.array().length;
                                        if (len != 46)
                                            System.out.println(len);
                                        res.decrementAndGet();
                                        cdl.countDown();
                                    }
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    });


            CountDownLatch connected = new CountDownLatch(COUNT);
            ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
            for (int i = 0; i < COUNT; i++) {
                b.connect("localhost", 20053)
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
            Thread.sleep(2500L);
            long start = System.nanoTime();
            for (int i = 0; i < PER_CONN; i++) {
                channels.writeAndFlush(MESSAGE);
            }
            cdl.await();
            long end = System.nanoTime();
            long millis = TimeUnit.NANOSECONDS.toMillis(end - start);
            System.out.println("TIME " + millis + " ms");
            System.out.println("MES/PER SEC = " +  1000 * COUNT * PER_CONN / (double)millis);
            Channel chan = channels.iterator().next();
        } finally {
            group.shutdownGracefully();
        }
    }
}

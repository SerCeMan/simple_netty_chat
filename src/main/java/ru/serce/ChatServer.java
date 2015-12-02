package ru.serce;

import com.google.protobuf.CodedOutputStream;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author serce
 * @since 20.11.15.
 */
public class ChatServer {
    public static final int LIMIT = 1000;
    public static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    public static final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    public static final EventLoopGroup workerGroup = new NioEventLoopGroup();
    public static final int RESCHEDULE_TIME = 50;
    public static final AtomicInteger sended = new AtomicInteger();
    public static final AtomicInteger tried = new AtomicInteger();

    static {
        new Thread(() -> {
            while (true) {
                try {
                    System.out.println("CHANS " + channels.size());
                    System.out.println("SENDED " + sended.get());
                    System.out.println("TRIED " + tried.get());

                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }


    public static void main(String[] args) throws InterruptedException {
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.TCP_NODELAY, true);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                            cause.printStackTrace();
                            ctx.close();
                        }

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new ProtobufVarint32FrameDecoder());
//                            p.addLast(new ProtobufDecoder(ChatProtocol.Message.getDefaultInstance()));

                            p.addLast(new ByteBufLenghtBasedWriter());
//                            p.addLast(new ProtobufEncoder());

                            p.addLast(new SimpleChannelInboundHandler<ByteBuf>(true) {

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    channels.add(ctx.channel());
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                    channels.remove(ctx.channel());
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                                    tried.incrementAndGet();
                                    ByteBuf copy = msg;
//                                    deque.put(new Pair<>(ctx.channel(), copy));
                                    boolean offer = false; //deque.offer(new Pair<>(ctx.channel(), copy));
                                    if (!offer) {
                                        Channel ctxChan = ctx.channel();
                                        CopyOnWriteArrayList<Channel> failed = new CopyOnWriteArrayList<>();
                                        for (Channel ch : channels) {
                                            if (ch != ctxChan) {
                                                copy.retain();
                                                if (ch.isWritable()) {
                                                    ch.writeAndFlush(copy, ch.voidPromise());
                                                } else {
                                                    failed.add(ch);
                                                }
                                            }
                                        }
                                        if (failed.size() > 0) {
                                            ctx.executor().schedule(() -> {
                                                try {
                                                    send(ctx, failed, copy);
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                }
                                            }, 50, TimeUnit.MILLISECONDS);
                                        }
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

            b.bind(20026).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static void send(ChannelHandlerContext ctx, CopyOnWriteArrayList<Channel> chans, ByteBuf msg) throws Exception {
        CopyOnWriteArrayList<Channel> failed = new CopyOnWriteArrayList<>();
        for (Channel ch : chans) {
            if (ch.isWritable()) {
                ch.writeAndFlush(msg, ch.voidPromise());
            } else {
                failed.add(ch);
            }
        }
        if (failed.size() > 0) {
            System.out.println("REQUEUE " + failed.size());
            ctx.executor().schedule(() -> {
                try {
                    send(ctx, failed, msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, RESCHEDULE_TIME, TimeUnit.MILLISECONDS);
        }
    }

    public static void writeRawVarint32(ByteBuf buf, int value) throws IOException {
        while (true) {
            if ((value & ~0x7F) == 0) {
                buf.writeByte((byte)value);
                return;
            } else {
                buf.writeByte((byte)(value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    @ChannelHandler.Sharable
    private static class ByteBufLenghtBasedWriter extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            ByteBuf buf = null;
            try {
                ByteBuf cast = (ByteBuf) msg;
                buf = ctx.alloc().ioBuffer();

                int bodyLen = cast.readableBytes();
                int headerLen = CodedOutputStream.computeRawVarint32Size(bodyLen);
                buf.ensureWritable(headerLen + bodyLen);
                writeRawVarint32(buf, bodyLen);
                try {
                    buf.writeBytes(cast, buf.readerIndex(), bodyLen);
                } finally {
                    cast.release();
                }

                if (buf.isReadable()) {
                    ctx.write(buf, promise);
                } else {
                    buf.release();
                    ctx.write(Unpooled.EMPTY_BUFFER, promise);
                }
                buf = null;
            } catch (EncoderException e) {
                e.printStackTrace();
                throw e;
            } catch (Throwable e) {
                e.printStackTrace();
                throw new EncoderException(e);
            } finally {
                if (buf != null) {
                    buf.release();
                }
            }
        }
    }
}

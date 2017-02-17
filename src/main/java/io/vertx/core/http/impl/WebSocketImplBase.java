/*
 * Copyright (c) 2011-2013 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.core.http.impl;

import io.netty.buffer.ByteBuf;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.http.impl.ws.WebSocketFrameImpl;
import io.vertx.core.http.impl.ws.WebSocketFrameInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.net.impl.ConnectionBase;

import java.util.UUID;

/**
 * This class is optimised for performance when used on the same event loop. However it can be used safely from other threads.
 * <p>
 * The internal state is protected using the synchronized keyword. If always used on the same event loop, then
 * we benefit from biased locking which makes the overhead of synchronized near zero.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public abstract class WebSocketImplBase implements WebSocketBase {

  private final boolean supportsContinuation;
  private final String textHandlerID;
  private final String binaryHandlerID;
  private final int maxWebSocketFrameSize;
  private final MessageConsumer binaryHandlerRegistration;
  private final MessageConsumer textHandlerRegistration;
  protected final ConnectionBase conn;

  private Buffer textMessageBuffer;
  private Buffer binaryMessageBuffer;

  protected Handler<WebSocketFrame> frameHandler;
  protected Handler<String> textMessageHandler;
  protected Handler<Buffer> binaryMessageHandler;
  protected Handler<Buffer> dataHandler;
  protected Handler<Void> drainHandler;
  protected Handler<Throwable> exceptionHandler;
  protected Handler<Void> closeHandler;
  protected Handler<Void> endHandler;
  protected boolean closed;

  protected WebSocketImplBase(VertxInternal vertx, ConnectionBase conn, boolean supportsContinuation,
                              int maxWebSocketFrameSize) {
    this.supportsContinuation = supportsContinuation;
    this.textHandlerID = UUID.randomUUID().toString();
    this.binaryHandlerID = UUID.randomUUID().toString();
    this.conn = conn;
    Handler<Message<Buffer>> binaryHandler = msg -> writeBinaryFrameInternal(msg.body());
    binaryHandlerRegistration = vertx.eventBus().<Buffer>localConsumer(binaryHandlerID).handler(binaryHandler);
    Handler<Message<String>> textHandler = msg -> writeTextFrameInternal(msg.body());
    textHandlerRegistration = vertx.eventBus().<String>localConsumer(textHandlerID).handler(textHandler);
    this.maxWebSocketFrameSize = maxWebSocketFrameSize;
    this.textMessageBuffer = Buffer.buffer(maxWebSocketFrameSize);
    this.binaryMessageBuffer = Buffer.buffer(maxWebSocketFrameSize);
  }

  public String binaryHandlerID() {
    return binaryHandlerID;
  }

  public String textHandlerID() {
    return textHandlerID;
  }

  public boolean writeQueueFull() {
    synchronized (conn) {
      checkClosed();
      return conn.isNotWritable();
    }
  }

  public void close() {
    synchronized (conn) {
      checkClosed();
      conn.close();
      cleanupHandlers();
    }
  }

  @Override
  public SocketAddress localAddress() {
    return conn.localAddress();
  }

  @Override
  public SocketAddress remoteAddress() {
    return conn.remoteAddress();
  }

  public abstract WebSocketBase exceptionHandler(Handler<Throwable> handler);

  protected void writeMessageInternal(Buffer data) {
    checkClosed();
    writePartialMessage(FrameType.BINARY, data, 0);
  }

  protected void writeTextMessageInternal(String text) {
    checkClosed();
    Buffer data = Buffer.buffer(text);
    writePartialMessage(FrameType.TEXT, data, 0);
  }

  /**
   * Splits the provided buffer into multiple frames (which do not exceed the maximum web socket frame size)
   * and writes them in order to the socket.
   */
  protected void writePartialMessage(FrameType frameType, Buffer data, int offset) {
    int end = offset + maxWebSocketFrameSize;
    boolean isFinal;
    if (end >= data.length()) {
      end  = data.length();
      isFinal = true;
    } else {
      isFinal = false;
    }
    Buffer slice = data.slice(offset, end);
    WebSocketFrame frame;
    if (offset == 0 || !supportsContinuation) {
      frame = new WebSocketFrameImpl(frameType, slice.getByteBuf(), isFinal);
    } else {
      frame = WebSocketFrame.continuationFrame(slice, isFinal);
    }
    writeFrame(frame);
    int newOffset = offset + maxWebSocketFrameSize;
    if (!isFinal) {
      writePartialMessage(frameType, data, newOffset);
    }
  }

  protected void writeBinaryFrameInternal(Buffer data) {
    ByteBuf buf = data.getByteBuf();
    WebSocketFrame frame = new WebSocketFrameImpl(FrameType.BINARY, buf);
    writeFrame(frame);
  }

  protected void writeTextFrameInternal(String str) {
    WebSocketFrame frame = new WebSocketFrameImpl(str);
    writeFrame(frame);
  }

  protected void writeFrameInternal(WebSocketFrame frame) {
    synchronized (conn) {
      checkClosed();
      conn.reportBytesWritten(frame.binaryData().length());
      conn.writeToChannel(frame);
    }
  }

  protected void checkClosed() {
    if (closed) {
      throw new IllegalStateException("WebSocket is closed");
    }
  }

  void handleFrame(WebSocketFrameInternal frame) {
    synchronized (conn) {
      conn.reportBytesRead(frame.binaryData().length());
      if (dataHandler != null) {
        Buffer buff = Buffer.buffer(frame.getBinaryData());
        dataHandler.handle(buff);
      }

      if (frameHandler != null) {
        frameHandler.handle(frame);
      }

      if (textMessageHandler != null) {
        textMessageBuffer.appendBuffer(Buffer.buffer(frame.getBinaryData()));
        if (frame.isFinal()) {
          String fullMessage = textMessageBuffer.toString();
          textMessageBuffer = Buffer.buffer(textMessageBuffer.length());
          textMessageHandler.handle(fullMessage);
        }
      }

      if (binaryMessageHandler != null) {
        binaryMessageBuffer.appendBuffer(Buffer.buffer(frame.getBinaryData()));
        if (frame.isFinal()) {
          Buffer fullMessage = binaryMessageBuffer.copy();
          binaryMessageBuffer = Buffer.buffer(binaryMessageBuffer.length());
          binaryMessageHandler.handle(fullMessage);
        }
      }
    }
  }

  void writable() {
    if (drainHandler != null) {
      Handler<Void> dh = drainHandler;
      drainHandler = null;
      dh.handle(null);
    }
  }

  void handleException(Throwable t) {
    synchronized (conn) {
      if (exceptionHandler != null) {
        exceptionHandler.handle(t);
      }
    }
  }

  void handleClosed() {
    synchronized (conn) {
      cleanupHandlers();
      if (endHandler != null) {
        endHandler.handle(null);
      }
      if (closeHandler != null) {
        closeHandler.handle(null);
      }
    }
  }

  private void cleanupHandlers() {
    if (!closed) {
      binaryHandlerRegistration.unregister();
      textHandlerRegistration.unregister();
      closed = true;
    }
  }

}

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    // TODO make the max buffer sizes configurable (and enforce them)?
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
    writeBinaryMessageChunks(data);
  }

  protected void writeTextMessageInternal(String text) {
    checkClosed();
    writeTextMessageChunks(text);
  }

  protected void writeBinaryMessageChunks(Buffer data) {
    List<Buffer> chunks = chunkMessage(data);
    writeChunksAsFrames(chunks, FrameType.BINARY);
  }

  protected void writeTextMessageChunks(String stringData) {
    Buffer data = Buffer.buffer(stringData);
    List<Buffer> chunks = chunkMessage(data);
    writeChunksAsFrames(chunks, FrameType.TEXT);
  }

  /**
   * Splits the provided buffer into chunks of byte lengths not exceeding the maximum web socket frame size.
   *
   * @param buffer the buffer to be split. Is assumed to not be null
   * @return list of chunks
   */
  private List<Buffer> chunkMessage(Buffer buffer) {
    // Messages that fit into one frame do not need to be split
    if (buffer.length() < maxWebSocketFrameSize) {
      return Collections.singletonList(buffer);
    }

    // Larger messages need to be split into multiple chunks
    int offset = 0;
    List<Buffer> chunks = new ArrayList<>();
    while (offset < buffer.length()) {
      int sliceEnd = Math.min(buffer.length(), offset + maxWebSocketFrameSize);
      Buffer chunk = buffer.slice(offset, sliceEnd);
      chunks.add(chunk);
      offset += chunk.length();
    }
    return chunks;
  }

  /**
   * Creates WebSocketFrames from the provided chunks and writes them to the socket
   * @param chunks data for each frame. Each chunk must fit within {@link #maxWebSocketFrameSize}
   * @param initialFrameType The type of the initial frame in the websocket communication
   */
  private void writeChunksAsFrames(List<Buffer> chunks, FrameType initialFrameType) {
    // Send chunks one by one
    int chunkCount = chunks.size();
    for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
      Buffer chunk = chunks.get(chunkIndex);
      boolean isFirst = (chunkIndex == 0);
      boolean isFinal = (chunkIndex == chunkCount - 1);
      WebSocketFrame frame;
      if (isFirst || !supportsContinuation) {
        frame = new WebSocketFrameImpl(initialFrameType, chunk.getByteBuf(), isFinal);
      } else {
        frame = WebSocketFrame.continuationFrame(chunk, isFinal);
      }
      writeFrame(frame);
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

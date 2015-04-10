/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.flume.serialization;

import com.google.common.base.Charsets;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;


/**
* This class represents all inputstreams that do not natively support mark,
* reset, markPosition(position) and seek(position)
* <p/>This class makes the following assumptions:
* <ol>
*   <li>The underlying file is not changing while it is being read</li>
* </ol>
*
* <p/>The ability to {@link #reset()} is dependent on the underlying {@link
* org.apache.flume.serialization.PositionTracker} instance's durability semantics.
*/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ResettableGenericInputStream extends ResettableInputStream
    implements RemoteMarkable, LengthMeasurable {

  Logger logger = LoggerFactory.getLogger(ResettableGenericInputStream.class);

  public static final int DEFAULT_BUF_SIZE = 16384;

  private final PositionTracker tracker;
  private final CharBuffer charBuf;
  private final byte[] byteBuf;
  private final long fileSize;
  private final CharsetDecoder decoder;
  private long position;
  private long syncPosition;
  private int maxCharWidth;
  private long length;
  private ByteArrayOutputStream markedBuffer;
  private boolean marked;
  private ByteBuffer buf;
  private InputStream stream;
  private StreamCreator streamCreator;
  private long totalBytesRead;

  /**
   *
   * @param streamCreator
   *        Stream creator to read stream from
   *
   * @param tracker
   *        PositionTracker implementation to make offset position durable
   *
   * @param length
   *        total length of the stream to be read
   *
   * @throws java.io.IOException
   */
  public ResettableGenericInputStream(StreamCreator streamCreator,
                                      PositionTracker tracker, int length) throws IOException {
    this(streamCreator, tracker, DEFAULT_BUF_SIZE, Charsets.UTF_8, DecodeErrorPolicy.FAIL, length);
  }

  /**
   *
   * @param streamCreator
   *        Stream creator to read stream from
   *
   * @param tracker
   *        PositionTracker implementation to make offset position durable
   *
   * @param bufSize
   *        Size of the underlying buffer used for input
   *
   * @param charset
   *        Character set used for decoding text, as necessary
   *
   * @throws java.io.IOException
   */
  public ResettableGenericInputStream(StreamCreator streamCreator,
                                      PositionTracker tracker, int bufSize, Charset charset,
                                      DecodeErrorPolicy policy, long length) throws IOException {
    this.tracker = tracker;
    this.streamCreator = streamCreator;
    this.stream = streamCreator.create();
    this.buf = ByteBuffer.wrap(new byte[bufSize]);
    printBufferState("Initial Buffer State");
    buf.flip();
    this.byteBuf = new byte[1]; // single byte
    this.charBuf = CharBuffer.allocate(1); // single char
    charBuf.flip();
    this.fileSize = length;
    this.decoder = charset.newDecoder();
    this.position = 0;
    this.maxCharWidth = (int)Math.ceil(charset.newEncoder().maxBytesPerChar());
    this.length = length;
    this.markedBuffer = new ByteArrayOutputStream();
    this.totalBytesRead = 0;

    CodingErrorAction errorAction;
    switch (policy) {
      case FAIL:
        errorAction = CodingErrorAction.REPORT;
        break;
      case REPLACE:
        errorAction = CodingErrorAction.REPLACE;
        break;
      case IGNORE:
        errorAction = CodingErrorAction.IGNORE;
        break;
      default:
        throw new IllegalArgumentException(
            "Unexpected value for decode error policy: " + policy);
    }
    decoder.onMalformedInput(errorAction);
    decoder.onUnmappableCharacter(errorAction);
    logger.debug("ResettableGenericInputStream Starting on File - "
            + tracker.getTarget() + "@ Position - " + tracker.getPosition());
    seek(tracker.getPosition());
  }

  @Override
  public synchronized int read() throws IOException {
    int len = read(byteBuf, 0, 1);
    if (len == -1) {
      return -1;
    // len == 0 should never happen
    } else if (len == 0) {
      return -1;
    } else {
      return byteBuf[0];
    }
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    logger.trace("read(buf, {}, {})", off, len);

    if (position >= fileSize) {
      return -1;
    }
    if (!buf.hasRemaining()) {
      refillBuf();
    }
    int rem = buf.remaining();
    if (len > rem) {
      len = rem;
    }
    buf.get(b, off, len);
    incrPosition(len, true);
    return len;
  }

  @Override
  public synchronized int readChar() throws IOException {
    // The decoder can have issues with multi-byte characters.
    // This check ensures that there are at least maxCharWidth bytes in the buffer
    // before reaching EOF.
    if (buf.remaining() < maxCharWidth) {
      logger.debug("buf.remaining() < maxCharWidth  "+ buf.remaining() +" - "+ maxCharWidth);
      refillBuf();
    }

    int start = buf.position();
    charBuf.clear();

    boolean isEndOfInput = false;
    if (position >= fileSize) {
      isEndOfInput = true;
    }

    CoderResult res = decoder.decode(buf, charBuf, isEndOfInput);
    if (res.isMalformed() || res.isUnmappable()) {
      res.throwException();
    }

    int delta = buf.position() - start;

    charBuf.flip();
    if (charBuf.hasRemaining()) {
      char c = charBuf.get();
      // don't increment the persisted location if we are in between a
      // surrogate pair, otherwise we may never recover if we seek() to this
      // location!
      incrPosition(delta, !Character.isHighSurrogate(c));
      return c;

    // there may be a partial character in the decoder buffer
    } else {
      incrPosition(delta, false);
      return -1;
    }

  }

  private void printBufferState(String str) {
    if(str != null && !str.isEmpty()) {
      logger.debug(str);
    }
    logger.debug("Current position @ "+ this.position);
    logger.debug("Buffer Stats: " +
            " Position - " + buf.position() +
            " Limit - " + buf.limit() +
            " Capacity - " + buf.capacity() +
            " Remaining: "+buf.hasRemaining());
  }

  private void refillBuf() throws IOException {
    printBufferState("Entering refillBuf() ");
    int currPos = buf.position();
    int remaining = buf.remaining();

    // there is a mark, and some data is present in the buf
    // that needs to be stored as part of the MarkedBuffer
    if(marked && currPos > 0) {
      int initMarkedBufSize = markedBuffer.size();
      byte[] processedBuf = new byte[currPos];
      buf.rewind();
      buf.get(processedBuf);
      assert buf.position() == currPos;
      markedBuffer.write(processedBuf);
      assert markedBuffer.size() == initMarkedBufSize + currPos;
    }

    // data yet to be processed is moved to front, and the remaining space
    // is made ready to be filled
    buf.compact();

    if(!buf.hasArray())
      throw new IllegalStateException("Byte buffer should be backed by array");

    printBufferState("Before filling buffer");
    // (buf.capacity() - buf.position()) same as buf.remaining(), when limit == capacity
    logger.debug("About to read - " + (buf.capacity() - buf.position()));
    int bytesRead = stream.read(buf.array(), buf.position(), buf.capacity() - buf.position());
    this.totalBytesRead += bytesRead;
    printBufferState("After filling buffer");
    logger.debug("totalBytesRead - " + totalBytesRead + " Current bytesRead - "+bytesRead);

    //If nothing was read
    if(bytesRead <= 0) {
      logger.warn("refillBuf ended up reading 0 bytes");
      buf.flip();
      return;
    }

    // prepare the buffer for consuming
    buf.clear();
    buf.position(remaining + bytesRead);
    buf.flip();
    printBufferState("Returning from refillBuf");
  }

  @Override
  public void mark() throws IOException {
    logger.debug("Mark() called " + tell());
    // Always mark the syncPosition, which is safe to be reset to
    mark(tell());
  }

  private void mark(long position) throws IOException {
    logger.debug("Mark(position) called " + position);
    tracker.storePosition(position);

    // Discard all data in the MarkBuffer and processed data before buf.position()
    markedBuffer.reset();
    // Move the unprocessed data to the beginning
    buf.compact();
    // Move the position to 0, to start reading the unprocessed data
    buf.flip();
    marked = true;
    logger.debug("Marked @ "+position);
  }

  @Override
  public void reset() throws IOException {
    logger.debug("Reset called - marked is - " + marked);
    if(!marked) {
      logger.warn("reset() called when marked is set to false " +
              "seeking to  tracker.getPosition() - " + tracker.getPosition());
      seek(tracker.getPosition());
      return;
    }
    marked = false;

    if(markedBuffer.size() == 0) {
      // make sure mark was placed in the current buffer
      assert tracker.getPosition() >= position - buf.position() &&
              tracker.getPosition() < position;
      buf.position((int)(tracker.getPosition() - (position - buf.position())));
    } else {
      // reset called with data in the MarkedBuffer
      logger.debug("Reset called - markedBuffer.size() != 0");
      assert tracker.getPosition() + markedBuffer.size() + buf.position() == position;
      buf.rewind();
      byte[] tempBuf = new byte[markedBuffer.size() + buf.remaining()];
      ByteBuffer newBuf = ByteBuffer.wrap(tempBuf);
      newBuf.put(markedBuffer.toByteArray()).put(buf);
      assert (!newBuf.hasRemaining());
      newBuf.flip();
      assert (newBuf.remaining() == newBuf.limit());
      buf = newBuf;
      markedBuffer.reset();
    }

    // clear decoder state
    decoder.reset();
    position = syncPosition = tracker.getPosition();
  }

  @Override
  public long length() throws IOException {
    return length;
  }

  @Override
  public long tell() throws IOException {
    logger.trace("Tell position: {}", syncPosition);
    return syncPosition;
  }

  @Override
  public synchronized void seek(long newPos) throws IOException {
    logger.debug("Seek to position: {}", newPos);

    // check to see if we can seek within our existing buffer
    long relativeChange = newPos - position;
    if (relativeChange == 0) return; // seek to current pos => no-op

    long newBufPos = buf.position() + relativeChange;
    if (newBufPos >= 0 && newBufPos < buf.limit()) {
      // we can reuse the read buffer
      buf.position((int)newBufPos);
    } else if(marked && newPos == tracker.getPosition()) {
      reset();
    } else {
      //if(seek position is beyond what we have read so far)
      if(newPos > totalBytesRead) {
        buf.rewind();
        markedBuffer.write(buf.array(), 0, buf.limit());
        int bytesToSkip = (int) (newPos - totalBytesRead);
        byte[] skippedBytes = new byte[bytesToSkip];
        stream.read(skippedBytes, 0, bytesToSkip);
        totalBytesRead += bytesToSkip;
        markedBuffer.write(skippedBytes);
        buf.clear();
        buf.flip();
        logger.debug("Skipping bytes - "+ bytesToSkip + " length is - " +
                this.length + " totalbytesRead - " + totalBytesRead);
      } else {
        // if(seek position is in the marked buffer outputstream or even before that)

        long markedBufPos = relativeChange + markedBuffer.size() + buf.position();
        if(markedBufPos > 0) {
          //seek position is in the marked buffer outputstream
          //TODO: Unit tests do not test this portion
          logger.debug("Seek to position: when markedBufPos > 0 - " + newPos);
          buf.rewind();
          byte[] tempBuf = new byte[(int)(markedBuffer.size() - markedBufPos) + buf.remaining()];
          ByteBuffer newBuf = ByteBuffer.wrap(tempBuf);
          byte[] markedArr = markedBuffer.toByteArray();
          newBuf.put(markedArr, (int)markedBufPos, (int)(markedBuffer.size() - markedBufPos)).put(buf);
          assert (!newBuf.hasRemaining());
          newBuf.flip();
          assert (newBuf.remaining() == newBuf.limit());

          buf = newBuf;
          markedBuffer.reset();
          markedBuffer.write(markedArr, 0, (int)markedBufPos);
        } else {
          //seeking to position before the currently marked position
          logger.warn("Seek leads to creation of new stream");
          stream.close();
          markedBuffer.reset();
          buf.clear();
          buf.flip();
          assert buf.remaining() == 0;
          stream = streamCreator.create();
          stream.skip(newPos);
          totalBytesRead = newPos;
        }
      }
    }

    // clear decoder state
    decoder.reset();

    // reset position pointers
    position = syncPosition = newPos;
    logger.debug("After seeking to newPos - "+ position);
  }

  private void incrPosition(int incr, boolean updateSyncPosition) {
    position += incr;
    if (updateSyncPosition) {
      syncPosition = position;
    }
  }

  @Override
  public void close() throws IOException {
    tracker.close();
    stream.close();
  }

  // markedBuffer.size() != 0 part of reset() and reset() part of seek()
  // and markPsoition(position) needs test cases

  /*
   * markPosition and getMarkPosition are supported as part of
   * RemoteMarkable interface used by AvroEventDeserializer
   *
   * These two methods need to be tested by using Avro Events
   *
   */
  @Override
  public void markPosition(long markPosition) throws IOException {
    logger.debug("MarkPosition(markPosition) called " + markPosition);
    long savedPosition = buf.position();
    int relMarkPos = (int)(savedPosition - (position - markPosition));

    // current sync position
    if(markPosition == tell()) {
      mark();
    }
    // current position
    else if (markPosition == position) {
      mark(position);
    }
    // future position = error
    else if(markPosition > position) {
      //no-op
      logger.warn(" Invalid case reached - markPosition > position  " +
              "- markPosition - "+ markPosition + " position - " + position);
    }
    // within current buf
    else if(markPosition >= position - buf.position()) {
      buf.position(relMarkPos);
      mark(markPosition);
    }
    // within outputStream
    else if(markedBuffer.size() + relMarkPos >= 0) {
      int newBufPos = markedBuffer.size() + relMarkPos;
      byte[] newBuf = markedBuffer.toByteArray();
      markedBuffer.reset();
      markedBuffer.write(newBuf, newBufPos, markedBuffer.size() - newBufPos);
      tracker.storePosition(markPosition);
      marked = true;
    }
    // past position, new stream, skip to mark position ,
    // copy everything from there to position to outputstream,
    else {
      logger.warn("Trying to mark a past position - should not reach here");
      stream.close();
      markedBuffer.reset();

      // move to the starting of mark
      assert buf.remaining() == 0;
      stream = streamCreator.create();
      stream.skip(markPosition);

      // copy the bytes not in the current byte buffer
      int bytesToCopy = (int) totalBytesRead - buf.limit();
      byte[] tempBuf = new byte[bytesToCopy];
      stream.read(tempBuf, 0, bytesToCopy);
      markedBuffer.write(tempBuf);

      // skip to where it was in the orginal stream
      stream.skip(totalBytesRead - markPosition);
    }
    logger.debug("Storing tracker position - Mark Position - " + markPosition);
  }

  @Override
  public long getMarkPosition() throws IOException {
    return tracker.getPosition();
  }

}

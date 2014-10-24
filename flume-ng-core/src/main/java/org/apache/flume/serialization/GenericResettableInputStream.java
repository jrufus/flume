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
public class GenericResettableInputStream extends ResettableInputStream
    implements RemoteMarkable, LengthMeasurable {

  Logger logger = LoggerFactory.getLogger(GenericResettableInputStream.class);

  public static final int DEFAULT_BUF_SIZE = 16384;

  private final PositionTracker tracker;
  private final CharBuffer charBuf;
  private final byte[] byteBuf;
  private final long fileSize;
  private final CharsetDecoder decoder;
  private long position;
  private long syncPosition;
  private int maxCharWidth;
  private int length;
  private ByteArrayOutputStream markedBuffer;
  private boolean marked;
  private ByteBuffer buf;
  private InputStream in;
  private StreamCreator streamCreator;

  /**
   *
   * @param streamCreator
   *        Stream to read
   *
   * @param tracker
   *        PositionTracker implementation to make offset position durable
   *
   * @throws java.io.FileNotFoundException
   */

  //TODO: Constructor should pass a new class object, that knows how to iniitialize and construt itself again, returning a new InputStream - eg : S3Stream.create()
  public GenericResettableInputStream(StreamCreator streamCreator, PositionTracker tracker, int length)
      throws IOException {
    this(streamCreator, tracker, DEFAULT_BUF_SIZE, Charsets.UTF_8, DecodeErrorPolicy.FAIL, length);
  }

  /**
   *
   * @param streamCreator
   *        Stream to read
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
   * @throws java.io.FileNotFoundException
   */
  public GenericResettableInputStream(StreamCreator streamCreator, PositionTracker tracker,
                                      int bufSize, Charset charset, DecodeErrorPolicy decodeErrorPolicy, int length)
      throws IOException {
    this.tracker = tracker;
    this.streamCreator = streamCreator;
    this.in = streamCreator.create();
    this.buf = ByteBuffer.wrap(new byte[bufSize]);
    System.out.println("processedSize ----"+buf.position()+"  "+buf.hasRemaining()+" --- "+buf.capacity()+"  "+buf.limit());
    buf.flip();
    this.byteBuf = new byte[1]; // single byte
    this.charBuf = CharBuffer.allocate(1); // single char
    charBuf.flip();
    this.fileSize = length;
    this.decoder = charset.newDecoder();
    this.position = 0;
    this.syncPosition = 0;
    this.maxCharWidth = (int)Math.ceil(charset.newEncoder().maxBytesPerChar());
    this.length = length;
    this.markedBuffer = new ByteArrayOutputStream();
    //this.marker = 0;

    CodingErrorAction errorAction;
    switch (decodeErrorPolicy) {
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
            "Unexpected value for decode error policy: " + decodeErrorPolicy);
    }
    decoder.onMalformedInput(errorAction);
    decoder.onUnmappableCharacter(errorAction);

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
      System.out.println("buf.remaining() < maxCharWidth  "+ buf.remaining() +" --- "+ maxCharWidth);
      //buf.clear();
      //buf.flip();
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

  private void refillBuf() throws IOException {
    //int prevSize = buf.remaining();
    System.out.println(" refillBuf() starting  --- "+buf.hasRemaining()+" --- "+buf.capacity()+"  "+buf.limit()+"  -- "+buf.position());
    int currPos = buf.position();
    int currLimit = buf.limit();
    int remaining = buf.remaining();
    assert buf.remaining() == currLimit - currPos;

    //int processedSize = buf.remaining() - prevSize;



    if(marked && currPos > 0) {
      byte[] processedBuf = new byte[currPos];
      buf.rewind();
      buf.get(processedBuf);
      assert buf.position() == currPos;
      markedBuffer.write(processedBuf);
    }
    //buf.position(currPos); TODO use this if assert buf.position() == currPos; above fails
    buf.compact();

    if(!buf.hasArray())
      throw new IllegalStateException("Byte buffer should be backed by ARRAY");
    System.out.println("processedSize ----"+currPos+"  "+buf.hasRemaining()+" --- "+buf.capacity()+"  "+buf.limit()+"  -- "+buf.position());

    int bytesRead = in.read(buf.array(), buf.position(), buf.capacity() - buf.position());

    System.out.println("bytesREAd ----"+bytesRead+ " --- "+buf.hasRemaining()+" --- "+buf.capacity()+"  "+buf.limit()+"  -- "+buf.position());
    if(bytesRead < 0 || bytesRead == 0) {
      buf.flip();
      return;
    }


//  buf.put(tempBuf, 0, bytesRead);
    buf.clear();
    buf.position(remaining + bytesRead);
    buf.flip();
    System.out.println("bytesREAd ----"+bytesRead+ " --- "+buf.hasRemaining()+" --- "+buf.capacity()+"  "+buf.limit()+"  -- "+buf.position());
    //TODO: after filling do flip()
//    chan.position(position); // ensure we read from the proper offset
//    chan.read(buf);
//    buf.flip();
  }

  @Override
  public void mark() throws IOException {
    //tracker.storePosition(tell());
    tracker.storePosition(tell());
    markedBuffer.reset();
    buf.compact();
    buf.flip();
    marked = true;
  }

  @Override // TODO: see where the position is
  public void markPosition(long markPosition) throws IOException {
    long savedPosition = buf.position();
    int relMarkPos = (int)(savedPosition - (position - markPosition));

    if (markPosition == position) { // current position
      mark();
      return;
    } else if(markPosition > position) { // future position = error // RARE
      System.out.println("------------------------ RARE CASE REACHED ------------mark position-----to future-------------");
      return;
    } else if(markPosition >= position - buf.position()) { // within buf //TODO: Verify >=
      buf.position(relMarkPos);
      buf.compact();
      //buf.position(buf.capacity() - relMarkPos); //TODO: verify this
      buf.flip();

      buf.position((int)savedPosition - relMarkPos);
      marked = true;
    } else if(markedBuffer.size() + relMarkPos >= 0) { // within outputStream
      int newBufPos = markedBuffer.size() + relMarkPos;
      byte[] newBuf = new byte[relMarkPos];
      markedBuffer.write(newBuf, markedBuffer.size() - relMarkPos, relMarkPos);
      markedBuffer = new ByteArrayOutputStream();
      markedBuffer.write(newBuf);
    } else { // past position, new stream, skipt to mark position , copy everything from there to position to outputstream, compact the buf// RARE
      //TODO:
    }

    tracker.storePosition(markPosition);
  }

  @Override
  public long getMarkPosition() throws IOException {
    return tracker.getPosition();
  }

  @Override
  public void reset() throws IOException {

    if(!marked) {
      seek(tracker.getPosition());
      return;
    }
    marked = false;
    buf.rewind();
    if(markedBuffer.size() == 0)  return;

    ByteBuffer newBuf = ByteBuffer.allocate(markedBuffer.size() + buf.remaining());
    newBuf.put(markedBuffer.toByteArray()).put(buf);
    assert (!newBuf.hasRemaining());
    newBuf.flip();
    assert (newBuf.remaining() == newBuf.limit());
    buf = newBuf;
    markedBuffer.reset();

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
    logger.trace("Seek to position: {}", newPos);

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
      buf.clear();
      buf.flip();
      if(newPos > position) {
        markedBuffer.reset();
        in.skip(position - newPos);
      } else {
        //TODO: if(seek position is in the marked buffer outputstream)
        long markedBufPos = relativeChange + markedBuffer.size() + buf.position();
        if(markedBufPos > 0) {
          buf.rewind();
          ByteBuffer newBuf = ByteBuffer.allocate(markedBuffer.size() + buf.remaining());
          byte[] markedArr = markedBuffer.toByteArray();
          newBuf.put(markedArr, (int)markedBufPos, (int)(markedBuffer.size() - markedBufPos));
          assert (!newBuf.hasRemaining());
          newBuf.flip();
          assert (newBuf.remaining() == newBuf.limit());

          buf = newBuf;
          markedBuffer.reset();
          markedBuffer.write(markedArr, 0, (int)markedBufPos);
        } else {
          //should be a v rare case
          //TODO : logic for closing inputStream, wipe buffer, wipe outputstream and reopen stream and seek
          in.close();
          markedBuffer.reset();
          buf.clear();
          buf.flip();
          assert buf.remaining() == 0;
          in = streamCreator.create();
          in.skip(newPos);
          System.out.println("------------------------ RARE CASE REACHED ------------------------------");
        }
      }
    }

    // clear decoder state
    decoder.reset();

    // perform underlying file seek

    //chan.position(newPos);

    // reset position pointers
    position = syncPosition = newPos;
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
    in.close();
  }

  public static void main(String[] args) {

//    File f = new File();
//    GenericResettableInputStream is = new GenericResettableInputStream(stream, tracker, length, bufSize, charset, decodeErrorPolicy, length);
  }

}

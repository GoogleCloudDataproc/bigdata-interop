/*
 * Copyright 2020 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.BackOffFactory;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hashing;
import com.google.google.storage.v1.GetObjectMediaRequest;
import com.google.google.storage.v1.GetObjectMediaResponse;
import com.google.google.storage.v1.GetObjectRequest;
import com.google.google.storage.v1.StorageGrpc;
import com.google.google.storage.v1.StorageGrpc.StorageBlockingStub;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Iterator;
import javax.annotation.Nullable;

public class GoogleCloudStorageGrpcReadChannel implements SeekableByteChannel {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // ZeroCopy version of GetObjectMedia Method
  private static final ZeroCopyMessageMarshaller getObjectMediaResponseMarshaller =
    new ZeroCopyMessageMarshaller(GetObjectMediaResponse.getDefaultInstance());
  private static final MethodDescriptor<GetObjectMediaRequest, GetObjectMediaResponse> getObjectMediaMethod =
    StorageGrpc.getGetObjectMediaMethod()
      .toBuilder()
      .setResponseMarshaller(getObjectMediaResponseMarshaller)
      .build();
  private static final boolean useZeroCopyMarshaller = ZeroCopyReadinessChecker.isReady();

  private volatile StorageBlockingStub stub;

  private final StorageStubProvider stubProvider;

  private final StorageResourceId resourceId;

  // We read from a specific generation, to maintain consistency between read() calls.
  private final long objectGeneration;

  // The size of this object generation, in bytes.
  private final long objectSize;

  // True if this channel is open, false otherwise.
  private boolean channelIsOpen = true;

  // Current position in the object.
  private long position = 0;

  // If a user seeks forwards by a configurably small amount, we continue reading from where
  // we are instead of starting a new connection. The user's intended read position is
  // position + bytesToSkipBeforeReading.
  private long bytesToSkipBeforeReading = 0;

  // The user may have read less data than we received from the server. If that's the case, we keep
  // the most recently received content and a reference to how much of it we've returned so far.
  @Nullable private ByteString bufferedContent = null;

  private int bufferedContentReadOffset = 0;

  // InputStream that backs bufferedContent. This needs to be closed when bufferedContent is no longer needed.
  @Nullable private InputStream streamForBufferedContent = null;

  // The streaming read operation. If null, there is not an in-flight read in progress.
  @Nullable private Iterator<GetObjectMediaResponse> resIterator = null;

  // Fine-grained options.
  private final GoogleCloudStorageReadOptions readOptions;

  private final BackOffFactory backOffFactory;

  // Context of the request that returned resIterator.
  @Nullable CancellableContext requestContext;

  Fadvise readStrategy;

  public static GoogleCloudStorageGrpcReadChannel open(
      StorageStubProvider stubProvider,
      StorageResourceId resourceId,
      GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return open(stubProvider, resourceId, readOptions, BackOffFactory.DEFAULT);
  }

  @VisibleForTesting
  static GoogleCloudStorageGrpcReadChannel open(
      StorageStubProvider stubProvider,
      StorageResourceId resourceId,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory)
      throws IOException {
    // The gRPC API's GetObjectMedia call does not provide a generation number, so to ensure
    // consistent reads, we need to begin by checking the current generation number with a separate
    // call.
    // TODO(b/135138893): We can avoid this call by adding metadata to a read request.
    //      That will save about 40ms per read.
    // TODO(b/136088557): If we add metadata to a read, we can also use that first call to
    //      implement footer prefetch.
    try {
      return ResilientOperation.retry(
          () -> {
            StorageBlockingStub stub = stubProvider.newBlockingStub();
            com.google.google.storage.v1.Object storageObject;
            try {
              // TODO(b/151184800): Implement per-message timeout, in addition to stream timeout.
              storageObject =
                  stub.withDeadlineAfter(readOptions.getGrpcReadMetadataTimeoutMillis(), MILLISECONDS)
                      .getObject(
                          GetObjectRequest.newBuilder()
                              .setBucket(resourceId.getBucketName())
                              .setObject(resourceId.getObjectName())
                              .build());
            } catch (StatusRuntimeException e) {
              throw convertError(e, resourceId);
            }
            // The non-gRPC read channel has special support for gzip. This channel doesn't
            // decompress gzip-encoded objects on the fly, so best to fail fast rather than return
            // gibberish unexpectedly.
            if (storageObject.getContentEncoding().contains("gzip")) {
              throw new IOException(
                  "Can't read GZIP encoded files - content encoding support is disabled.");
            }

            return new GoogleCloudStorageGrpcReadChannel(
                stub,
                stubProvider,
                resourceId,
                storageObject.getGeneration(),
                storageObject.getSize(),
                readOptions,
                backOffFactory);
          },
          backOffFactory.newBackOff(),
          RetryDeterminer.ALL_ERRORS,
          IOException.class);
    } catch (Exception e) {
      throw new IOException(String.format("Error reading '%s'", resourceId), e);
    }
  }

  private GoogleCloudStorageGrpcReadChannel(
      StorageBlockingStub gcsGrpcBlockingStub,
      StorageStubProvider stubProvider,
      StorageResourceId resourceId,
      long objectGeneration,
      long objectSize,
      GoogleCloudStorageReadOptions readOptions,
      BackOffFactory backOffFactory) {
    this.stub = gcsGrpcBlockingStub;
    this.stubProvider = stubProvider;
    this.resourceId = resourceId;
    this.objectGeneration = objectGeneration;
    this.objectSize = objectSize;
    this.readOptions = readOptions;
    this.backOffFactory = backOffFactory;
    this.readStrategy = readOptions.getFadvise();
  }

  private static IOException convertError(
      StatusRuntimeException error, StorageResourceId resourceId) {
    String msg = String.format("Error reading '%s'", resourceId);
    switch (Status.fromThrowable(error).getCode()) {
      case NOT_FOUND:
        return GoogleCloudStorageExceptions.createFileNotFoundException(
            resourceId.getBucketName(), resourceId.getObjectName(), new IOException(msg, error));
      case OUT_OF_RANGE:
        return (IOException) new EOFException(msg).initCause(error);
      default:
        return new IOException(msg, error);
    }
  }

  /** Writes part of a ByteString into a ByteBuffer with as little copying as possible */
  private static void put(ByteString source, int offset, int size, ByteBuffer dest) {
    ByteString croppedSource = source.substring(offset, offset + size);
    for (ByteBuffer sourcePiece : croppedSource.asReadOnlyByteBufferList()) {
      dest.put(sourcePiece);
    }
  }

  private int readBufferedContentInto(ByteBuffer byteBuffer) {
    // Handle skipping forward through the buffer for a seek.
    long bufferSkip =
        min(bufferedContent.size() - bufferedContentReadOffset, bytesToSkipBeforeReading);
    bufferSkip = max(0, bufferSkip);
    bufferedContentReadOffset += bufferSkip;
    bytesToSkipBeforeReading -= bufferSkip;
    int remainingBufferedBytes = bufferedContent.size() - bufferedContentReadOffset;

    boolean remainingBufferedContentLargerThanByteBuffer =
        remainingBufferedBytes > byteBuffer.remaining();
    int bytesToWrite =
        remainingBufferedContentLargerThanByteBuffer
            ? byteBuffer.remaining()
            : remainingBufferedBytes;
    put(bufferedContent, bufferedContentReadOffset, bytesToWrite, byteBuffer);
    position += bytesToWrite;

    if (remainingBufferedContentLargerThanByteBuffer) {
      bufferedContentReadOffset += bytesToWrite;
    } else {
      invalidateBufferedContent();
    }

    return bytesToWrite;
  }

  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    logger.atFiner().log(
        "GCS gRPC read request for up to %d bytes at offset %d from object '%s'",
        byteBuffer.remaining(), position(), resourceId);

    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    int bytesRead = 0;

    // The server responds in 2MB chunks, but the client can ask for less than that. We store the
    // remainder in bufferedContent and return pieces of that on the next read call (and flush
    // that buffer if there is a seek).
    if (bufferedContent != null) {
      bytesRead += readBufferedContentInto(byteBuffer);
    }
    if (!byteBuffer.hasRemaining()) {
      return bytesRead;
    }
    if (position == objectSize) {
      return bytesRead > 0 ? bytesRead : -1;
    }
    if (resIterator == null) {
      Integer bytesToRead =
          readStrategy == Fadvise.RANDOM
              ? max(byteBuffer.remaining(), readOptions.getMinRangeRequestSize())
              : null;
      requestObjectMedia(bytesToRead);
    }
    while (moreServerContent() && byteBuffer.hasRemaining()) {
      GetObjectMediaResponse res = resIterator.next();
      // When zero-copy mashaller is used, the stream that backs GetObjectMediaResponse
      // should be closed when the mssage is no longed needed so that all buffers in the
      // stream can be reclaimed. If zero-copy is not used, stream will be null.
      InputStream stream = getObjectMediaResponseMarshaller.popStream(res);
      ByteString content = res.getChecksummedData().getContent();
      if (bytesToSkipBeforeReading >= 0 && bytesToSkipBeforeReading < content.size()) {
        content = res.getChecksummedData().getContent().substring((int) bytesToSkipBeforeReading);
        position += bytesToSkipBeforeReading;
        bytesToSkipBeforeReading = 0;
      } else if (bytesToSkipBeforeReading >= content.size()) {
        position += content.size();
        bytesToSkipBeforeReading -= content.size();
        if (stream != null) {
          stream.close();
        }
        continue;
      }

      if (readOptions.isGrpcChecksumsEnabled() && res.getChecksummedData().hasCrc32C()) {
        // TODO: Concatenate all these hashes together and compare the result at the end.
        int calculatedChecksum =
            Hashing.crc32c().hashBytes(res.getChecksummedData().getContent().toByteArray()).asInt();
        int expectedChecksum = res.getChecksummedData().getCrc32C().getValue();
        if (calculatedChecksum != expectedChecksum) {
          if (stream != null) {
            stream.close();
          }
          throw new IOException(
              String.format(
                  "Message checksum (%s) didn't match expected checksum (%s) for '%s'",
                  expectedChecksum, calculatedChecksum, resourceId));
        }
      }

      boolean responseSizeLargerThanRemainingBuffer = content.size() > byteBuffer.remaining();
      int bytesToWrite =
          responseSizeLargerThanRemainingBuffer ? byteBuffer.remaining() : content.size();
      put(content, 0, bytesToWrite, byteBuffer);
      bytesRead += bytesToWrite;
      position += bytesToWrite;

      if (responseSizeLargerThanRemainingBuffer) {
        invalidateBufferedContent();
        bufferedContent = content;
        bufferedContentReadOffset = bytesToWrite;
        streamForBufferedContent = stream;
      } else {
        if (stream != null) {
          stream.close();
        }
      }
    }
    return bytesRead;
  }

  private void requestObjectMedia(@Nullable Integer bytesToRead) throws IOException {
    GetObjectMediaRequest.Builder requestBuilder =
        GetObjectMediaRequest.newBuilder()
            .setBucket(resourceId.getBucketName())
            .setObject(resourceId.getObjectName())
            .setGeneration(objectGeneration)
            .setReadOffset(position);
    if (bytesToRead != null) {
      requestBuilder.setReadLimit(bytesToRead);
    }
    GetObjectMediaRequest request = requestBuilder.build();
    try {
      ResilientOperation.retry(
          () -> {
            try {
              requestContext = Context.current().withCancellation();
              Context toReattach = requestContext.attach();
              StorageBlockingStub blockingStub = stub.withDeadlineAfter(readOptions.getGrpcReadTimeoutMillis(), MILLISECONDS);
              try {
                if (useZeroCopyMarshaller) {
                  resIterator = io.grpc.stub.ClientCalls.blockingServerStreamingCall(
                    blockingStub.getChannel(), getObjectMediaMethod, blockingStub.getCallOptions(), request);
                } else {
                  resIterator = blockingStub.getObjectMedia(request);
                }
              } finally {
                requestContext.detach(toReattach);
              }
            } catch (StatusRuntimeException e) {
              recreateStub(e);
              throw convertError(e, resourceId);
            }
            return null;
          },
          backOffFactory.newBackOff(),
          RetryDeterminer.ALL_ERRORS,
          IOException.class);
    } catch (Exception e) {
      throw new IOException(String.format("Error reading '%s'", resourceId), e);
    }
  }

  private void cancelCurrentRequest() {
    if (requestContext != null) {
      requestContext.close();
      requestContext = null;
    }
    if (resIterator != null) {
      resIterator = null;
    }
  }

  /**
   * Waits until more data is available from the server, or returns false if read is done.
   *
   * @return true if more data is available with .next()
   * @throws IOException of the appropriate type if there was an I/O error.
   */
  private boolean moreServerContent() throws IOException {
    if (resIterator == null || requestContext == null || requestContext.isCancelled()) {
      return false;
    }

    try {
      return ResilientOperation.retry(
          () -> {
            try {
              boolean moreDataAvailable = resIterator.hasNext();
              if (!moreDataAvailable) {
                cancelCurrentRequest();
              }
              return moreDataAvailable;
            } catch (StatusRuntimeException e) {
              recreateStub(e);
              throw convertError(e, resourceId);
            }
          },
          backOffFactory.newBackOff(),
          RetryDeterminer.ALL_ERRORS,
          IOException.class);
    } catch (Exception e) {
      cancelCurrentRequest();
      throw new IOException(String.format("Error reading '%s'", resourceId), e);
    }
  }

  private void recreateStub(StatusRuntimeException e) {
    if (stubProvider.isStubBroken(Status.fromThrowable(e).getCode())) {
      stub = stubProvider.newBlockingStub();
    }
  }

  @Override
  public int write(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException("Cannot mutate read-only channel: " + this);
  }

  @Override
  public long position() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    // Our real position is tracked in "position," but if the user is skipping forwards a bit, we
    // pretend we're at the new position already.
    return position + bytesToSkipBeforeReading;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    Preconditions.checkArgument(
        newPosition >= 0, "Read position must be non-negative, but was %s", newPosition);
    Preconditions.checkArgument(
        newPosition < size(),
        "Read position must be before end of file (%s), but was %s",
        size(),
        newPosition);
    if (newPosition == position) {
      return this;
    }

    long seekDistance = newPosition - position;

    if (seekDistance >= 0 && seekDistance <= readOptions.getInplaceSeekLimit()) {
      bytesToSkipBeforeReading = seekDistance;
      return this;
    }

    if (readStrategy == Fadvise.AUTO) {
      if (seekDistance < 0 || seekDistance > readOptions.getInplaceSeekLimit()) {
        readStrategy = Fadvise.RANDOM;
      }
    }

    // Reset any ongoing read operations or local data caches.
    cancelCurrentRequest();
    invalidateBufferedContent();

    position = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return objectSize;
  }

  @Override
  public SeekableByteChannel truncate(long l) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return channelIsOpen;
  }

  @Override
  public void close() {
    cancelCurrentRequest();
    invalidateBufferedContent();
    channelIsOpen = false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("resourceId", resourceId)
        .add("generation", objectGeneration)
        .toString();
  }

  private void invalidateBufferedContent() {
    bufferedContent = null;
    bufferedContentReadOffset = 0;
    if (streamForBufferedContent != null) {
      try {
        streamForBufferedContent.close();
        streamForBufferedContent = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

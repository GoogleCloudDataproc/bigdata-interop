/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.concurrent.Executors.newFixedThreadPool;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationDelete;
import com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationRename;
import com.google.cloud.hadoop.util.CheckedFunction;
import com.google.cloud.hadoop.util.LazyExecutorService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Provides a POSIX like file system layered on top of Google Cloud Storage (GCS).
 *
 * <p>All file system aspects (eg, path) are encapsulated in this class, they are not exposed to the
 * underlying layer. That is, all interactions with the underlying layer are strictly in terms of
 * buckets and objects.
 *
 * @see <a
 *     href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/index.html">Hadoop
 *     FileSystem specification.</a>
 */
public class GoogleCloudStorageFileSystem {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // URI scheme for GCS.
  public static final String SCHEME = "gs";

  // URI of the root path.
  public static final URI GCS_ROOT = URI.create(SCHEME + ":/");

  private static final ThreadFactory DAEMON_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("gcsfs-thread-%d").setDaemon(true).build();

  private static final ListObjectOptions GET_FILE_INFO_LIST_OPTIONS =
      ListObjectOptions.DEFAULT.toBuilder().setIncludePrefix(true).setMaxResults(1).build();

  private static final ListObjectOptions LIST_FILE_INFO_LIST_OPTIONS =
      ListObjectOptions.DEFAULT.toBuilder().setIncludePrefix(true).build();

  public static final ListFileOptions DELETE_RENAME_LIST_OPTIONS =
      ListFileOptions.DEFAULT.toBuilder().setFields("bucket,name,generation").build();

  // GCS access instance.
  private GoogleCloudStorage gcs;

  // FS options
  private final GoogleCloudStorageFileSystemOptions options;

  /** Cached executor for asynchronous tasks. */
  private ExecutorService cachedExecutor = createCachedExecutor();

  /** Cached executor for synchronous tasks. */
  private ExecutorService lazyExecutor = new LazyExecutorService();

  // Comparator used for sorting paths.
  //
  // For some bulk operations, we need to operate on parent directories before
  // we operate on their children. To achieve this, we sort paths such that
  // shorter paths appear before longer paths. Also, we sort lexicographically
  // within paths of the same length (this is not strictly required but helps when
  // debugging/testing).
  @VisibleForTesting
  static final Comparator<URI> PATH_COMPARATOR =
      comparing(
          URI::toString,
          (as, bs) ->
              (as.length() == bs.length())
                  ? as.compareTo(bs)
                  : Integer.compare(as.length(), bs.length()));

  // Comparator used for sorting a collection of FileInfo items based on path comparison.
  @VisibleForTesting
  static final Comparator<FileInfo> FILE_INFO_PATH_COMPARATOR =
      comparing(FileInfo::getPath, PATH_COMPARATOR);

  /**
   * Constructs an instance of GoogleCloudStorageFileSystem.
   *
   * @param credential OAuth2 credential that allows access to GCS.
   * @param options Options for how this filesystem should operate and configure its underlying
   *     storage.
   * @throws IOException
   */
  public GoogleCloudStorageFileSystem(
      Credential credential, GoogleCloudStorageFileSystemOptions options) throws IOException {
    this(
        new GoogleCloudStorageImpl(
            checkNotNull(options, "options must not be null").getCloudStorageOptions(), credential),
        options);
    logger.atFiner().log("GoogleCloudStorageFileSystem(options: %s)", options);
  }

  /**
   * Constructs a GoogleCloudStorageFilesystem based on an already-configured underlying
   * GoogleCloudStorage {@code gcs}. Any options pertaining to GCS creation will be ignored.
   */
  @VisibleForTesting
  public GoogleCloudStorageFileSystem(
      CheckedFunction<GoogleCloudStorageOptions, GoogleCloudStorage, IOException> gcsFn,
      GoogleCloudStorageFileSystemOptions options)
      throws IOException {
    this(gcsFn.apply(options.getCloudStorageOptions()), options);
  }

  private GoogleCloudStorageFileSystem(
      GoogleCloudStorage gcs, GoogleCloudStorageFileSystemOptions options) {
    checkArgument(
        gcs.getOptions() == options.getCloudStorageOptions(),
        "gcs and gcsfs should use the same options");
    options.throwIfNotValid();
    this.gcs =
        options.isPerformanceCacheEnabled()
            ? new PerformanceCachingGoogleCloudStorage(gcs, options.getPerformanceCacheOptions())
            : gcs;
    this.options = options;
  }

  private static ExecutorService createCachedExecutor() {
    ThreadPoolExecutor service =
        new ThreadPoolExecutor(
            /* corePoolSize= */ 2,
            /* maximumPoolSize= */ Integer.MAX_VALUE,
            /* keepAliveTime= */ 30,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("gcsfs-misc-%d").setDaemon(true).build());
    service.allowCoreThreadTimeOut(true);
    // allowCoreThreadTimeOut needs to be enabled for cases where the encapsulating class does not
    return service;
  }

  /** Retrieve the options that were used to create this GoogleCloudStorageFileSystem. */
  public GoogleCloudStorageFileSystemOptions getOptions() {
    return options;
  }

  /** Convert {@code CreateFileOptions} to {@code CreateObjectOptions}. */
  public static CreateObjectOptions objectOptionsFromFileOptions(CreateFileOptions options) {
    return CreateObjectOptions.builder()
        .setContentType(options.getContentType())
        .setMetadata(options.getAttributes())
        .setOverwriteExisting(options.isOverwriteExisting())
        .build();
  }

  /**
   * Creates and opens an object for writing. If the object already exists, it is deleted.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for writing to the given object.
   */
  public WritableByteChannel create(URI path) throws IOException {
    logger.atFiner().log("create(path: %s)", path);
    return create(path, CreateFileOptions.DEFAULT_OVERWRITE);
  }

  /**
   * Creates and opens an object for writing.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for writing to the given object.
   */
  public WritableByteChannel create(URI path, CreateFileOptions createOptions) throws IOException {
    logger.atFiner().log("create(path: %s, createOptions: %s)", path, createOptions);
    Preconditions.checkNotNull(path, "path could not be null");
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName=*/ true);

    if (resourceId.isDirectory()) {
      throw new IOException(
          String.format(
              "Cannot create a file whose name looks like a directory: '%s'", resourceId));
    }

    // Because create call should create parent directories too, before creating an actual file
    // we need to check if there are no conflicting items in the directory tree:
    // - if there are no conflicting files with the same name as any parent subdirectory
    // - if there are no conflicting directory with the name as a file
    //
    // For example, for a new `gs://bucket/c/d/f` file:
    // - files `gs://bucket/c` and `gs://bucket/c/d` should not exist
    // - directory `gs://bucket/c/d/f/` should not exist
    if (options.isEnsureNoConflictingItems()) {
      // Asynchronously check if a directory with the same name exists.
      StorageResourceId dirId = resourceId.toDirectoryId();
      Future<Boolean> conflictingDirExist =
          createOptions.isEnsureNoDirectoryConflict()
              ? cachedExecutor.submit(
                  () -> getFileInfoInternal(dirId, /* inferImplicitDirectories */ true).exists())
              : immediateFuture(false);

      checkNoFilesConflictingWithDirs(resourceId);

      // Check if a directory with the same name exists.
      if (getFromFuture(conflictingDirExist)) {
        throw new FileAlreadyExistsException("A directory with that name exists: " + path);
      }
    }

    if (createOptions.getOverwriteGenerationId() != StorageResourceId.UNKNOWN_GENERATION_ID) {
      resourceId =
          new StorageResourceId(
              resourceId.getBucketName(),
              resourceId.getObjectName(),
              createOptions.getOverwriteGenerationId());
    }

    return gcs.create(resourceId, objectOptionsFromFileOptions(createOptions));
  }

  /**
   * Opens an object for reading.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  public SeekableByteChannel open(URI path) throws IOException {
    return open(path, GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Opens an object for reading.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @param readOptions Fine-grained options for behaviors of retries, buffering, etc.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  public SeekableByteChannel open(URI path, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    logger.atFiner().log("open(path: %s, readOptions: %s)", path, readOptions);
    Preconditions.checkNotNull(path, "path should not be null");
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ false);
    checkArgument(!resourceId.isDirectory(), "Cannot open a directory for reading: %s", path);

    return gcs.open(resourceId, readOptions);
  }

  /**
   * Deletes one or more items indicated by the given path.
   *
   * <p>If path points to a directory:
   *
   * <ul>
   *   <li>if recursive is true, all items under that path are recursively deleted followed by
   *       deletion of the directory.
   *   <li>else,
   *       <ul>
   *         <li>the directory is deleted if it is empty,
   *         <li>else, an IOException is thrown.
   *       </ul>
   * </ul>
   *
   * <p>The recursive parameter is ignored for a file.
   *
   * @param path Path of the item to delete.
   * @param recursive If true, all sub-items are also deleted.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException
   */
  public void delete(URI path, boolean recursive) throws IOException {
    Preconditions.checkNotNull(path, "path can not be null");
    checkArgument(!path.equals(GCS_ROOT), "Cannot delete root path (%s)", path);
    logger.atFiner().log("delete(path: %s, recursive: %b)", path, recursive);

    FileInfo fileInfo = getFileInfo(path);
    if (!fileInfo.exists()) {
      throw new FileNotFoundException("Item not found: " + path);
    }

    Future<GoogleCloudStorageItemInfo> parentInfoFuture = null;
    if (options.getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled()) {
      StorageResourceId parentId =
          StorageResourceId.fromUriPath(UriPaths.getParentPath(path), true);
      parentInfoFuture =
          cachedExecutor.submit(
              () -> getFileInfoInternal(parentId, /* inferImplicitDirectories= */ false));
    }

    Optional<CoopLockOperationDelete> coopLockOp =
        options.isCooperativeLockingEnabled() && fileInfo.isDirectory()
            ? Optional.of(CoopLockOperationDelete.create(gcs, fileInfo.getPath()))
            : Optional.empty();
    coopLockOp.ifPresent(CoopLockOperationDelete::lock);

    List<FileInfo> itemsToDelete;
    // Delete sub-items if it is a directory.
    if (fileInfo.isDirectory()) {
      itemsToDelete =
          recursive
              ? listFileInfoForPrefix(fileInfo.getPath(), DELETE_RENAME_LIST_OPTIONS)
              // TODO: optimize by listing just one object instead of whole page
              //  (up to 1024 objects now)
              : listFileInfoForPrefixPage(
                      fileInfo.getPath(), DELETE_RENAME_LIST_OPTIONS, /* pageToken= */ null)
                  .getItems();
      if (!itemsToDelete.isEmpty() && !recursive) {
        throw new DirectoryNotEmptyException("Cannot delete a non-empty directory.");
      }
    } else {
      itemsToDelete = new ArrayList<>();
    }

    List<FileInfo> bucketsToDelete = new ArrayList<>();
    (fileInfo.getItemInfo().isBucket() ? bucketsToDelete : itemsToDelete).add(fileInfo);

    coopLockOp.ifPresent(o -> o.persistAndScheduleRenewal(itemsToDelete, bucketsToDelete));
    try {
      deleteInternal(itemsToDelete, bucketsToDelete);

      coopLockOp.ifPresent(CoopLockOperationDelete::unlock);
    } finally {
      coopLockOp.ifPresent(CoopLockOperationDelete::cancelRenewal);
    }

    repairImplicitDirectory(parentInfoFuture);
  }

  /** Deletes all items in the given path list followed by all bucket items. */
  private void deleteInternal(List<FileInfo> itemsToDelete, List<FileInfo> bucketsToDelete)
      throws IOException {
    // TODO(user): We might need to separate out children into separate batches from parents to
    // avoid deleting a parent before somehow failing to delete a child.

    // Delete children before their parents.
    //
    // Note: we modify the input list, which is ok for current usage.
    // We should make a copy in case that changes in future.
    itemsToDelete.sort(FILE_INFO_PATH_COMPARATOR.reversed());

    if (!itemsToDelete.isEmpty()) {
      List<StorageResourceId> objectsToDelete = new ArrayList<>(itemsToDelete.size());
      for (FileInfo fileInfo : itemsToDelete) {
        // TODO(b/110833109): populate generation ID in StorageResourceId when listing infos?
        if (!fileInfo.isInferredDirectory()) {
          objectsToDelete.add(
              new StorageResourceId(
                  fileInfo.getItemInfo().getBucketName(),
                  fileInfo.getItemInfo().getObjectName(),
                  fileInfo.getItemInfo().getContentGeneration()));
        }
      }
      gcs.deleteObjects(objectsToDelete);
    }

    if (!bucketsToDelete.isEmpty()) {
      List<String> bucketNames = new ArrayList<>(bucketsToDelete.size());
      for (FileInfo bucketInfo : bucketsToDelete) {
        bucketNames.add(bucketInfo.getItemInfo().getResourceId().getBucketName());
      }
      if (options.isBucketDeleteEnabled()) {
        gcs.deleteBuckets(bucketNames);
      } else {
        logger.atInfo().log(
            "Skipping deletion of buckets because enableBucketDelete is false: %s", bucketNames);
      }
    }
  }

  /**
   * Indicates whether the given item exists.
   *
   * @param path Path of the item to check.
   * @return true if the given item exists, false otherwise.
   * @throws IOException
   */
  public boolean exists(URI path) throws IOException {
    logger.atFiner().log("exists(path: %s)", path);
    return getFileInfo(path).exists();
  }

  /**
   * Creates a directory at the specified path. Also creates any parent directories as necessary.
   * Similar to 'mkdir -p' command.
   *
   * @param path Path of the directory to create.
   * @throws IOException
   */
  public void mkdirs(URI path) throws IOException {
    logger.atFiner().log("mkdirs(path: %s)", path);
    Preconditions.checkNotNull(path, "path should not be null");

    mkdirsInternal(StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ true));
  }

  private void mkdirsInternal(StorageResourceId resourceId) throws IOException {
    if (resourceId.isRoot()) {
      // GCS_ROOT directory always exists, no need to go through the rest of the method.
      return;
    }

    // In case path is a bucket we just attempt to create it without additional checks
    if (resourceId.isBucket()) {
      try {
        gcs.createBucket(resourceId.getBucketName());
      } catch (FileAlreadyExistsException e) {
        // This means that bucket already exist and we do not need to do anything.
        logger.atFiner().withCause(e).log(
            "mkdirs: %s already exists, ignoring creation failure", resourceId);
      }
      return;
    }

    resourceId = resourceId.toDirectoryId();

    // Before creating a leaf directory we need to check if there are no conflicting files
    // with the same name as any subdirectory
    if (options.isEnsureNoConflictingItems()) {
      checkNoFilesConflictingWithDirs(resourceId);
    }

    // Create only a leaf directory because subdirectories will be inferred
    // if leaf directory exists
    try {
      gcs.createEmptyObject(resourceId);
    } catch (FileAlreadyExistsException e) {
      // This means that directory object already exist and we do not need to do anything.
      logger.atFiner().withCause(e).log(
          "mkdirs: %s already exists, ignoring creation failure", resourceId);
    }
  }

  /**
   * Renames the given item's path.
   *
   * <p>The operation is disallowed if any of the following is true:
   *
   * <ul>
   *   <li>src equals GCS_ROOT
   *   <li>src is a file and dst equals GCS_ROOT
   *   <li>src does not exist
   *   <li>dst is a file that already exists
   *   <li>parent of the destination does not exist
   * </ul>
   *
   * <p>Otherwise, the expected behavior is as follows:
   *
   * <ul>
   *   <li>if src is a directory:
   *       <ul>
   *         <li>if dst is an existing file then disallowed
   *         <li>if dst is a directory then rename the directory
   *       </ul>
   *       <p>
   *   <li>if src is a file:
   *       <ul>
   *         <li>if dst is a file then rename the file.
   *         <li>if dst is a directory then similar to the previous case after appending src
   *             file-name to dst
   *       </ul>
   * </ul>
   *
   * <p>Note: This function is very expensive to call for directories that have many sub-items.
   *
   * @param src Path of the item to rename.
   * @param dst New path of the item.
   * @throws FileNotFoundException if src does not exist.
   */
  public void rename(URI src, URI dst) throws IOException {
    logger.atFiner().log("rename(src: %s, dst: %s)", src, dst);
    Preconditions.checkNotNull(src);
    Preconditions.checkNotNull(dst);
    checkArgument(!src.equals(GCS_ROOT), "Root path cannot be renamed.");

    // Parent of the destination path.
    URI dstParent = UriPaths.getParentPath(dst);

    // Obtain info on source, destination and destination-parent.
    List<URI> paths = new ArrayList<>();
    paths.add(src);
    paths.add(dst);
    if (dstParent != null) {
      // dstParent is null if dst is GCS_ROOT.
      paths.add(dstParent);
    }
    List<FileInfo> fileInfos = getFileInfos(paths);
    FileInfo srcInfo = fileInfos.get(0);
    FileInfo dstInfo = fileInfos.get(1);

    // Make sure paths match what getFileInfo() returned (it can add / at the end).
    src = srcInfo.getPath();
    dst = dstInfo.getPath();

    // Throw if the source file does not exist.
    if (!srcInfo.exists()) {
      throw new FileNotFoundException("Item not found: " + src);
    }

    Optional<CoopLockOperationRename> coopLockOp =
        options.isCooperativeLockingEnabled()
                && src.getAuthority().equals(dst.getAuthority())
                && srcInfo.isDirectory()
            ? Optional.of(CoopLockOperationRename.create(gcs, src, dst))
            : Optional.empty();
    coopLockOp.ifPresent(CoopLockOperationRename::lock);
    if (coopLockOp.isPresent()) {
      fileInfos = getFileInfos(paths);
      srcInfo = fileInfos.get(0);
      dstInfo = fileInfos.get(1);
      if (!srcInfo.exists()) {
        coopLockOp.ifPresent(CoopLockOperationRename::unlock);
        throw new FileNotFoundException("Item not found: " + src);
      }
      if (!srcInfo.isDirectory()) {
        coopLockOp.ifPresent(CoopLockOperationRename::unlock);
        coopLockOp = Optional.empty();
      }
    }

    FileInfo dstParentInfo = dstParent == null ? null : fileInfos.get(2);
    try {
      dst = getDstUri(srcInfo, dstInfo, dstParentInfo);
    } catch (IOException e) {
      coopLockOp.ifPresent(CoopLockOperationRename::unlock);
      throw e;
    }

    // if src and dst are equal then do nothing
    if (src.equals(dst)) {
      coopLockOp.ifPresent(CoopLockOperationRename::unlock);
      return;
    }

    Future<GoogleCloudStorageItemInfo> srcParentInfoFuture = null;
    if (options.getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled()) {
      StorageResourceId srcParentId =
          StorageResourceId.fromUriPath(
              UriPaths.getParentPath(src), /* allowEmptyObjectName= */ true);
      srcParentInfoFuture =
          cachedExecutor.submit(
              () -> getFileInfoInternal(srcParentId, /* inferImplicitDirectories= */ false));
    }

    if (srcInfo.isDirectory()) {
      renameDirectoryInternal(srcInfo, dst, coopLockOp);
    } else {
      coopLockOp.ifPresent(CoopLockOperationRename::unlock);

      StorageResourceId srcResourceId =
          StorageResourceId.fromUriPath(src, /* allowEmptyObjectName= */ true);
      StorageResourceId dstResourceId =
          StorageResourceId.fromUriPath(dst, /* allowEmptyObjectName= */ true);

      gcs.copy(
          srcResourceId.getBucketName(), ImmutableList.of(srcResourceId.getObjectName()),
          dstResourceId.getBucketName(), ImmutableList.of(dstResourceId.getObjectName()));

      // TODO(b/110833109): populate generation ID in StorageResourceId when getting info
      gcs.deleteObjects(
          ImmutableList.of(
              new StorageResourceId(
                  srcInfo.getItemInfo().getBucketName(),
                  srcInfo.getItemInfo().getObjectName(),
                  srcInfo.getItemInfo().getContentGeneration())));
    }

    repairImplicitDirectory(srcParentInfoFuture);
  }

  private URI getDstUri(FileInfo srcInfo, FileInfo dstInfo, @Nullable FileInfo dstParentInfo)
      throws IOException {
    URI src = srcInfo.getPath();
    URI dst = dstInfo.getPath();

    // Throw if src is a file and dst == GCS_ROOT
    if (!srcInfo.isDirectory() && dst.equals(GCS_ROOT)) {
      throw new IOException("A file cannot be created in root.");
    }

    // Throw if the destination is a file that already exists, and it's not a source file.
    if (dstInfo.exists() && !dstInfo.isDirectory() && (srcInfo.isDirectory() || !dst.equals(src))) {
      throw new IOException("Cannot overwrite an existing file: " + dst);
    }

    // Rename operation cannot be completed if parent of destination does not exist.
    if (dstParentInfo != null && !dstParentInfo.exists()) {
      throw new IOException(
          "Cannot rename because path does not exist: " + dstParentInfo.getPath());
    }

    // Leaf item of the source path.
    String srcItemName = getItemName(src);

    // Having taken care of the initial checks, apply the regular rules.
    // After applying the rules, we will be left with 2 paths such that:
    // -- either both are files or both are directories
    // -- src exists and dst leaf does not exist
    if (srcInfo.isDirectory()) {
      // -- if src is a directory
      //    -- dst is an existing file => disallowed
      //    -- dst is a directory => rename the directory.

      // The first case (dst is an existing file) is already checked earlier.
      // If the destination path looks like a file, make it look like a
      // directory path. This is because users often type 'mv foo bar'
      // rather than 'mv foo bar/'.
      if (!dstInfo.isDirectory()) {
        dst = UriPaths.toDirectory(dst);
      }

      // Throw if renaming directory to self - this is forbidden
      if (src.equals(dst)) {
        throw new IOException("Rename dir to self is forbidden");
      }

      URI dstRelativeToSrc = src.relativize(dst);
      // Throw if dst URI relative to src is not equal to dst,
      // because this means that src is a parent directory of dst
      // and src cannot be "renamed" to its subdirectory
      if (!dstRelativeToSrc.equals(dst)) {
        throw new IOException("Rename to subdir is forbidden");
      }

      if (dstInfo.exists()) {
        if (dst.equals(GCS_ROOT)) {
          dst =
              UriPaths.fromStringPathComponents(
                  srcItemName, /* objectName= */ null, /* allowEmptyObjectName= */ true);
        } else {
          dst = UriPaths.toDirectory(dst.resolve(srcItemName));
        }
      }
    } else {
      // -- src is a file
      //    -- dst is a file => rename the file.
      //    -- dst is a directory => similar to the previous case after
      //                             appending src file-name to dst

      if (dstInfo.isDirectory()) {
        if (!dstInfo.exists()) {
          throw new IOException("Cannot rename because path does not exist: " + dstInfo.getPath());
        } else {
          dst = dst.resolve(srcItemName);
        }
      }
    }

    return dst;
  }

  /**
   * Composes inputs into a single GCS object. This performs a GCS Compose. Objects will be composed
   * according to the order they appear in the input. The destination object, if already present,
   * will be overwritten. Sources and destination are assumed to be in the same bucket.
   *
   * @param sources the list of URIs to be composed
   * @param destination the resulting URI with composed sources
   * @param contentType content-type of the composed object
   * @throws IOException if the Compose operation was unsuccessful
   */
  public void compose(List<URI> sources, URI destination, String contentType) throws IOException {
    StorageResourceId destResource = StorageResourceId.fromStringPath(destination.toString());
    List<String> sourceObjects =
        Lists.transform(
            sources, uri -> StorageResourceId.fromStringPath(uri.toString()).getObjectName());
    gcs.compose(
        destResource.getBucketName(), sourceObjects, destResource.getObjectName(), contentType);
  }

  /**
   * Renames given directory without checking any parameters.
   *
   * <p>GCS does not support atomic renames therefore a rename is implemented as copying source
   * metadata to destination and then deleting source metadata. Note that only the metadata is
   * copied and not the content of any file.
   */
  private void renameDirectoryInternal(
      FileInfo srcInfo, URI dst, Optional<CoopLockOperationRename> coopLockOp) throws IOException {
    checkArgument(srcInfo.isDirectory(), "'%s' should be a directory", srcInfo);
    checkArgument(dst.toString().endsWith(PATH_DELIMITER), "'%s' should be a directory", dst);

    URI src = srcInfo.getPath();

    // Mapping from each src to its respective dst.
    // Sort src items so that parent directories appear before their children.
    // That allows us to copy parent directories before we copy their children.
    Map<FileInfo, URI> srcToDstItemNames = new TreeMap<>(FILE_INFO_PATH_COMPARATOR);
    Map<FileInfo, URI> srcToDstMarkerItemNames = new TreeMap<>(FILE_INFO_PATH_COMPARATOR);

    // List of individual paths to rename;
    // we will try to carry out the copies in this list's order.
    List<FileInfo> srcItemInfos = listFileInfoForPrefix(src, DELETE_RENAME_LIST_OPTIONS);

    // Create a list of sub-items to copy.
    Pattern markerFilePattern = options.getMarkerFilePattern();
    String prefix = src.toString();
    for (FileInfo srcItemInfo : srcItemInfos) {
      String relativeItemName = srcItemInfo.getPath().toString().substring(prefix.length());
      URI dstItemName = dst.resolve(relativeItemName);
      if (markerFilePattern != null && markerFilePattern.matcher(relativeItemName).matches()) {
        srcToDstMarkerItemNames.put(srcItemInfo, dstItemName);
      } else {
        srcToDstItemNames.put(srcItemInfo, dstItemName);
      }
    }

    coopLockOp.ifPresent(
        o -> o.persistAndScheduleRenewal(srcToDstItemNames, srcToDstMarkerItemNames));
    try {
      // Create the destination directory in case Cooperative Locking
      // is enabled - this is necessary because it uses parent directory
      // as a marker during recovery of failed rename operations.
      // It is not necessary to crete this directory explicitly in other cases,
      // because parent destination directory will be automatically inferred
      // when child objects will be renamed.
      if (coopLockOp.isPresent()) {
        mkdir(dst);
      }

      // First, copy all items except marker items
      copyInternal(srcToDstItemNames);
      // Finally, copy marker items (if any) to mark rename operation success
      copyInternal(srcToDstMarkerItemNames);

      coopLockOp.ifPresent(CoopLockOperationRename::checkpoint);

      List<FileInfo> bucketsToDelete = new ArrayList<>(1);
      List<FileInfo> srcItemsToDelete = new ArrayList<>(srcToDstItemNames.size() + 1);
      srcItemsToDelete.addAll(srcToDstItemNames.keySet());
      if (srcInfo.getItemInfo().isBucket()) {
        bucketsToDelete.add(srcInfo);
      } else {
        // If src is a directory then srcItemInfos does not contain its own name,
        // therefore add it to the list before we delete items in the list.
        srcItemsToDelete.add(srcInfo);
      }

      // First delete marker files from the src
      deleteInternal(new ArrayList<>(srcToDstMarkerItemNames.keySet()), new ArrayList<>());
      // Then delete rest of the items that we successfully copied.
      deleteInternal(srcItemsToDelete, bucketsToDelete);

      coopLockOp.ifPresent(CoopLockOperationRename::unlock);
    } finally {
      coopLockOp.ifPresent(CoopLockOperationRename::cancelRenewal);
    }
  }

  /** Copies items in given map that maps source items to destination items. */
  private void copyInternal(Map<FileInfo, URI> srcToDstItemNames) throws IOException {
    if (srcToDstItemNames.isEmpty()) {
      return;
    }

    String srcBucketName = null;
    String dstBucketName = null;
    List<String> srcObjectNames = new ArrayList<>(srcToDstItemNames.size());
    List<String> dstObjectNames = new ArrayList<>(srcToDstItemNames.size());

    // Prepare list of items to copy.
    for (Map.Entry<FileInfo, URI> srcToDstItemName : srcToDstItemNames.entrySet()) {
      StorageResourceId srcResourceId = srcToDstItemName.getKey().getItemInfo().getResourceId();
      srcBucketName = srcResourceId.getBucketName();
      String srcObjectName = srcResourceId.getObjectName();
      srcObjectNames.add(srcObjectName);

      StorageResourceId dstResourceId =
          StorageResourceId.fromUriPath(srcToDstItemName.getValue(), true);
      dstBucketName = dstResourceId.getBucketName();
      String dstObjectName = dstResourceId.getObjectName();
      dstObjectNames.add(dstObjectName);
    }

    // Perform copy.
    gcs.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  /**
   * Attempts to create the directory object explicitly for provided {@code infoFuture} if it
   * doesn't already exist as a directory object.
   */
  private void repairImplicitDirectory(Future<GoogleCloudStorageItemInfo> infoFuture)
      throws IOException {
    if (infoFuture == null) {
      return;
    }

    checkState(
        options.getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled(),
        "implicit directories auto repair should be enabled");

    GoogleCloudStorageItemInfo info = getFromFuture(infoFuture);
    StorageResourceId resourceId = info.getResourceId();
    logger.atFiner().log("repairImplicitDirectory(resourceId: %s)", resourceId);

    if (info.exists()
        || resourceId.isRoot()
        || resourceId.isBucket()
        || PATH_DELIMITER.equals(resourceId.getObjectName())) {
      return;
    }

    checkState(resourceId.isDirectory(), "'%s' should be a directory", resourceId);

    try {
      gcs.createEmptyObject(resourceId);
      logger.atInfo().log("Successfully repaired '%s' directory.", resourceId);
    } catch (IOException e) {
      logger.atWarning().withCause(e).log("Failed to repair '%s' directory", resourceId);
    }
  }

  static <T> T getFromFuture(Future<T> future) throws IOException {
    try {
      return future.get();
    } catch (ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException(
          String.format(
              "Failed to get result: %s", e instanceof ExecutionException ? e.getCause() : e),
          e);
    }
  }

  /**
   * Equivalent to a recursive listing of {@code prefix}, except that {@code prefix} doesn't have to
   * represent an actual object but can just be a partial prefix string. The 'authority' component
   * of the {@code prefix} <b>must</b> be the complete authority, however; we can only list prefixes
   * of <b>objects</b>, not buckets.
   *
   * @param prefix the prefix to use to list all matching objects.
   */
  public List<FileInfo> listFileInfoForPrefix(URI prefix) throws IOException {
    return listFileInfoForPrefix(prefix, ListFileOptions.DEFAULT);
  }

  /**
   * Equivalent to a recursive listing of {@code prefix}, except that {@code prefix} doesn't have to
   * represent an actual object but can just be a partial prefix string. The 'authority' component
   * of the {@code prefix} <b>must</b> be the complete authority, however; we can only list prefixes
   * of <b>objects</b>, not buckets.
   *
   * @param prefix the prefix to use to list all matching objects.
   */
  public List<FileInfo> listFileInfoForPrefix(URI prefix, ListFileOptions listOptions)
      throws IOException {
    logger.atFiner().log("listAllFileInfoForPrefix(prefix: %s)", prefix);
    StorageResourceId prefixId = getPrefixId(prefix);
    List<GoogleCloudStorageItemInfo> itemInfos =
        gcs.listObjectInfo(
            prefixId.getBucketName(),
            prefixId.getObjectName(),
            updateListObjectOptions(ListObjectOptions.DEFAULT_FLAT_LIST, listOptions));
    List<FileInfo> fileInfos = FileInfo.fromItemInfos(itemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  /**
   * Equivalent to {@link #listFileInfoForPrefix} but returns {@link FileInfo}s listed by single
   * request (1 page).
   *
   * @param prefix the prefix to use to list all matching objects.
   * @param pageToken the page token to list
   */
  public ListPage<FileInfo> listFileInfoForPrefixPage(URI prefix, String pageToken)
      throws IOException {
    return listFileInfoForPrefixPage(prefix, ListFileOptions.DEFAULT, pageToken);
  }

  /**
   * Equivalent to {@link #listFileInfoForPrefix} but returns {@link FileInfo}s listed by single
   * request (1 page).
   *
   * @param prefix the prefix to use to list all matching objects.
   * @param pageToken the page token to list
   */
  public ListPage<FileInfo> listFileInfoForPrefixPage(
          URI prefix, ListFileOptions listOptions, String pageToken) throws IOException {
    logger.atFiner().log(
            "listAllFileInfoForPrefixPage(prefix: %s, pageToken:%s)", prefix, pageToken);
    return listFileInfoForPrefixPage(prefix, listOptions, pageToken, true);
  }

  /**
   * Equivalent to {@link #listFileInfoForPrefixPage} but returns {@link FileInfo}s listed by single
   * request (1 page).
   *
   * @param prefix    the prefix to use to list all matching objects.
   * @param pageToken the page token to list
   * @param recursive the flag to scan recursively
   */
  public ListPage<FileInfo> listFileInfoForPrefixPage(
          URI prefix, String pageToken, boolean recursive) throws IOException {
    logger.atFiner().log(
            "listAllFileInfoForPrefixPage(prefix: %s, pageToken:%s)", prefix, pageToken);
    return listFileInfoForPrefixPage(prefix, ListFileOptions.DEFAULT, pageToken, recursive);
  }

  /**
   * Equivalent to {@link #listFileInfoForPrefixPage} but returns {@link FileInfo}s listed by single
   * request (1 page).
   *
   * @param prefix    the prefix to use to list all matching objects.
   * @param pageToken the page token to list
   * @param recursive the options to do recursive scan
   */
  public ListPage<FileInfo> listFileInfoForPrefixPage(
          URI prefix, ListFileOptions listOptions, String pageToken, boolean recursive) throws IOException {
    logger.atFiner().log(
            "listFileInfoForPrefixPage(prefix: %s, pageToken:%s, recursive:%s)", prefix, pageToken, recursive);
    StorageResourceId prefixId = getPrefixId(prefix);
    ListObjectOptions listObjectOptions = recursive ? ListObjectOptions.DEFAULT_FLAT_LIST.toBuilder().build() : ListObjectOptions.DEFAULT.toBuilder().build();
    ListPage<GoogleCloudStorageItemInfo> itemsInfoPage =
            gcs.listObjectInfoPage(
                    prefixId.getBucketName(),
                    prefixId.getObjectName(),
                    updateListObjectOptions(listObjectOptions, listOptions),
                    pageToken);
    List<FileInfo> filesInfoPage = FileInfo.fromItemInfos(itemsInfoPage.getItems());
    filesInfoPage.sort(FILE_INFO_PATH_COMPARATOR);
    return new ListPage<>(filesInfoPage, itemsInfoPage.getNextPageToken());
  }

  private StorageResourceId getPrefixId(URI prefix) {
    Preconditions.checkNotNull(prefix, "prefix could not be null");

    StorageResourceId prefixId = StorageResourceId.fromUriPath(prefix, true);
    Preconditions.checkArgument(
        !prefixId.isRoot(), "prefix must not be global root, got '%s'", prefix);

    return prefixId;
  }

  /**
   * If the given path points to a directory then the information about its children is returned,
   * otherwise information about the given file is returned.
   *
   * @param path Given path.
   * @return Information about a file or children of a directory.
   * @throws FileNotFoundException if the given path does not exist.
   */
  public List<FileInfo> listFileInfo(URI path) throws IOException {
    return listFileInfo(path, ListFileOptions.DEFAULT);
  }

  /**
   * If the given path points to a directory then the information about its children is returned,
   * otherwise information about the given file is returned.
   *
   * @param path Given path.
   * @return Information about a file or children of a directory.
   * @throws FileNotFoundException if the given path does not exist.
   */
  public List<FileInfo> listFileInfo(URI path, ListFileOptions listOptions) throws IOException {
    Preconditions.checkNotNull(path, "path can not be null");
    logger.atFiner().log("listFileInfo(path: %s)", path);

    StorageResourceId pathId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ true);
    StorageResourceId dirId = pathId.toDirectoryId();

    Future<List<GoogleCloudStorageItemInfo>> dirItemInfosFuture =
        (options.isStatusParallelEnabled() ? cachedExecutor : lazyExecutor)
            .submit(
                () ->
                    dirId.isRoot()
                        ? gcs.listBucketInfo()
                        : gcs.listObjectInfo(
                            dirId.getBucketName(),
                            dirId.getObjectName(),
                            updateListObjectOptions(LIST_FILE_INFO_LIST_OPTIONS, listOptions)));

    if (!pathId.isDirectory()) {
      try {
        GoogleCloudStorageItemInfo pathInfo = gcs.getItemInfo(pathId);
        if (pathInfo.exists()) {
          List<FileInfo> listedInfo = new ArrayList<>();
          listedInfo.add(FileInfo.fromItemInfo(pathInfo));
          dirItemInfosFuture.cancel(/* mayInterruptIfRunning= */ true);
          return listedInfo;
        }
      } catch (Exception e) {
        dirItemInfosFuture.cancel(/* mayInterruptIfRunning= */ true);
        throw e;
      }
    }

    List<GoogleCloudStorageItemInfo> dirItemInfos = getFromFuture(dirItemInfosFuture);
    if (pathId.isStorageObject() && dirItemInfos.isEmpty()) {
      throw new FileNotFoundException("Item not found: " + path);
    }

    if (!dirItemInfos.isEmpty() && Objects.equals(dirItemInfos.get(0).getResourceId(), dirId)) {
      dirItemInfos.remove(0);
    }

    List<FileInfo> fileInfos = FileInfo.fromItemInfos(dirItemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  /**
   * Gets information about the given path item.
   *
   * @param path The path we want information about.
   * @return Information about the given path item.
   */
  public FileInfo getFileInfo(URI path) throws IOException {
    checkArgument(path != null, "path must not be null");
    // Validate the given path. true == allow empty object name.
    // One should be able to get info about top level directory (== bucket),
    // therefore we allow object name to be empty.
    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);
    FileInfo fileInfo =
        FileInfo.fromItemInfo(
            getFileInfoInternal(resourceId, /* inferImplicitDirectories= */ true));
    logger.atFiner().log("getFileInfo(path: %s): %s", path, fileInfo);
    return fileInfo;
  }

  /** @see #getFileInfo(URI) */
  private GoogleCloudStorageItemInfo getFileInfoInternal(
      StorageResourceId resourceId, boolean inferImplicitDirectories) throws IOException {
    if (resourceId.isRoot() || resourceId.isBucket()) {
      return gcs.getItemInfo(resourceId);
    }
    StorageResourceId dirId = resourceId.toDirectoryId();

    Future<List<GoogleCloudStorageItemInfo>> listDirFuture =
        (options.isStatusParallelEnabled() ? cachedExecutor : lazyExecutor)
            .submit(
                () ->
                    inferImplicitDirectories
                        ? gcs.listObjectInfo(
                            dirId.getBucketName(),
                            dirId.getObjectName(),
                            GET_FILE_INFO_LIST_OPTIONS)
                        : ImmutableList.of(gcs.getItemInfo(dirId)));

    if (!resourceId.isDirectory()) {
      try {
        GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(resourceId);
        if (itemInfo.exists()) {
          listDirFuture.cancel(/* mayInterruptIfRunning= */ true);
          return itemInfo;
        }
      } catch (Exception e) {
        listDirFuture.cancel(/* mayInterruptIfRunning= */ true);
        throw e;
      }
    }

    List<GoogleCloudStorageItemInfo> listDirInfo = getFromFuture(listDirFuture);
    if (listDirInfo.isEmpty()) {
      return GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }
    checkState(listDirInfo.size() <= 2, "listed more than 2 objects: '%s'", listDirInfo);
    GoogleCloudStorageItemInfo dirInfo = Iterables.get(listDirInfo, /* position= */ 0);
    checkState(
        dirInfo.getResourceId().equals(dirId) || !inferImplicitDirectories,
        "listed wrong object '%s', but should be '%s'",
        dirInfo.getResourceId(),
        resourceId);
    return dirInfo.getResourceId().equals(dirId) && dirInfo.exists()
        ? dirInfo
        : GoogleCloudStorageItemInfo.createNotFound(resourceId);
  }

  /**
   * Gets information about each path in the given list; more efficient than calling getFileInfo()
   * on each path individually in a loop.
   *
   * @param paths List of paths.
   * @return Information about each path in the given list.
   */
  public List<FileInfo> getFileInfos(List<URI> paths) throws IOException {
    checkArgument(paths != null, "paths must not be null");
    logger.atFiner().log("getFileInfos(paths: %s)", paths);

    if (paths.size() == 1) {
      return new ArrayList<>(Collections.singleton(getFileInfo(paths.get(0))));
    }

    int maxThreads = gcs.getOptions().getBatchThreads();
    ExecutorService fileInfoExecutor =
        maxThreads == 0
            ? newDirectExecutorService()
            : newFixedThreadPool(min(maxThreads, paths.size()), DAEMON_THREAD_FACTORY);
    try {
      List<Future<FileInfo>> infoFutures = new ArrayList<>(paths.size());
      for (URI path : paths) {
        infoFutures.add(fileInfoExecutor.submit(() -> getFileInfo(path)));
      }
      fileInfoExecutor.shutdown();

      List<FileInfo> infos = new ArrayList<>(paths.size());
      for (Future<FileInfo> infoFuture : infoFutures) {
        infos.add(getFromFuture(infoFuture));
      }
      return infos;
    } finally {
      fileInfoExecutor.shutdownNow();
    }
  }

  /** Releases resources used by this instance. */
  public void close() {
    if (gcs == null) {
      return;
    }
    logger.atFiner().log("close()");
    try {
      cachedExecutor.shutdown();
      lazyExecutor.shutdown();
      gcs.close();
    } finally {
      cachedExecutor = null;
      lazyExecutor = null;
      gcs = null;
    }
  }

  /**
   * Creates a directory at the specified path.
   *
   * <p>There are two conventions for using objects as directories in GCS. 1. An object of zero size
   * with name ending in / 2. An object of zero size with name ending in _$folder$
   *
   * <p>#1 is the recommended convention by the GCS team. We use it when creating a directory.
   *
   * <p>However, some old tools still use #2. We will decide based on customer use cases whether to
   * support #2 as well. For now, we only support #1.
   *
   * <p>Note that a bucket is always considered a directory. Doesn't create parent directories;
   * normal use cases should only call mkdirs().
   */
  @VisibleForTesting
  public void mkdir(URI path) throws IOException {
    Preconditions.checkNotNull(path);
    logger.atFiner().log("mkdir(path: %s)", path);
    checkArgument(!path.equals(GCS_ROOT), "Cannot create root directory.");

    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);

    // If this is a top level directory, create the corresponding bucket.
    if (resourceId.isBucket()) {
      gcs.createBucket(resourceId.getBucketName());
      return;
    }

    // Ensure that the path looks like a directory path.
    resourceId = resourceId.toDirectoryId();

    // Not a top-level directory, create 0 sized object.
    gcs.createEmptyObject(resourceId);
  }

  private void checkNoFilesConflictingWithDirs(StorageResourceId resourceId) throws IOException {
    // Create a list of all files that can conflict with intermediate/subdirectory paths.
    // For example: gs://foo/bar/zoo/ => (gs://foo/bar, gs://foo/bar/zoo)
    List<StorageResourceId> fileIds =
        getDirs(resourceId.getObjectName()).stream()
            .filter(subdir -> !isNullOrEmpty(subdir))
            .map(
                subdir ->
                    new StorageResourceId(
                        resourceId.getBucketName(), StringPaths.toFilePath(subdir)))
            .collect(toImmutableList());

    // Each intermediate path must ensure that corresponding file does not exist
    //
    // If for any of the intermediate paths file already exists then bail out early.
    // It is possible that the status of intermediate paths can change after
    // we make this check therefore this is a good faith effort and not a guarantee.
    for (GoogleCloudStorageItemInfo fileInfo : gcs.getItemInfos(fileIds)) {
      if (fileInfo.exists()) {
        throw new FileAlreadyExistsException(
            "Cannot create directories because of existing file: " + fileInfo.getResourceId());
      }
    }
  }

  /**
   * For objects whose name looks like a path (foo/bar/zoo), returns all directory paths.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>foo/bar/zoo => returns: (foo/, foo/bar/)
   *   <li>foo/bar/zoo/ => returns: (foo/, foo/bar/, foo/bar/zoo/)
   *   <li>foo => returns: ()
   * </ul>
   *
   * @param objectName Name of an object.
   * @return List of sub-directory like paths.
   */
  static List<String> getDirs(String objectName) {
    if (isNullOrEmpty(objectName)) {
      return ImmutableList.of();
    }
    List<String> dirs = new ArrayList<>();
    int index = 0;
    while ((index = objectName.indexOf(PATH_DELIMITER, index)) >= 0) {
      index = index + PATH_DELIMITER.length();
      dirs.add(objectName.substring(0, index));
    }
    return dirs;
  }

  /** Gets the leaf item of the given path. */
  String getItemName(URI path) {
    Preconditions.checkNotNull(path, "path can not be null");

    // There is no leaf item for the root path.
    if (path.equals(GCS_ROOT)) {
      return null;
    }

    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);

    if (resourceId.isBucket()) {
      return resourceId.getBucketName();
    }

    String objectName = resourceId.getObjectName();
    int index =
        StringPaths.isDirectoryPath(objectName)
            ? objectName.lastIndexOf(PATH_DELIMITER, objectName.length() - 2)
            : objectName.lastIndexOf(PATH_DELIMITER);
    return index < 0 ? objectName : objectName.substring(index + 1);
  }

  private static ListObjectOptions updateListObjectOptions(
      ListObjectOptions listObjectOptions, ListFileOptions listFileOptions) {
    return listObjectOptions.toBuilder().setFields(listFileOptions.getFields()).build();
  }

  /** Retrieve our internal gcs. */
  public GoogleCloudStorage getGcs() {
    return gcs;
  }
}

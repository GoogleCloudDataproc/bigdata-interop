/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.UUID;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

@RunWith(JUnit4.class)
public class GoogleHadoopFSInputStreamIntegrationTest {

  static FileSystem ghfs;
  static FileSystemDescriptor ghfsFileSystemDescriptor;

  private static HadoopFileSystemIntegrationHelper ghfsHelper;
  private static GoogleCloudStorageFileSystem gcsfs;
  private static GoogleHadoopFileSystemIntegrationHelper ghfsIHelper;

  @BeforeClass
  public static void beforeClass() throws Throwable {
    ghfsIHelper = new GoogleHadoopFileSystemIntegrationHelper();
    gcsfs = ghfsIHelper.initializeGcfs();
    GoogleHadoopFileSystem testInstance = new GoogleHadoopFileSystem();
    ghfs = ghfsIHelper.initializeGhfs(testInstance);
    ghfsFileSystemDescriptor = testInstance;
    ghfsHelper = new HadoopFileSystemIntegrationHelper(ghfs, ghfsFileSystemDescriptor);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    ghfsIHelper.after(gcsfs);
  }

  private static String TEST_DIRECTORY_PATH_FORMAT = "gs://%s/testFSInputStream/";

  @Test
  public void testSeekIllegalArgument() throws IOException {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    byte[] data = new byte[0];
    Path directory =
        new Path(String.format(TEST_DIRECTORY_PATH_FORMAT, myGhfs.getRootBucketName()));
    Path file = new Path(directory, String.format("file-%s", UUID.randomUUID()));
    ghfsHelper.writeFile(file, data, 100, /* overwrite= */ false);
    GoogleHadoopFSInputStream in =
        new GoogleHadoopFSInputStream(
            myGhfs,
            myGhfs.getGcsPath(file),
            GoogleCloudStorageReadOptions.DEFAULT,
            new FileSystem.Statistics(ghfs.getScheme()));
    Throwable exception = assertThrows(java.io.EOFException.class, () -> in.seek(1));
    assertThat(exception).hasMessageThat().contains("Invalid seek offset");

    // Cleanup.
    assertThat(ghfs.delete(directory, true)).isTrue();
  }

  @Test
  public void testRead() throws IOException {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    Path directory =
        new Path(String.format(TEST_DIRECTORY_PATH_FORMAT, myGhfs.getRootBucketName()));
    Path file = new Path(directory, String.format("file-%s", UUID.randomUUID()));
    ghfsHelper.writeFile(file, "Some text", 100, /* overwrite= */ false);
    GoogleHadoopFSInputStream in =
        new GoogleHadoopFSInputStream(
            myGhfs,
            myGhfs.getGcsPath(file),
            GoogleCloudStorageReadOptions.DEFAULT,
            new FileSystem.Statistics(ghfs.getScheme()));
    assertThat(in.read(new byte[2], 1, 1)).isEqualTo(1);
    assertThat(in.read(1, new byte[2], 1, 1)).isEqualTo(1);
    // Cleanup.
    assertThat(ghfs.delete(directory, true)).isTrue();
  }

  @Test
  public void testAvailable() throws IOException {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    byte[] data = new byte[10];
    Path directory =
        new Path(String.format(TEST_DIRECTORY_PATH_FORMAT, myGhfs.getRootBucketName()));
    Path file = new Path(directory, String.format("file-%s", UUID.randomUUID()));
    ghfsHelper.writeFile(file, data, 100, /* overwrite= */ false);
    GoogleHadoopFSInputStream in =
        new GoogleHadoopFSInputStream(
            myGhfs,
            myGhfs.getGcsPath(file),
            GoogleCloudStorageReadOptions.DEFAULT,
            new FileSystem.Statistics(ghfs.getScheme()));
    assertThat(in.available()).isEqualTo(0);
    in.close();
    assertThrows(java.nio.channels.ClosedChannelException.class, () -> in.available());
    // Cleanup.
    assertThat(ghfs.delete(directory, true)).isTrue();
  }
}

/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import com.google.protobuf.MessageLite;
import io.grpc.KnownLength;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Provider;

public class ZeroCopyReadinessChecker {
  private static final boolean isZeroCopyReady;

  static {
    // Check whether io.grpc.Detachable exists?
    boolean detachableClassExists = false;
    try {
      // Try to load Detachable interface in the package where KnownLength is in.
      // This can be done directly by looking up io.grpc.Detachable but rather
      // done indirectly to handle the case where gRPC is being shaded in a
      // different package.
      String knownLengthClassName = KnownLength.class.getName();
      String detachableClassName = knownLengthClassName.substring(0, knownLengthClassName.lastIndexOf('.') + 1)
          + "Detachable";
      Class<?> detachableClass = Class.forName(detachableClassName);
      detachableClassExists = (detachableClass != null);
    } catch (ClassNotFoundException ex) {
    }
    // Check whether com.google.protobuf.UnsafeByteOperations exists?
    boolean unsafeByteOperationsClassExists = false;
    try {
      // Same above
      String messageLiteClassName = MessageLite.class.getName();
      String unsafeByteOperationsClassName = messageLiteClassName.substring(0,
          messageLiteClassName.lastIndexOf('.') + 1) + "UnsafeByteOperations";
      Class<?> unsafeByteOperationsClass = Class.forName(unsafeByteOperationsClassName);
      unsafeByteOperationsClassExists = (unsafeByteOperationsClass != null);
    } catch (ClassNotFoundException ex) {
    }
    isZeroCopyReady = detachableClassExists && unsafeByteOperationsClassExists;
  }

  public static boolean isReady() {
    return isZeroCopyReady;
  }
}

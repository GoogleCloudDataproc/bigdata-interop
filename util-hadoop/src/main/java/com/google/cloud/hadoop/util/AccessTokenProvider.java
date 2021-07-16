/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.util;

import com.google.api.services.storage.StorageRequest;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;

/** A provider to provide access token, and upon access token expiration, the utility to refresh. */
public interface AccessTokenProvider extends Configurable {

  /** An access token and its expiration time. */
  class AccessToken {

    private final String token;
    private final Long expirationTimeMilliSeconds;

    public AccessToken(String token, Long expirationTimeMillis) {
      this.token = token;
      this.expirationTimeMilliSeconds = expirationTimeMillis;
    }

    /** @return the Access Token string. */
    public String getToken() {
      return token;
    }

    /** @return the Time when the token will expire, expressed in milliseconds. */
    public Long getExpirationTimeMilliSeconds() {
      return expirationTimeMilliSeconds;
    }
  }

  /** @return an access token. */
  AccessToken getAccessToken();

  /**
   * @param storageRequest a storage request holding the context.
   * @return an access token.
   */
  default<RequestT extends StorageRequest<?>> AccessToken getAccessToken(RequestT storageRequest) {
    return this.getAccessToken();
  }

  /**
   * Force this provider to refresh its access token.
   *
   * @throws IOException when refresh fails.
   */
  void refresh() throws IOException;

  /**
   * Force this provider to refresh its access token.
   *
   * @param storageRequest a storage request holding the context.
   * @throws IOException when refresh fails.
   */
  default<RequestT extends StorageRequest<?>>void refresh(RequestT storageRequest) throws IOException {
    this.refresh();
  }
}

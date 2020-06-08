/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.util.Clock;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenResponse;
import com.google.cloud.hadoop.util.CredentialFromAccessTokenProviderClassFactory.GoogleCredentialWithAccessTokenProvider;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class CredentialFromAccessTokenProviderClassFactoryTest {

  private static final String TEST_PROPERTY_PREFIX = "test.prefix";

  private Configuration config;
  @Mock private Clock clock;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    config = new Configuration();
    config.setClass(
        TEST_PROPERTY_PREFIX + ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX.getKey(),
        TestingAccessTokenProvider.class,
        AccessTokenProvider.class);
  }

  @Test
  public void testCreateCredentialFromProviderClassFactory()
      throws IOException, GeneralSecurityException {
    Credential credential =
        CredentialFromAccessTokenProviderClassFactory.credential(
            config, ImmutableList.of(TEST_PROPERTY_PREFIX), ImmutableList.of());

    assertThat(credential.getAccessToken()).isEqualTo(TestingAccessTokenProvider.FAKE_ACCESS_TOKEN);
    assertThat(credential.getExpirationTimeMilliseconds())
        .isEqualTo(TestingAccessTokenProvider.EXPIRATION_TIME_MILLISECONDS);
  }

  @Test
  public void testCreateCredentialFromAccessTokenProvider() {
    AccessTokenProvider accessTokenProvider = new TestingAccessTokenProvider();
    GoogleCredential credential =
        GoogleCredentialWithAccessTokenProvider.fromAccessTokenProvider(clock, accessTokenProvider);

    assertThat(credential.getAccessToken()).isEqualTo(TestingAccessTokenProvider.FAKE_ACCESS_TOKEN);
    assertThat(credential.getExpirationTimeMilliseconds())
        .isEqualTo(TestingAccessTokenProvider.EXPIRATION_TIME_MILLISECONDS);
  }

  @Test
  public void testCreateCredentialFromGoogleCloudStorageAccessTokenProvider()
      throws IOException, GeneralSecurityException {
    GenerateAccessTokenResponse accessTokenRes = new GenerateAccessTokenResponse();
    accessTokenRes.setAccessToken(TestingAccessTokenProvider.FAKE_ACCESS_TOKEN);
    // 1970-01-01T00:00:02Z is equal to 2000 milliseconds since Epoch time.
    accessTokenRes.setExpireTime("1970-01-01T00:00:02Z");
    MockHttpTransport transport = mockTransport(jsonDataResponse(accessTokenRes));
    CredentialFactory.setStaticHttpTransport(transport);
    List<HttpRequest> requests = new ArrayList<>();

    GoogleCredential credential =
        new GoogleCredentialWithIamAccessToken("test-service-account", requests::add, Clock.SYSTEM);

    assertThat(credential.getAccessToken()).isEqualTo(TestingAccessTokenProvider.FAKE_ACCESS_TOKEN);
    assertThat(credential.getExpirationTimeMilliseconds())
        .isEqualTo(TestingAccessTokenProvider.EXPIRATION_TIME_MILLISECONDS);
  }
}

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import java.io.File;
import java.io.FileReader;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;

public class CertificateUtils {

  public static final String ALIAS_PRIVATE = "private";
  public static final String ALIAS_CERT = "cert";
  // The password is not used to encrypt keys on disk.
  public static final String IN_MEMORY_PWD = "opendistro";
  private static final String CERTIFICATE_FILE_PATH = "certificate-file-path";
  private static final String PRIVATE_KEY_FILE_PATH = "private-key-file-path";
  private static final Logger LOGGER = LogManager.getLogger(CertificateUtils.class);

  public static Certificate getCertificate(final FileReader certReader) throws Exception {
    try (PEMParser pemParser = new PEMParser(certReader)) {
      X509CertificateHolder certificateHolder = (X509CertificateHolder) pemParser.readObject();
      Certificate caCertificate =
          new JcaX509CertificateConverter().setProvider("BC").getCertificate(certificateHolder);
      return caCertificate;
    }
  }

  public static PrivateKey getPrivateKey(final FileReader keyReader) throws Exception {
    try (PEMParser pemParser = new PEMParser(keyReader)) {
      PrivateKeyInfo pki = (PrivateKeyInfo) pemParser.readObject();
      return BouncyCastleProvider.getPrivateKey(pki);
    }
  }

  public static KeyStore createKeyStore() throws Exception {
    String certFilePath = PluginSettings.instance().getSettingValue(CERTIFICATE_FILE_PATH);
    String keyFilePath = PluginSettings.instance().getSettingValue(PRIVATE_KEY_FILE_PATH);
    PrivateKey pk = getPrivateKey(new FileReader(keyFilePath));
    KeyStore ks = createEmptyStore();
    Certificate certificate = getCertificate(new FileReader(certFilePath));
    ks.setCertificateEntry(ALIAS_CERT, certificate);
    ks.setKeyEntry(ALIAS_PRIVATE, pk, IN_MEMORY_PWD.toCharArray(), new Certificate[] {certificate});
    return ks;
  }

  public static KeyStore createEmptyStore() throws Exception {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, IN_MEMORY_PWD.toCharArray());
    return ks;
  }

  public static File getCertificateFile() {
    String certFilePath = PluginSettings.instance().getSettingValue(CERTIFICATE_FILE_PATH);
    return new File(certFilePath);
  }

  public static File getPrivateKeyFile() {
    String privateKeyPath = PluginSettings.instance().getSettingValue(PRIVATE_KEY_FILE_PATH);
    return new File(privateKeyPath);
  }
}

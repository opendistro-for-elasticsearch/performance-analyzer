package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import java.io.FileReader;
import java.security.KeyStore;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.Certificate;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;

public class CertificateUtils {

    public static final String ALIAS_PRIVATE = "private";
    public static final String ALIAS_CERT = "cert";
    //The password is not used to encrypt keys on disk.
    public static final String IN_MEMORY_PWD = "opendistro";
    private static final String CERTIFICATE_FILE_PATH = "certificate-file-path";
    private static final String PRIVATE_KEY_FILE_PATH = "private-key-file-path";
    private static final Logger LOGGER = LogManager.getLogger(CertificateUtils.class);

    public static Certificate getCertificate(final FileReader certReader) throws Exception {
        try (PEMParser pemParser = new PEMParser(certReader)) {
            X509CertificateHolder certificateHolder = (X509CertificateHolder) pemParser.readObject();
            Certificate caCertificate = new JcaX509CertificateConverter()
                .setProvider("BC")
                .getCertificate(certificateHolder);
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
        KeyStore ks = emptyStore();
        Certificate certificate = getCertificate(new FileReader(certFilePath));
        ks.setCertificateEntry(ALIAS_CERT, certificate);
        ks.setKeyEntry(ALIAS_PRIVATE, pk, IN_MEMORY_PWD.toCharArray(), new Certificate[]{certificate});
        return ks;
    }

    public static KeyStore emptyStore() throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, IN_MEMORY_PWD.toCharArray());
        return ks;
    }
}


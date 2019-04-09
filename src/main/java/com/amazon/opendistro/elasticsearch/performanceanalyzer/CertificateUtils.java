package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import org.bouncycastle.asn1.x500.X500Name;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.SignatureException;
import java.util.Date;
import java.util.Calendar;
import java.security.PrivateKey;

import javax.security.auth.x500.X500Principal;

import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.X509Extensions;
import org.bouncycastle.asn1.x509.X509Name;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.x509.X509V3CertificateGenerator;
import org.bouncycastle.operator.ContentSigner;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.SignatureException;
import java.util.Date;

import javax.security.auth.x500.X500Principal;

import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.X509Extensions;
import org.bouncycastle.asn1.x509.X509Name;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.x509.X509V3CertificateGenerator;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;

import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.cert.X509CertificateHolder;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.io.FileReader;

public class CertificateUtils {

    public static final String KEY_TYPE_RSA = "RSA";
    public static final String SIG_ALG_SHA_RSA = "SHA1WithRSA";
    public static final int KEY_SIZE = 1024;
    public static final long CERT_VALIDITY = 365 * 24 * 3600L;
    public static final String ALIAS_PRIVATE = "private";
    public static final String ALIAS_CERT = "cert";
    public static final String IN_MEMORY_PWD = "notReallyImportant"; // this would only ever be relevant if/when persisted.
    private static final Logger LOGGER = LogManager.getLogger(CertificateUtils.class);


    public static Certificate selfSign(KeyPair keyPair, String subjectDN) throws Exception {
                        Provider bcProvider = new BouncyCastleProvider();
            Security.addProvider(bcProvider);

            long now = System.currentTimeMillis();
            Date startDate = new Date(now);

            X500Name dnName = new X500Name(subjectDN);
            BigInteger certSerialNumber = new BigInteger(Long.toString(now)); // <-- Using the current timestamp as the certificate serial number

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);
            calendar.add(Calendar.YEAR, 1); // <-- 1 Yr validity

            Date endDate = calendar.getTime();

            String signatureAlgorithm = "SHA256WithRSA"; // <-- Use appropriate signature algorithm based on your keyPair algorithm.

            ContentSigner contentSigner = new JcaContentSignerBuilder(signatureAlgorithm).build(keyPair.getPrivate());

            JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(dnName, certSerialNumber, startDate, endDate, dnName, keyPair.getPublic());

            // Extensions --------------------------

            // Basic Constraints
            BasicConstraints basicConstraints = new BasicConstraints(true); // <-- true for CA, false for EndEntity

            certBuilder.addExtension(new ASN1ObjectIdentifier("2.5.29.19"), true, basicConstraints); // Basic Constraints is usually marked as critical.

            // -------------------------------------

            return new JcaX509CertificateConverter().setProvider(bcProvider).getCertificate(certBuilder.build(contentSigner));
    }

    public static KeyStore createTrustStore(final FileReader certReader) 
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
               try (PEMParser pemParser = new PEMParser(certReader)) {
                   X509CertificateHolder certificateHolder = (X509CertificateHolder) pemParser.readObject();
                   Certificate caCertificate = new JcaX509CertificateConverter()
                       .setProvider("BC")
                       .getCertificate(certificateHolder);

                   KeyStore trustStore = KeyStore.getInstance("JKS");
                   trustStore.load(null);
                   trustStore.setCertificateEntry("ca", caCertificate);

                   return trustStore;
               }
    }

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
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
        try (PEMParser pemParser = new PEMParser(keyReader)) {
            PrivateKeyInfo pki = (PrivateKeyInfo) pemParser.readObject();
            return BouncyCastleProvider.getPrivateKey(pki);
        }
    }

    public static KeyStore createSelfSigned(String certValues) {
        try {
            /*
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BC");
            keyPairGenerator.initialize(4096, new SecureRandom());
            KeyPair keyPair = keyPairGenerator.generateKeyPair();
            */
            PrivateKey pk = getPrivateKey(new FileReader("/tmp/key"));

            KeyStore ks = emptyStore();
            if (ks == null) {
                return null;
            }

            //Certificate certificate = selfSign(keyPair, certValues);
            Certificate certificate = getCertificate(new FileReader("/tmp/cert"));
            ks.setCertificateEntry(ALIAS_CERT, certificate);
            ks.setKeyEntry(ALIAS_PRIVATE, pk, IN_MEMORY_PWD.toCharArray(), new Certificate[]{certificate});
            return ks;
        } catch (Exception e) {
            LOGGER.error("Cannot create self signed certificate.", e);
        }
        return null;
    }

    public static KeyStore createSelfSignedForHost(String host) {
        return createSelfSigned("CN=" + host);
    }

    public static KeyStore emptyStore() {
        try {
            KeyStore ks = KeyStore.getInstance("JKS");

            // Loading creates the store, can't do anything with it until it's loaded
            ks.load(null, IN_MEMORY_PWD.toCharArray());
            return ks;
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException e) {
            LOGGER.error("Cannot create empty keystore.", e);
        }
        return null;
    }

}

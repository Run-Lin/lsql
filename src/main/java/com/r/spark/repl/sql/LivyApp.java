package com.r.spark.repl.sql;

import com.r.spark.repl.sql.common.Utils;
import com.r.spark.repl.sql.exception.APINotFoundException;
import com.r.spark.repl.sql.exception.LivyException;
import com.r.spark.repl.sql.exception.SessionDeadException;
import com.r.spark.repl.sql.exception.SessionNotFoundException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.UUID;


public class LivyApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(LivyApp.class);
    private static final String SESSION_NOT_FOUND_PATTERN = "\"Session '\\d+' not found.\"";
    protected static Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
    protected static final int pullStatusInterval = 1000;
    protected String livyURL;
    protected RestTemplate restTemplate;
    protected RestTemplate createRestTemplate() {
        String keytabLocation = "";
        String principal = "";
        String keystoreFile = "";
        String password = "";
        boolean isSpnegoEnabled = StringUtils.isNotEmpty(keytabLocation) &&
                StringUtils.isNotEmpty(principal);

        HttpClient httpClient = null;
        if (livyURL!=null && livyURL.startsWith("https:")) {
            if (StringUtils.isBlank(keystoreFile)) {
                throw new RuntimeException("No zeppelin.livy.ssl.trustStore specified for livy ssl");
            }
            if (StringUtils.isBlank(password)) {
                throw new RuntimeException("No zeppelin.livy.ssl.trustStorePassword specified " +
                        "for livy ssl");
            }
            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(keystoreFile);
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                trustStore.load(new FileInputStream(keystoreFile), password.toCharArray());
                SSLContext sslContext = SSLContexts.custom()
                        .loadTrustMaterial((TrustStrategy) trustStore)
                        .build();
                SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext);
                HttpClientBuilder httpClientBuilder = HttpClients.custom().setSSLSocketFactory(csf);
                RequestConfig reqConfig = new RequestConfig() {
                    @Override
                    public boolean isAuthenticationEnabled() {
                        return true;
                    }
                };
                httpClientBuilder.setDefaultRequestConfig(reqConfig);
                Credentials credentials = new Credentials() {
                    public Principal getUserPrincipal() {
                        return null;
                    }

                    public String getPassword() {
                        return null;
                    }
                };
                CredentialsProvider credsProvider = new BasicCredentialsProvider();
                credsProvider.setCredentials(AuthScope.ANY, credentials);
                httpClientBuilder.setDefaultCredentialsProvider(credsProvider);
                if (isSpnegoEnabled) {
                    Registry<AuthSchemeProvider> authSchemeProviderRegistry =
                            RegistryBuilder.<AuthSchemeProvider>create()
                                    .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory())
                                    .build();
                    httpClientBuilder.setDefaultAuthSchemeRegistry(authSchemeProviderRegistry);
                }

                httpClient = httpClientBuilder.build();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create SSL HttpClient", e);
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        LOGGER.error("Failed to close keystore file", e);
                    }
                }
            }
        }

        RestTemplate restTemplate = null;
        if (httpClient == null) {
            restTemplate = new RestTemplate();
        } else {
            restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient));
        }
        restTemplate.getMessageConverters().add(0,
                new StringHttpMessageConverter(Charset.forName("UTF-8")));
        return restTemplate;
    }

    protected String callRestAPI(String targetURL, String method, String jsonData)
            throws LivyException {
        targetURL = livyURL + targetURL;
        LOGGER.debug("Call rest api in {}, method: {}, jsonData: {}", targetURL, method, jsonData);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE);
        ResponseEntity<String> response = null;
        try {
            if (method.equals("POST")) {
                HttpEntity<String> entity = new HttpEntity<String>(jsonData, headers);
                response = restTemplate.exchange(targetURL, HttpMethod.POST, entity, String.class);
            } else if (method.equals("GET")) {
                HttpEntity<String> entity = new HttpEntity<String>(headers);
                response = restTemplate.exchange(targetURL, HttpMethod.GET, entity, String.class);
            } else if (method.equals("DELETE")) {
                HttpEntity<String> entity = new HttpEntity<String>(headers);
                response = restTemplate.exchange(targetURL, HttpMethod.DELETE, entity, String.class);
            }
        } catch (HttpClientErrorException e) {
            response = new ResponseEntity(e.getResponseBodyAsString(), e.getStatusCode());
            LOGGER.error(String.format("Error with %s StatusCode: %s",
                    response.getStatusCode().value(), e.getResponseBodyAsString()));
        } catch (RestClientException e) {
            // Exception happens when kerberos is enabled.
            if (e.getCause() instanceof HttpClientErrorException) {
                HttpClientErrorException cause = (HttpClientErrorException) e.getCause();
                if (cause.getResponseBodyAsString().matches(SESSION_NOT_FOUND_PATTERN)) {
                    throw new SessionNotFoundException(cause.getResponseBodyAsString());
                }
                throw new LivyException(cause.getResponseBodyAsString() + "\n"
                        + ExceptionUtils.getFullStackTrace(ExceptionUtils.getRootCause(e)));
            }
            if (e instanceof HttpServerErrorException) {
                HttpServerErrorException errorException = (HttpServerErrorException) e;
                String errorResponse = errorException.getResponseBodyAsString();
                if (errorResponse.contains("Session is in state dead")) {
                    throw new SessionDeadException();
                }
                throw new LivyException(errorResponse, e);
            }
            throw new LivyException(e);
        }
        if (response == null) {
            throw new LivyException("No http response returned");
        }
        LOGGER.debug("Get response, StatusCode: {}, responseBody: {}", response.getStatusCode(),
                response.getBody());
        if (response.getStatusCode().value() == 200
                || response.getStatusCode().value() == 201) {
            return response.getBody();
        } else if (response.getStatusCode().value() == 404) {
            if (response.getBody().matches(SESSION_NOT_FOUND_PATTERN)) {
                throw new SessionNotFoundException(response.getBody());
            } else {
                throw new APINotFoundException("No rest api found for " + targetURL +
                        ", " + response.getStatusCode());
            }
        } else {
            String responseString = response.getBody();
            if (responseString.contains("CreateInteractiveRequest[\\\"master\\\"]")) {
                return responseString;
            }
            throw new LivyException(String.format("Error with %s StatusCode: %s",
                    response.getStatusCode().value(), responseString));
        }
    }
    public static String uploadLocalFile(String fileName) throws  Exception{
        Configuration conf = new Configuration();

        Properties properties = Utils.loadHDFSProperties();
        if(properties == null) {
            System.err.println("no properties!");
            System.exit(1);
        }
        conf.addResource(new URL(properties.get("hdfs-site.xml").toString()));
        conf.addResource(new URL(properties.get("core-site.xml").toString()));
        Object mountTable = properties.get("mount-table.xml");
        if(mountTable != null && mountTable.toString().trim().length() > 0) {
            conf.addResource(new URL(properties.get("mount-table.xml").toString()));
        }
        conf.set("fs.defaultFS", properties.get("fs.defaultFS").toString());
        conf.set("fs.hdfs.impl", properties.get("fs.hdfs.impl").toString());
        String destFormat = properties.get("destFormat").toString();

        Path srcPath = new Path(fileName);
        FileSystem localFs;
        if (!srcPath.toUri().isAbsolute()) {
            localFs = FileSystem.getLocal(conf);
            srcPath = localFs.makeQualified(srcPath);
        } else {
            localFs = FileSystem.get(srcPath.toUri(), conf);
        }
        if(localFs.getScheme().equals("file")){
            String uuid = "batch-"+ UUID.randomUUID().toString();
            String destDir = MessageFormat.format(destFormat, UserGroupInformation.getCurrentUser().getUserName(),uuid);
            Path destPath = new Path(destDir+ File.separator+srcPath.getName());
            FileSystem remoteFs = FileSystem.newInstance(conf);
            remoteFs.copyFromLocalFile(false,true,srcPath,destPath);
            remoteFs.close();
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.err.println("process exit clean dest file:"+destDir);
                        FileSystem deleteFs = FileSystem.newInstance(conf);
                        deleteFs.delete(new Path(destDir),true);
                        deleteFs.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }));
            return destPath.toString();
        }
        return srcPath.toString();
    }
}

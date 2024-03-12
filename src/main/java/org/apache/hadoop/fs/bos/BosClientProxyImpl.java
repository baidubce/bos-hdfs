/*
 * Copyright 2023 Baidu, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.fs.bos;

import com.baidubce.BceServiceException;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.auth.DefaultBceSessionCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.services.bos.model.HeadBucketResponse;
import com.baidubce.services.bos.model.PutObjectRequest;
import com.baidubce.services.bos.model.PutObjectResponse;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.UploadPartResponse;
import com.baidubce.services.bos.model.UploadPartRequest;
import com.baidubce.services.bos.model.UploadPartCopyRequest;
import com.baidubce.services.bos.model.UploadPartCopyResponse;
import com.baidubce.services.bos.model.CopyObjectRequest;
import com.baidubce.services.bos.model.CopyObjectResponse;
import com.baidubce.services.bos.model.RenameObjectResponse;
import com.baidubce.services.bos.model.CompleteMultipartUploadResponse;
import com.baidubce.services.bos.model.CompleteMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadResponse;
import com.baidubce.services.bos.model.DeleteDirectoryResponse;
import com.baidubce.services.bos.model.DeleteMultipleObjectsRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;

public class BosClientProxyImpl implements BosClientProxy {

    public static final Log LOG = LogFactory.getLog(BosClientProxyImpl.class);
    private BosClient bosClient = null;
    private static final int BOS_UPLOAD_CONFLICT_CODE = 412;
    private static final int BOS_REQUEST_LIMIT_CODE = 429;
    private static final int BOS_NO_SUCH_KEY_CODE = 404;
    private static final int STREAM_BUFFER_DEFAULT_SIZE = 64 * 1024;
    private boolean crc32cChecksumEnable = false;

    public void init(URI uri, Configuration conf) {
        BosClientConfiguration config = new BosClientConfiguration();
        String accessKey = null;
        String secretAccessKey = null;
        String sessionToken = null;
        String userInfo = uri.getUserInfo();

        if (userInfo != null) {
            int index = userInfo.indexOf(':');
            if (index != -1) {
                accessKey = userInfo.substring(0, index);
                secretAccessKey = userInfo.substring(index + 1);
            } else {
                accessKey = userInfo;
            }
        }

        if (accessKey == null && secretAccessKey == null) {
            accessKey = conf.get("fs.bos.access.key");
            secretAccessKey = conf.get("fs.bos.secret.access.key");
        }

        if (sessionToken == null) {
            sessionToken = conf.get("fs.bos.session.token.key");
        }

        if (accessKey == null || secretAccessKey == null) {
            throw new IllegalArgumentException("accessKey and secretAccessKey should not be null");
        }

        if (sessionToken == null || sessionToken.trim().isEmpty()) {
            config.setCredentials(new DefaultBceCredentials(accessKey, secretAccessKey));
        } else {
            config.setCredentials(new DefaultBceSessionCredentials(accessKey, secretAccessKey, sessionToken));
        }

        // support ConsistencyView
        // config.setConsistencyView(BosClientConfiguration.STRONG_CONSISTENCY_VIEW);

        String endPoint = conf.get("fs.bos.endpoint");
        config.setEndpoint(endPoint);
        int maxConnections = conf.getInt("fs.bos.max.connections", 1000);
        config.setMaxConnections(maxConnections);
        // config.setStaleConnectionCheckEnabled(false);
        config.setUserAgent(BaiduBosConfigKeys.BOS_FILESYSTEM_USER_AGENT + "/" +
                BosClientConfiguration.DEFAULT_USER_AGENT);
        config.withStreamBufferSize(conf.getInt("fs.bos.stream.buffer.size", STREAM_BUFFER_DEFAULT_SIZE));

        crc32cChecksumEnable = conf.getBoolean(BaiduBosConfigKeys.BOS_CRC32C_CHECKSUM_ENABLE, false);

        bosClient = new BosClient(config);
    }

    public PutObjectResponse putObject(String bucketName, String key, File file) throws IOException {
        PutObjectResponse response = null;
        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, key, file);
            if (crc32cChecksumEnable) {
                request.setxBceCrc32cFlag(true);
            }
            response = bosClient.putObject(request);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        return response;
    }

    public PutObjectResponse putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata)
            throws IOException {
        PutObjectResponse response = null;
        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, key, input, metadata);
            if (crc32cChecksumEnable) {
                request.setxBceCrc32cFlag(true);
            }
            response = bosClient.putObject(request);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        return response;
    }

    public void putEmptyObject(String bucketName, String key) throws IOException {
        InputStream in = null;
        try {
            in = new ByteArrayInputStream(new byte[0]);
            ObjectMetadata objectMeta = new ObjectMetadata();
            objectMeta.setContentType("binary/octet-stream");
            objectMeta.setContentLength(0);
            PutObjectRequest request = new PutObjectRequest(bucketName, key, in, objectMeta);
            if (crc32cChecksumEnable) {
                request.setxBceCrc32cFlag(true);
            }
            bosClient.putObject(request);
        } catch (BceServiceException e) {
            if (BOS_UPLOAD_CONFLICT_CODE == e.getStatusCode() || BOS_REQUEST_LIMIT_CODE == e.getStatusCode()) {
                // when status code = 412 or 429, it means another thread uploading same object
                LOG.debug("another thread uploading same bos object");
                try {
                    bosClient.getObjectMetadata(bucketName, key);
                } catch (BceServiceException ex) {
                    if (BOS_NO_SUCH_KEY_CODE == ex.getStatusCode()) {
                        LOG.error("concurrentUpload bos failed");
                        handleBosServiceException(key, ex);
                    }
                }
            } else {
                handleBosServiceException(e);
            }
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
            }
        }
    }

    public ObjectMetadata getObjectMetadata(String bucketName, String key) throws IOException {
        ObjectMetadata response = null;
        try {
            response = bosClient.getObjectMetadata(bucketName, key);
        } catch (BceServiceException e) {
            handleBosServiceException(key, e);
        }
        return response;
    }

    public BosObject getObject(String bucketName, String key) throws IOException {
        BosObject response = null;
        try {
            response = bosClient.getObject(bucketName, key);
        } catch (BceServiceException e) {
            handleBosServiceException(key, e);
        }
        return response;
    }

    public BosObject getObject(GetObjectRequest request) throws IOException {
        BosObject response = null;
        try {
            response = bosClient.getObject(request);
        } catch (BceServiceException e) {
            handleBosServiceException(request.getKey(), e);
        }
        return response;
    }

    public ListObjectsResponse listObjects(ListObjectsRequest request) throws IOException {
        ListObjectsResponse response = null;
        try {
            response = bosClient.listObjects(request);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        return response;
    }

    public ListObjectsResponse listObjects(String bucketName, String prefix) throws IOException {
        ListObjectsResponse response = null;
        try {
            response = bosClient.listObjects(bucketName, prefix);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        return response;
    }

    public ListObjectsResponse listObjects(String bucketName) throws IOException {
        ListObjectsResponse response = null;
        try {
            response = bosClient.listObjects(bucketName);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        return response;
    }

    public UploadPartResponse uploadPart(UploadPartRequest request) throws IOException {
        UploadPartResponse response = null;
        try {
            if (crc32cChecksumEnable) {
                request.setxBceCrc32cFlag(true);
            }
            response = bosClient.uploadPart(request);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        return response;
    }

    public UploadPartCopyResponse uploadPartCopy(UploadPartCopyRequest request) throws IOException {
        UploadPartCopyResponse response = null;
        try {
            if (crc32cChecksumEnable) {
                request.setxBceCrc32cFlag(true);
            }
            response = bosClient.uploadPartCopy(request);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        return response;
    }

    public void deleteObject(String bucketName, String key) throws IOException {
        try {
            bosClient.deleteObject(bucketName, key);
        } catch (BceServiceException e) {
            if (BOS_NO_SUCH_KEY_CODE == e.getStatusCode()) {
                // if key is not exist, we think key deleted success
                LOG.warn("Deleting key: " + key + " from bucket: " + bucketName + " but key is not exist");
            } else {
                handleBosServiceException(e);
            }
        }
    }

    public void deleteMultipleObjects(DeleteMultipleObjectsRequest request) throws IOException {
        try {
            bosClient.deleteMultipleObjects(request);
        } catch (BceServiceException e) {
            // BOS_NO_SUCH_KEY_CODE in response.getErrors, don't handle
            handleBosServiceException(e);
        }
    }

    public DeleteDirectoryResponse deleteDirectory(String bucketName, String key,
                                                   boolean isDeleteRecursive, String marker) throws IOException {
        DeleteDirectoryResponse response = null;
        try {
            response = bosClient.deleteDirectory(bucketName, key, isDeleteRecursive, marker);
        } catch (BceServiceException e) {
            if (BOS_NO_SUCH_KEY_CODE == e.getStatusCode()) {
                // if key is not exist, we think key deleted success
                LOG.warn("Deleting Directory: " + key + " from bucket: " + bucketName + " but Directory is not exist");
            } else {
                handleBosServiceException(e);
            }
        }
        return response;
    }

    public CopyObjectResponse copyObject(String sourceBucketName, String sourceKey,
                                         String destinationBucketName, String destinationKey) throws IOException {
        CopyObjectResponse response = null;
        try {
            CopyObjectRequest request = new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
            if (crc32cChecksumEnable) {
                request.setxBceCrc32cFlag(true);
            }
            response = bosClient.copyObject(request);
        } catch (BceServiceException e) {
            handleBosServiceException(sourceKey, e);
        }
        return response;
    }

    public RenameObjectResponse renameObject(String bucketName, String sourceKey, String destinationKey)
            throws IOException {
        RenameObjectResponse response = null;
        try {
            response = bosClient.renameObject(bucketName, sourceKey, destinationKey);
        } catch (BceServiceException e) {
            handleBosServiceException(sourceKey, e);
        }
        return response;
    }

    public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest request)
            throws IOException {
        CompleteMultipartUploadResponse response = null;
        try {
            if (crc32cChecksumEnable) {
                request.setxBceCrc32cFlag(true);
            }
            response = bosClient.completeMultipartUpload(request);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        return response;
    }

    public InitiateMultipartUploadResponse initiateMultipartUpload(InitiateMultipartUploadRequest request)
            throws IOException {
        InitiateMultipartUploadResponse response = null;
        try {
            response = bosClient.initiateMultipartUpload(request);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        return response;
    }

    public void close() {
        if (bosClient != null) {
            bosClient.shutdown();
            bosClient = null;
        }
    }

    private void handleBosServiceException(String key, BceServiceException e) throws IOException {
        if (BOS_NO_SUCH_KEY_CODE == e.getStatusCode()) {
            throw new FileNotFoundException("Key '" + key + "' does not exist in BOS");
        } else {
            handleBosServiceException(e);
        }
    }

    private void handleBosServiceException(BceServiceException e) throws IOException {
        // if (400 == e.getStatusCode() && "InvalidSessionToken".equalsIgnoreCase(e.getErrorCode())) {
        // process all 400 status, because as token expired bos server return "Error Code=null" when request object use a http head method"
        if (400 == e.getStatusCode()) {
            // updateBosClient();
            throw new SessionTokenExpireException(e);
        } else if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("BOS Error code: " + e.getErrorCode() + "; BOS Error message: " + e.getErrorMessage());
            }
            if (5 == (e.getStatusCode() / 100)) {
                throw new BosServerException(e);
            }
            throw new BosException(e);
        }
    }

    public boolean isHierarchyBucket(String bucketName) throws IOException {
        HeadBucketResponse response = null;
        try {
            response = this.bosClient.headBucket(bucketName);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }

        if (response != null && response.getMetadata() != null && response.getMetadata().getBucketType() != null) {
            return response.getMetadata().getBucketType().equals("namespace");
        }

        return false;
    }
}


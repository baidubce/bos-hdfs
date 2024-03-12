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

import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.services.bos.model.PutObjectResponse;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.UploadPartResponse;
import com.baidubce.services.bos.model.UploadPartRequest;
import com.baidubce.services.bos.model.UploadPartCopyRequest;
import com.baidubce.services.bos.model.UploadPartCopyResponse;
import com.baidubce.services.bos.model.CopyObjectResponse;
import com.baidubce.services.bos.model.RenameObjectResponse;
import com.baidubce.services.bos.model.CompleteMultipartUploadResponse;
import com.baidubce.services.bos.model.CompleteMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadResponse;
import com.baidubce.services.bos.model.DeleteDirectoryResponse;
import com.baidubce.services.bos.model.DeleteMultipleObjectsRequest;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.net.URI;

/**
 * Created by zhangwei95 on 2020/3/12.
 */
public interface BosClientProxy {

    void init(URI uri, Configuration conf);

    PutObjectResponse putObject(String bucketName, String key, File file) throws IOException;

    PutObjectResponse putObject(String bucketName, String key, InputStream input,
                                ObjectMetadata metadata) throws IOException;

    void putEmptyObject(String bucketName, String key) throws IOException;

    ObjectMetadata getObjectMetadata(String bucketName, String key) throws IOException;

    BosObject getObject(String bucketName, String key) throws IOException;

    BosObject getObject(GetObjectRequest request) throws IOException;

    ListObjectsResponse listObjects(ListObjectsRequest request) throws IOException;

    ListObjectsResponse listObjects(String bucketName, String prefix) throws IOException;

    ListObjectsResponse listObjects(String bucketName) throws IOException;

    UploadPartResponse uploadPart(UploadPartRequest request) throws IOException;

    UploadPartCopyResponse uploadPartCopy(UploadPartCopyRequest request) throws IOException;

    void deleteObject(String bucketName, String key) throws IOException;

    DeleteDirectoryResponse deleteDirectory(String bucketName, String key,
                                            boolean isDeleteRecursive, String marker) throws IOException;

    void deleteMultipleObjects(DeleteMultipleObjectsRequest request) throws IOException;

    CopyObjectResponse copyObject(String sourceBucketName, String sourceKey,
                                  String destinationBucketName, String destinationKey) throws IOException;

    RenameObjectResponse renameObject(String bucketName, String sourceKey, String destinationKey) throws IOException;

    CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest request) throws IOException;

    InitiateMultipartUploadResponse initiateMultipartUpload(InitiateMultipartUploadRequest request) throws IOException;

    boolean isHierarchyBucket(String bucketName) throws IOException;

    void close();

}

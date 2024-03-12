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

import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import com.baidubce.BceClientException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.PartETag;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.UploadPartResponse;
import com.baidubce.services.bos.model.UploadPartRequest;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.CompleteMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadResponse;
import com.baidubce.services.bos.model.DeleteDirectoryResponse;
import com.baidubce.services.bos.model.DeleteMultipleObjectsRequest;
import com.baidubce.services.bos.model.UploadPartCopyRequest;
import com.baidubce.services.bos.model.UploadPartCopyResponse;

import org.apache.hadoop.io.retry.DefaultFailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.fs.FileChecksum;

class BosNativeFileSystemStore {
    public static final Log LOG =
            LogFactory.getLog(BosNativeFileSystemStore.class);
    private BosClientProxy bosClientProxy = null;
    private FileMetaCache fileMetaCache = null;
    private String bucketName;
    private boolean isHierarchy;
    private int multipartBlockSize;
    private boolean multipartEnabled;
    private int multiUploadThreadNum;
    private Configuration conf = null;
    private static ExecutorService renamePool = null;
    private static int renameConcurrentSize;
    private static int renameConcurrentMaxSize;

    private static BosClientProxy createDefaultBosClientProxy(Configuration conf) {
        BosClientProxy bosClientProxy = new BosClientProxyImpl();
        RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithProportionalSleep(
                conf.getInt("fs.bos.maxRetries", 5),
                conf.getLong("fs.bos.sleep.millSeconds", 200), TimeUnit.MILLISECONDS);
        Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
                new HashMap<Class<? extends Exception>, RetryPolicy>();

        exceptionToPolicyMap.put(IOException.class, basePolicy);
        exceptionToPolicyMap.put(SessionTokenExpireException.class, basePolicy);
        exceptionToPolicyMap.put(BceClientException.class, basePolicy);
        exceptionToPolicyMap.put(BosServerException.class, basePolicy);
        RetryPolicy methodPolicy = RetryPolicies.retryByException(
                RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);

        Map<String, RetryPolicy> methodNameToPolicyMap =
                new HashMap<String, RetryPolicy>();

        methodNameToPolicyMap.put("putObject", methodPolicy);
        methodNameToPolicyMap.put("putEmptyObject", methodPolicy);
        methodNameToPolicyMap.put("getObjectMetadata", methodPolicy);
        methodNameToPolicyMap.put("getObject", methodPolicy);
        methodNameToPolicyMap.put("listObjects", methodPolicy);
        methodNameToPolicyMap.put("uploadPart", methodPolicy);
        methodNameToPolicyMap.put("deleteObject", methodPolicy);
        methodNameToPolicyMap.put("copyObject", methodPolicy);
        methodNameToPolicyMap.put("renameObject", methodPolicy);
        methodNameToPolicyMap.put("completeMultipartUpload", methodPolicy);
        methodNameToPolicyMap.put("initiateMultipartUpload", methodPolicy);
        methodNameToPolicyMap.put("isHierarchyBucket", methodPolicy);

        return (BosClientProxy) RetryProxy.create(BosClientProxy.class,
                new DefaultFailoverProxyProvider<BosClientProxy>(BosClientProxy.class, bosClientProxy),
                methodNameToPolicyMap, new BosTryOnceThenFail());
    }

    public void initialize(URI uri, Configuration conf) throws IOException {
        this.conf = conf;

        bosClientProxy = createDefaultBosClientProxy(conf);
        bosClientProxy.init(uri, conf);

        multipartEnabled =
                conf.getBoolean("fs.bos.multipart.store.enabled", true);
        multipartBlockSize = conf.getInt(BaiduBosConfigKeys.BOS_MULTI_UPLOAD_BLOCK_SIZE, BaiduBosConfigKeys.DEFAULT_BOS_MULTI_UPLOAD_BLOCK_SIZE);
        multiUploadThreadNum = conf.getInt(BaiduBosConfigKeys.BOS_MULTI_UPLOAD_CONCURRENT_SIZE, BaiduBosConfigKeys.DEFAULT_MULTI_UPLOAD_CONCURRENT_SIZE);
        renameConcurrentSize = conf.getInt(BaiduBosConfigKeys.BOS_RENAME_CONCURRENT_SIZE, 4);
        renameConcurrentMaxSize = conf.getInt(BaiduBosConfigKeys.BOS_RENAME_CONCURRENT_MAX_SIZE, 32);

        bucketName = uri.getHost();
        if (conf.get(BaiduBosConfigKeys.BOS_BUCKET_HIERARCHY, "null").equals("true")) {
            isHierarchy = true;
        } else if (conf.get(BaiduBosConfigKeys.BOS_BUCKET_HIERARCHY, "null").equals("false")) {
            isHierarchy = false;
        } else {
            isHierarchy = bosClientProxy.isHierarchyBucket(this.bucketName);
        }

        if (conf.getBoolean(BaiduBosConfigKeys.BOS_FILE_META_CACHE_ENABLE, false)) {
            fileMetaCache = new FileMetaCache(conf.getInt(BaiduBosConfigKeys.BOS_FILE_META_CACHE_SIZE, -1),
                    conf.getInt(BaiduBosConfigKeys.BOS_FILE_META_CACHE_EXPIRE, -1),
                    conf.getInt(BaiduBosConfigKeys.BOS_FILE_META_CACHE_REFRESH, -1),
                    conf.getBoolean(BaiduBosConfigKeys.BOS_FILE_META_CACHE_STATS, false));
        }
    }

    public boolean isHierarchy() {
        return this.isHierarchy;
    }

    public synchronized void close() {
        try {
            if (fileMetaCache != null) {
                fileMetaCache.printCacheStats();
            }
            if (bosClientProxy != null) {
                bosClientProxy.close();
                bosClientProxy = null;
            }
            if (renamePool != null) {
                renamePool.shutdownNow();
                renamePool = null;
            }
        } catch (Exception e) {
            LOG.warn("bos client close fail:" + e);
        }
    }

    private void completeMultipartUpload(String bucketName, String key, String uploadId, List<PartETag> eTags)
            throws IOException {
        Collections.sort(eTags, new Comparator<PartETag>() {

            public int compare(PartETag arg1, PartETag arg2) {
                PartETag part1 = arg1;
                PartETag part2 = arg2;
                return part1.getPartNumber() - part2.getPartNumber();
            }
        });
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, key, uploadId, eTags);
        bosClientProxy.completeMultipartUpload(completeMultipartUploadRequest);
        if (fileMetaCache != null) {
            fileMetaCache.invalidate(key);
        }
    }

    private int calPartCount(File file) {
        int partCount = (int) (file.length() / multipartBlockSize);
        if (file.length() % multipartBlockSize != 0) {
            partCount++;
        }
        return partCount;
    }

    private String initMultipartUpload(String key) throws IOException {
        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResponse initiateMultipartUploadResult =
                bosClientProxy.initiateMultipartUpload(initiateMultipartUploadRequest);
        return initiateMultipartUploadResult.getUploadId();
    }

    public void storeEmptyFile(String key) throws IOException {
        bosClientProxy.putEmptyObject(bucketName, key);
        if (fileMetaCache != null) {
            fileMetaCache.invalidate(key);
        }
    }

    public OutputStream createFile(String key, Configuration conf)
            throws IOException {

        return new BosOutputStream(key, conf) {


            private void createBlockBufferIfNull() throws IOException {
                if (this.currBlock == null) {
                    LOG.debug("Ready to get Block Buffer ! key is " + this.key
                            + ". blkIndex is " + this.blkIndex + ". blockSize is " + this.blockSize);
                    try {
                        if (createBlockSize < this.concurrentUpload) {
                            this.currBlock = new BosBlockBuffer(this.key, this.blkIndex, blockSize);
                            ++createBlockSize;
                            LOG.debug("create " + createBlockSize + "th BosBlockBuffer");
                        } else {
                            this.currBlock = this.blocks.take();
                            this.currBlock.setBlkId(this.blkIndex);
                            LOG.debug("get free block: " + this.currBlock);
                        }
                        LOG.debug("Block Buffer get ! key is " + this.key
                                + ". blkIndex is " + this.blkIndex + ". blockSize is " + this.blockSize);
                        this.bytesWrittenToBlock = 0;
                        this.blkIndex++;
                    } catch (Throwable throwable) {
                        LOG.error("catch exception when allocating BosBlockBuffer: ", throwable);
                        throw new IOException("catch exception when allocating BosBlockBuffer: ", throwable);
                    }
                }
            }

            @Override
            public synchronized void write(int b) throws IOException {
                if (this.closed) {
                    throw new IOException("Stream closed");
                }

                flush();
                createBlockBufferIfNull();

                this.currBlock.outBuffer.write(b);
                this.bytesWrittenToBlock++;
                this.filePos++;
            }

            @Override
            public synchronized void write(byte[] b, int off, int len) throws IOException {
                if (this.closed) {
                    throw new IOException("Stream closed");
                }

                while (len > 0) {
                    flush();
                    createBlockBufferIfNull();

                    int remaining = this.blockSize - this.bytesWrittenToBlock;
                    int toWrite = Math.min(remaining, len);
                    this.currBlock.outBuffer.write(b, off, toWrite);
                    this.bytesWrittenToBlock += toWrite;
                    this.filePos += toWrite;
                    off += toWrite;
                    len -= toWrite;
                }
            }

            @Override
            public synchronized void flush() throws IOException {
                if (this.closed) {
                    throw new IOException("Stream closed");
                }

                if (this.bytesWrittenToBlock >= this.blockSize) {
                    endBlock();
                }
            }

            private synchronized void endBlock() throws IOException {
                if (this.currBlock == null) {
                    return;
                }

                LOG.debug("Enter endBlock() ! key is " + this.key
                        + ". blkIndex is " + this.blkIndex + ". blockSize is " + this.blockSize
                        + ". Size of eTags is " + this.eTags.size()
                        + ". concurrentUpload is " + this.concurrentUpload);

                if (this.currBlock.getBlkId() == 1) {
                    this.uploadId = initMultipartUpload(this.key);
                    LOG.debug("init multipart upload! key is " + this.key
                            + " upload id is " + this.uploadId);
                }

                //
                // Move outBuffer to inBuffer
                //
                this.currBlock.outBuffer.close();
                this.currBlock.moveData();

                //
                // Block this when too many active UploadPartThread
                //
                if ((this.blkIndex - this.eTags.size()) > this.concurrentUpload) {
                    while (true) {
                        synchronized(this.blocksMap) {
                            try {
                                this.blocksMap.wait(10);
                            } catch (InterruptedException e) {
                            }
                        }
                        if ((this.blkIndex - this.eTags.size()) <= this.concurrentUpload) {
                            break;
                        }
                        if (this.exceptionMap.size() > 0) {
                            //
                            // Exception happens during upload
                            //
                            throw new IOException("Exception happens during upload:" + exceptionMap);
                        }
                    }
                }

                if (this.exceptionMap.size() > 0) {
                    //
                    // Exception happens during upload
                    //
                    throw new IOException("Exception happens during upload:" + exceptionMap);
                }

                synchronized(this.blocksMap) {
                    this.blocksMap.put(this.currBlock.getBlkId(), this.currBlock);
                }

                //
                // New a UploadPartThread and add it into ExecuteService pool
                //
                UploadPartThread upThread = new UploadPartThread(
                        bosClientProxy,
                        bucketName,
                        this.key,
                        this.currBlock,
                        this.uploadId,
                        this.eTags,
                        this.blocksMap,
                        this.exceptionMap,
                        this.conf,
                        this.blocks
                );
                this.futureList.add(pool.submit(upThread));

                // Clear current BosBlockBuffer
                this.currBlock = null;
            }

            @Override
            public void init() {
                this.blockSize = multipartBlockSize;
                this.concurrentUpload = multiUploadThreadNum;
                this.pool = new ThreadPoolExecutor(this.concurrentUpload,
                        this.concurrentUpload, 0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>());
                this.blocks = new ArrayBlockingQueue<BosBlockBuffer>(concurrentUpload);

            }

            public synchronized void putObject() throws IOException {
                if (this.currBlock == null) {
                    return;
                }

                //
                // Move outBuffer to inBuffer
                //
                this.currBlock.outBuffer.close();
                this.currBlock.moveData();

                ObjectMetadata objectMeta = new ObjectMetadata();
                objectMeta.setContentType("binary/octet-stream");
                objectMeta.setContentLength(this.currBlock.inBuffer.getLength());
                bosClientProxy.putObject(bucketName, this.key, this.currBlock.inBuffer, objectMeta);
                this.currBlock = null;
                if (fileMetaCache != null) {
                    fileMetaCache.invalidate(this.key);
                }
            }

            @Override
            public synchronized void close() throws IOException {
                if (this.closed) {
                    return;
                }

                if (this.filePos == 0) {
                    storeEmptyFile(this.key);
                } else if (null != this.currBlock && this.currBlock.getBlkId() == 1) {
                    putObject();
                } else {
                    if (this.bytesWrittenToBlock != 0) {
                        endBlock();
                    }

                    LOG.debug("start to wait upload part threads done. futureList size: " + futureList.size());
                    // wait UploadPartThread threads done
                    try {
                        int index = 0;
                        for (Future future : this.futureList) {
                            future.get();
                            index += 1;
                            LOG.debug("future.get() index: " + index + " is done");
                        }
                    } catch (Exception e) {
                        LOG.warn("catch exception when waiting UploadPartThread done: ", e);
                    }

                    LOG.debug("success to wait upload part threads done");

                    LOG.debug("Size of eTags is " + this.eTags.size() + ". blkIndex is " + this.blkIndex);

                    try {
                        if (this.eTags.size() != this.blkIndex - 1) {
                            throw new IllegalStateException("Multipart failed!");
                        }

                        completeMultipartUpload(bucketName, this.key, this.uploadId, this.eTags);
                        if (this.eTags.size() != this.blkIndex - 1) {
                            throw new IllegalStateException("Multipart failed!");
                        }
                    } finally {
                        // release block from queue
                        if (this.pool != null) {
                            this.pool.shutdownNow();
                        }
                    }
                }

                super.close();
                this.closed = true;
            }
        };
    }

    public class FileMetaCache {
        LoadingCache<String, FileMetadata> cache = null;
        private static final int DEFAULT_CACHE_SIZE = 1000;
        private static final int DEFAULT_EXPIRE_TIME_SEC = 5;
        private static final int DEFAULT_REFRESH_TIME_SEC = 3;
        private final FileMetadata fileNotExist = new FileMetadata("", -1, -1, false);

        public FileMetaCache() {
            this(DEFAULT_CACHE_SIZE, DEFAULT_EXPIRE_TIME_SEC, DEFAULT_REFRESH_TIME_SEC, false);
        }

        public FileMetaCache(int expire, int refresh) {
            this(DEFAULT_CACHE_SIZE, expire, refresh, false);
        }

        public FileMetaCache(int size, int expire, int refresh, boolean stats) {
            if (size <= 0) {
                size = DEFAULT_CACHE_SIZE;
            }
            if (expire <= 0) {
                expire = DEFAULT_EXPIRE_TIME_SEC;
            }
            if (refresh <= 0) {
                refresh = DEFAULT_REFRESH_TIME_SEC;
            }
            CacheBuilder builder = CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .initialCapacity(size) // 不触发扩容操作
                    .maximumSize(size)
                    .expireAfterWrite(expire, TimeUnit.SECONDS)
                    .refreshAfterWrite(refresh, TimeUnit.SECONDS);
            if (stats) {
                //记录缓存情况，调试用
                builder.recordStats();
            }
            cache = builder.build(new BOSMetaCacheLoader());
        }

        public FileMetadata get(String key) {
            FileMetadata metadata = null;
            try {
                metadata = cache.get(key);
            } catch (ExecutionException ignore) {

            }
            if (metadata == fileNotExist) {
                cache.invalidate(key);
                metadata = null;
            }
            return metadata;
        }

        public void put(String key, FileMetadata metadata) {
            cache.put(key, metadata);
        }

        public void invalidate(String key) {
            cache.invalidate(key);
        }

        public void invalidateBatch(Iterable<String> keys) {
            cache.invalidateAll(keys);
        }

        public void invalidatePath(String prefix) {
            Set<String> originSets = cache.asMap().keySet();
            HashSet<String> invalidSets = new HashSet<>();
            for (String key : originSets) {
                if (key.startsWith(prefix)) {
                    invalidSets.add(key);
                }
            }
            cache.invalidateAll(invalidSets);

        }

        public void printCacheStats() {
            if (cache != null) {
                LOG.debug("FileMetaCache report: " + cache.stats().toString());
            }
        }

        /**
         * load object metadata when cache miss
         */
        public class BOSMetaCacheLoader extends CacheLoader<String, FileMetadata> {
            @Override
            public FileMetadata load(String key) throws Exception {
                FileMetadata metadata = null;
                try {
                    metadata = doRetrieveMetadata(key);
                    //LOG.debug(Thread.currentThread().getName() + " load metadata from bos success, key:" + key);
                } catch (IOException e) {
                    return fileNotExist;
                }
                // CacheLoader can't return null
                if (metadata == null) {
                    metadata = fileNotExist;
                }
                return metadata;
            }
        }

    }

    public FileMetadata retrieveMetadata(String key) throws IOException {
        if (fileMetaCache != null) {
            return fileMetaCache.get(key);
        }
        return doRetrieveMetadata(key);
    }

    public FileMetadata doRetrieveMetadata(String key) throws IOException {
        boolean isFolder = false;
        if (!this.isHierarchy) {
            try {
                ObjectMetadata meta = bosClientProxy.getObjectMetadata(bucketName, key);
                if (key.endsWith(BaiduBosFileSystem.PATH_DELIMITER)) {
                    isFolder = true;
                }
                return new FileMetadata(key, meta.getContentLength(), meta.getLastModified().getTime(), isFolder);
            } catch (FileNotFoundException e) {
                try {
                    ObjectMetadata meta = bosClientProxy.getObjectMetadata(bucketName,
                            key + BaiduBosFileSystem.PATH_DELIMITER);
                    return new FileMetadata(key, meta.getContentLength(), meta.getLastModified().getTime(), true);
                } catch (FileNotFoundException ex) {
                    try {
                        PartialListing listing = list(prefixToDir(key), 1, null);
                        if (listing != null) {
                            if (listing.getFiles().length > 0 || (listing.getCommonPrefixes() != null &&
                                    listing.getCommonPrefixes().length > 0)) {
                                return new FileMetadata(key, 0, 0, true);
                            }
                        }
                    } catch (FileNotFoundException exc) {
                        throw new FileNotFoundException(key + ": No such file or directory.");
                    }
                }
            }
        } else {
            try {
                ObjectMetadata meta = bosClientProxy.getObjectMetadata(bucketName, key);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("isHierarchy Getting metadata: " + meta.toString() + " for key: " + key +
                            " from bucket:" + bucketName);
                    LOG.debug(key + " get object type return:" + meta.getObjectType());
                }
                if (meta.getObjectType() != null && meta.getObjectType().equals("Directory")) {
                    isFolder = true;
                }
                return new FileMetadata(key, meta.getContentLength(), meta.getLastModified().getTime(), isFolder);
            } catch (FileNotFoundException e) {
                throw new FileNotFoundException(key + ": No such file or directory.");
            }
        }

        return null;
    }

    /**
     * @param key
     * The key is the object name that is being retrieved from the S3 bucket
     * @return
     * This method returns null if the key is not found
     * @throws IOException
     */
    public InputStream retrieve(String key) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting key: " + key + " from bucket:" + bucketName);
        }
        BosObject object = bosClientProxy.getObject(bucketName, key);
        return object.getObjectContent();
    }

    /**
     *
     * @param key
     * The key is the object name that is being retrieved from the S3 bucket
     * @return
     * This method returns null if the key is not found
     * @throws IOException
     */
    public InputStream retrieve(String key, long byteRangeStart, long byteRangeEnd) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting key: " + key + " from bucket:" + bucketName + " with byteRangeStart: "
                    + byteRangeStart + " byteRangeEnd: " + byteRangeEnd);
        }

        ObjectMetadata meta = bosClientProxy.getObjectMetadata(bucketName, key);
        GetObjectRequest request = new GetObjectRequest(bucketName, key);
        //Due to the InvalidRange exception of bos, we shouldn't set range for empty file
        if (meta.getContentLength() != 0) {
            request.setRange(byteRangeStart, byteRangeEnd);
        }
        BosObject object = bosClientProxy.getObject(request);
        return object.getObjectContent();
    }

    /**
     *
     * @param key
     * The key is the object name that is being retrieved from the S3 bucket
     * @param meta
     * The meta must be the specif key
     * @return
     * This method returns null if the key is not found
     * @throws IOException
     */
    public InputStream retrieve(String key, long byteRangeStart,
                                long byteRangeEnd, FileMetadata meta) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting key: " + key + " from bucket:" + bucketName + " with byteRangeStart: "
                    + byteRangeStart + " byteRangeEnd: " + byteRangeEnd);
        }
        if (meta == null || !meta.getKey().equals(key)) {
            throw new IOException("FileMetadata not match key:" + key);
        }
        GetObjectRequest request = new GetObjectRequest(bucketName, key);
        //Due to the InvalidRange exception of bos, we shouldn't set range for empty file
        if (meta.getLength() != 0) {
            request.setRange(byteRangeStart, byteRangeEnd);
        }
        BosObject object = bosClientProxy.getObject(request);
        return object.getObjectContent();
    }

    public FileChecksum getFileChecksum(String key) throws IOException {
        try {
            ObjectMetadata metadata = bosClientProxy.getObjectMetadata(bucketName, key);
            if (metadata.getxBceCrc32c() == null) {
                return null;
            }
            return new BOSCRC32CCheckSum(metadata.getxBceCrc32c());
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException(key + ": No such file or directory.");
        }
    }

    public String prefixToDir(String prefix) {
        if (prefix != null && prefix.length() > 0 && !prefix.endsWith(BaiduBosFileSystem.PATH_DELIMITER)) {
            prefix += BaiduBosFileSystem.PATH_DELIMITER;
        }

        return prefix;
    }
    public PartialListing list(String prefix, int maxListingLength)
            throws IOException {
        return list(prefix, maxListingLength, null);
    }

    public PartialListing list(String prefix, int maxListingLength, String priorLastKey) throws IOException {
        return list(prefix, BaiduBosFileSystem.PATH_DELIMITER, maxListingLength, priorLastKey);
    }

    public PartialListing list(String prefix, int maxListingLength, String priorLastKey, String delimiter)
            throws IOException {
        return list(prefix, delimiter, maxListingLength, priorLastKey);
    }

    /**
     *
     * @return
     * This method returns null if the list could not be populated
     * due to S3 giving ServiceException
     * @throws IOException
     */

    private PartialListing list(String prefix, String delimiter,
                                int maxListingLength, String priorLastKey) throws IOException {
        ListObjectsRequest request = new ListObjectsRequest(bucketName);
        request.setPrefix(prefix);
        request.setMarker(priorLastKey);
        if (delimiter != null) {
            request.setDelimiter(delimiter);
        }
        request.setMaxKeys(maxListingLength);

        ListObjectsResponse objects = bosClientProxy.listObjects(request);

        List<FileMetadata> fileMetadata = new LinkedList<FileMetadata>();
        for (int i = 0; i < objects.getContents().size(); i++) {
            BosObjectSummary object = objects.getContents().get(i);
            if (object.getKey() != null && object.getKey().length() > 0) {
                fileMetadata.add(new FileMetadata(object.getKey(),
                        object.getSize(), object.getLastModified().getTime(), false));
            }
        }

        String[] commonPrefix = null;
        if (objects.getCommonPrefixes() != null) {
            commonPrefix = objects.getCommonPrefixes().toArray(new String[objects.getCommonPrefixes().size()]);
        }

        return new PartialListing(objects.getNextMarker(),
                fileMetadata.toArray(new FileMetadata[fileMetadata.size()]),
                commonPrefix);
    }

    public void deleteNsDir(String key, boolean recursive) throws IOException {
        if (!recursive) {
            PartialListing listing = list(key, 1, null, "/");
            if (listing.getFiles().length + listing.getCommonPrefixes().length > 0) {
                throw new IOException("Directory " + key + " is not empty.");
            }
            delete(key);
        } else {
            String marker = null;
            DeleteDirectoryResponse response = null;
            do {
                response = bosClientProxy.deleteDirectory(bucketName, key, true, marker);
                if (response.isTruncated()) {
                    marker = response.getNextDeleteMarker();
                }
            } while (response.isTruncated());
        }
        if (fileMetaCache != null) {
            fileMetaCache.invalidatePath(key);
        }
    }

    public void deleteDirs(String key, boolean recursive) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting dirs key:" + key + " from bucket" + bucketName);
        }
        if (this.isHierarchy) {
            deleteNsDir(key, recursive);
            return;
        }
        List<Future> futures = null;
        String listKey = key;
        ArrayList<String> keys = new ArrayList<String>();
        String priorLastKey = null;

        do {
            keys.clear();
            PartialListing listing = list(listKey, 1000, priorLastKey, null);
            if (listing != null) {
                DeleteMultipleObjectsRequest deleteMultipleObjectsRequest =
                        (new DeleteMultipleObjectsRequest()).withBucketName(bucketName);
                for (FileMetadata fileMetadata : listing.getFiles()) {
                    if (fileMetadata.getKey() != null && fileMetadata.getKey().length() > 0) {
                        keys.add(fileMetadata.getKey());
                        // Fix bug: if the key path that concurrently created by multi nodes is empty dir,
                        // it is  invalid to delete it. The resolution is to store an empty file in the path before deleting.
                        if (fileMetadata.getKey().endsWith(BaiduBosFileSystem.PATH_DELIMITER)) {
                            this.storeEmptyFile(fileMetadata.getKey());
                        }
                    }
                }
                if (!recursive && (keys.size() > 1 || !prefixToDir(keys.get(0)).equals(prefixToDir(key)))) {
                    throw new IOException("Directory " + key + " is not empty.");
                }
                if (!keys.isEmpty()) {
                    deleteMultipleObjectsRequest.setObjectKeys(keys);
                    bosClientProxy.deleteMultipleObjects(deleteMultipleObjectsRequest);
                }
                priorLastKey = listing.getPriorLastKey();
            }
        } while (priorLastKey != null && priorLastKey.length() > 0);

        if (fileMetaCache != null) {
            fileMetaCache.invalidatePath(key);
        }
    }

    public void delete(String key) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting key:" + key + "from bucket" + bucketName);
        }
        bosClientProxy.deleteObject(bucketName, key);
        if (fileMetaCache != null) {
            fileMetaCache.invalidate(key);
        }
    }

    public void copy(String srcKey, String dstKey) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Copying srcKey: " + srcKey + "to dstKey: " + dstKey + "in bucket: " + bucketName);
        }
        bosClientProxy.copyObject(bucketName, srcKey, bucketName, dstKey);
    }

    public void copyLargeFile(String srcKey, String dstKey, long fileSize) throws IOException {
        String copyUploadId = initMultipartUpload(dstKey);
        long leftSize = fileSize;
        long skipBytes = 0;
        int partNumber = 1;
        List<PartETag> partETags = new ArrayList<PartETag>();
        long partSize = multipartBlockSize;

        while (leftSize > 0) {
            if (leftSize < partSize) {
                partSize = leftSize;
            }
            UploadPartCopyRequest uploadPartCopyRequest = new UploadPartCopyRequest();
            uploadPartCopyRequest.setBucketName(bucketName);
            uploadPartCopyRequest.setKey(dstKey);
            uploadPartCopyRequest.setSourceBucketName(bucketName);
            uploadPartCopyRequest.setSourceKey(srcKey);
            uploadPartCopyRequest.setUploadId(copyUploadId);
            uploadPartCopyRequest.setPartSize(partSize);
            uploadPartCopyRequest.setOffSet(skipBytes);
            uploadPartCopyRequest.setPartNumber(partNumber);
            UploadPartCopyResponse uploadPartCopyResponse = bosClientProxy.uploadPartCopy(uploadPartCopyRequest);
            // 将返回的PartETag保存到List中
            PartETag partETag = new PartETag(partNumber,uploadPartCopyResponse.getETag());
            partETags.add(partETag);
            leftSize -= partSize;
            skipBytes += partSize;
            partNumber+=1;
        }

        completeMultipartUpload(bucketName, dstKey, copyUploadId, partETags);
    }

    /**
     *
     * @param srcKey src object
     * @param dstKey dst object
     * @param isFile srcKey is a file or dir
     * @throws IOException
     */
    public void rename(String srcKey, String dstKey, final boolean isFile)
            throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Renaming srcKey: " + srcKey + "to dstKey: " + dstKey + "in bucket: " + bucketName);
        }
        List<Future> futures = new ArrayList<Future>();
        try {
            if (this.isHierarchy) {
                bosClientProxy.renameObject(bucketName, srcKey, dstKey);
            } else if (isFile) {
                FileMetadata fileMetadata = this.retrieveMetadata(srcKey);
                long blockSize = multipartBlockSize;
                if (fileMetadata.getLength() <= blockSize) {
                    // simple copy
                    this.copy(srcKey, dstKey);
                } else {
                    // three step copy
                    this.copyLargeFile(srcKey, dstKey, fileMetadata.getLength());
                }
                this.delete(srcKey);
            } else {
                // hadoop will not rename file
                // Move the folder object
                // srcKeys.add(srcKey + FOLDER_SUFFIX);
                // store.storeEmptyFile(dstKey + FOLDER_SUFFIX);
                // srcKey may be created by multiple nodes simultaneously,
                // so have to store an empty file to cover it
                this.storeEmptyFile(srcKey + BaiduBosFileSystem.PATH_DELIMITER);

                // Move everything inside the folder
                String priorLastKey = null;
                if (renamePool == null) {
                    renamePool = renameConcurrentSize > 1 ? new ThreadPoolExecutor(renameConcurrentSize,
                            renameConcurrentMaxSize, 0L, TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<Runnable>()) : null;
                }

                do {
                    PartialListing listing = this.list(this.prefixToDir(srcKey),
                            1000, priorLastKey, null);
                    for (FileMetadata file : listing.getFiles()) {
                        if (renamePool == null) {
                            bosClientProxy.renameObject(bucketName, file.getKey(),
                                    dstKey + file.getKey().substring(srcKey.length()));
                        } else {
                            futures.add(renamePool.submit(new RenameTask(this, file.getKey(), dstKey
                                    + file.getKey().substring(srcKey.length()))));
                        }
                    }
                    priorLastKey = listing.getPriorLastKey();
                } while (priorLastKey != null);

                if (renamePool != null) {
                    for (Future future : futures) {
                        try {
                            future.get();
                        } catch (Exception e) {
                            future.cancel(true);
                            throw new IOException(e);
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Renaming srcKey: " + srcKey + "to dstKey: " + dstKey + "in bucket: " +
                        bucketName + "failed, FileNotFoundException");
            }
            throw e;
        } finally {
            if (fileMetaCache != null) {
                if (isFile) {
                    fileMetaCache.invalidate(srcKey);
                    fileMetaCache.invalidate(dstKey);
                } else {
                    fileMetaCache.invalidatePath(srcKey);
                    fileMetaCache.invalidatePath(dstKey);
                }
            }
        }
    }

    private static class RenameTask implements Callable<Void> {

        private String srcKey;
        private String dstKey;
        private BosNativeFileSystemStore store;

        public RenameTask(BosNativeFileSystemStore store, String srcKey, String dstKey) {
            this.store = store;
            this.srcKey = srcKey;
            this.dstKey = dstKey;
        }

        public Void call() throws IOException {
            store.rename(srcKey, dstKey, true);
            return null;
        }
    }

    private static class DeleteTask implements Callable<Void> {
        private String key;
        private BosNativeFileSystemStore store;

        public DeleteTask(BosNativeFileSystemStore store, String key) {
            this.store = store;
            this.key = key;
        }

        public Void call() throws IOException {
            store.delete(key);
            return null;
        }
    }

    public void purge(String prefix) throws IOException {
        ListObjectsResponse objects = bosClientProxy.listObjects(bucketName, prefix);
        for (BosObjectSummary object : objects.getContents()) {
            bosClientProxy.deleteObject(bucketName, object.getKey());
        }
    }

    public void dump() throws IOException {
        StringBuilder sb = new StringBuilder("BOS Native Filesystem, ");
        sb.append(bucketName).append("\n");
        ListObjectsResponse objects = bosClientProxy.listObjects(bucketName);
        for (BosObjectSummary object : objects.getContents()) {
            sb.append(object.getKey()).append("\n");
        }
    }

    private static class UploadPartTask implements Callable<UploadPartResponse> {
        private BosClientProxy client;
        private UploadPartRequest uploadPartRequest;

        public UploadPartTask(BosClientProxy client, String bucket, String object,
                              String uploadId, InputStream in, long size, int partId) {
            this.client = client;
            UploadPartRequest uploadPartRequest = new UploadPartRequest();
            uploadPartRequest.setBucketName(bucket);
            uploadPartRequest.setKey(object);
            uploadPartRequest.setUploadId(uploadId);
            uploadPartRequest.setInputStream(in);
            uploadPartRequest.setPartSize(size);
            uploadPartRequest.setPartNumber(partId);
            this.uploadPartRequest = uploadPartRequest;
        }

        public UploadPartResponse call() throws Exception {
            return this.client.uploadPart(this.uploadPartRequest);
        }
    }

    private static class UploadPartThread implements Runnable {
        private Configuration conf;
        private File uploadFile;
        private String bucket;
        private String object;
        private long start;
        private long size;
        private List<PartETag> eTags;
        private int partId;
        private BosClientProxy client;
        private String uploadId;
        private InputStream in;
        private Map<Integer, BosBlockBuffer> dataMap;
        private Map<Integer, Exception> exceptionMap;
        private ArrayBlockingQueue<BosBlockBuffer> blocks;
        private BosBlockBuffer block;

        UploadPartThread(BosClientProxy client, String bucket, String object,
                         File uploadFile, String uploadId, int partId,
                         long start, long partSize, List<PartETag> eTags, Configuration conf) throws IOException {
            this.uploadFile = uploadFile;
            this.bucket = bucket;
            this.object = object;
            this.start = start;
            this.size = partSize;
            this.eTags = eTags;
            this.partId = partId;
            this.client = client;
            this.uploadId = uploadId;
            this.in = new FileInputStream(this.uploadFile);
            this.in.skip(this.start);
            this.conf = conf;
        }

        UploadPartThread(BosClientProxy client, String bucket, String object,
                         InputStream in, String uploadId, int partId,
                         long partSize, List<PartETag> eTags, Map<Integer, BosBlockBuffer> dataMap,
                         Map<Integer, Exception> exceptionMap, Configuration conf) throws IOException {
            this.bucket = bucket;
            this.object = object;
            this.size = partSize;
            this.eTags = eTags;
            this.partId = partId;
            this.client = client;
            this.uploadId = uploadId;
            this.in = in;
            this.dataMap = dataMap;
            this.exceptionMap = exceptionMap;
            this.conf = conf;
        }


        UploadPartThread(BosClientProxy client, String bucket, String object, BosBlockBuffer block,
                         String uploadId, List<PartETag> eTags, Map<Integer, BosBlockBuffer> dataMap,
                         Map<Integer, Exception> exceptionMap, Configuration conf,
                         ArrayBlockingQueue<BosBlockBuffer> blocks) throws IOException {
            this.bucket = bucket;
            this.object = object;
            this.size = block.inBuffer.getLength();
            this.eTags = eTags;
            this.partId = block.getBlkId();
            this.client = client;
            this.uploadId = uploadId;
            this.in = block.inBuffer;
            this.dataMap = dataMap;
            this.exceptionMap = exceptionMap;
            this.conf = conf;
            this.blocks = blocks;
            this.block = block;
        }

        private int getMultiUploadAttempts() {
            return Math.max(this.conf.getInt("fs.bos.multipart.uploads.attempts", 5), 3);
        }

        private int getMultiUploadRetryInterval() {
            return this.conf.getInt("fs.bos.multipart.uploads.retry.interval", 1000);
        }

        private double getTimeoutFactor() {
            double minFactor = 10.0;
            return Math.max(this.conf.getDouble("fs.bos.multipart.uploads.factor", minFactor), minFactor);
        }

        private long getUploadSpeed() {
            long defaultUploadSpeed = 10 * 1024 * 1024;
            long uploadSpeed = this.conf.getLong("fs.bos.multipart.uploads.speed", defaultUploadSpeed);
            return uploadSpeed <= 0 ? defaultUploadSpeed : uploadSpeed;
        }

        private long getTimeout() {
            long uploadSpeed = this.getUploadSpeed();
            double factor = this.getTimeoutFactor();
            long minTimeout = 120000;
            double uploadTime = (double) this.size * 1000 / uploadSpeed;
            return Math.max((long) (uploadTime * factor), minTimeout);
        }

        public void run() {
            try {
                String partInfo = object + " with part id " + partId;
                LOG.debug("start uploading " + partInfo);
                int attempts = this.getMultiUploadAttempts();
                int retryInterval = this.getMultiUploadRetryInterval();
                long timeout = this.getTimeout();
                boolean isSuccess = false;
                UploadPartResponse uploadPartResult = null;
                for (int i = 0; i < attempts; i++) {
                    try {
                        in.reset();
                    } catch (Exception e) {
                        LOG.warn("failed to reset inputstream for " + partInfo);
                        break;
                    }
                    ExecutorService subPool = Executors.newSingleThreadExecutor();
                    UploadPartTask task = new UploadPartTask(this.client, bucket, object, uploadId, in, size, partId);
                    Future<UploadPartResponse> future = subPool.submit(task);
                    try {
                        LOG.debug("[upload attempt " + i + " timeout: " + timeout + "] start uploadPart " + partInfo);
                        uploadPartResult = future.get(timeout, TimeUnit.MILLISECONDS);
                        LOG.debug("[upload attempt " + i + " timeout: " + timeout + "] end uploadPart " + partInfo);
                        isSuccess = true;
                        break;
                    } catch (Exception e) {
                        future.cancel(true);
                        LOG.error("[upload attempt " + i + "] failed to upload " + partInfo + ": " + e);
                        Thread.sleep(retryInterval);
                    } finally {
                        if (subPool != null) {
                            subPool.shutdown();
                            while (!subPool.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
                                LOG.warn("waiting for all task to be done");
                            }
                        }
                    }
                }

                if (!isSuccess) {
                    BceServiceException e = new BceServiceException("failed to upload part " + partInfo);
                    exceptionMap.put(partId, e);
                    LOG.error("failed to upload " + partInfo + " after " + attempts + " attempts");
                } else {
                    eTags.add(uploadPartResult.getPartETag());
                    if (dataMap != null) {
                        synchronized(dataMap) {
                            dataMap.get(partId).clear();
                            dataMap.notify();
                        }
                    }
                    LOG.debug("end uploading key " + partInfo);
                }
            } catch (BceServiceException boe) {
                LOG.error("BOS Error code: " + boe.getErrorCode() + "; BOS Error message: " + boe.getErrorMessage());
                exceptionMap.put(partId, boe);
            } catch (Exception e) {
                LOG.error("Exception catched: ", e);
                exceptionMap.put(partId, e);
            } finally {
                try {
                    if (dataMap != null) {
                        this.blocks.put(dataMap.get(partId));
                    }
                    if (in != null) {
                        in.close();
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    abstract static class BosOutputStream extends OutputStream {
        public final Map<Integer, BosBlockBuffer> blocksMap = new HashMap<Integer, BosBlockBuffer>();
        public String key;
        public Configuration conf;
        public int blockSize;
        public BosBlockBuffer currBlock;
        public int blkIndex = 1;
        public boolean closed;
        public long filePos = 0;
        public int bytesWrittenToBlock = 0;
        public ExecutorService pool = null;
        public String uploadId = null;
        public int concurrentUpload;
        public List<PartETag> eTags = Collections.synchronizedList(new ArrayList<PartETag>());
        public List<Future> futureList = new LinkedList<Future>();
        public Map<Integer, Exception> exceptionMap = new ConcurrentHashMap<Integer, Exception>();
        public ArrayBlockingQueue<BosBlockBuffer> blocks;
        public int createBlockSize;

        public BosOutputStream(String key, Configuration conf) {
            this.key = key;
            this.conf = conf;
            this.init();
            createBlockSize = 0;
        }

        public abstract void init();
    }
}

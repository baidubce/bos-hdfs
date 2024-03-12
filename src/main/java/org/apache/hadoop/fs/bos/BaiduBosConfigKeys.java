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

public class BaiduBosConfigKeys {
    public static final long DEFAULT_BOS_BLOCK_SIZE = 128 * 1024 * 1024L;

    public static final long DEFAULT_BOS_READ_AHEAD_SIZE = 4 * 1024 * 1024;

    public static final int DEFAULT_BOS_READ_BUFFER_SIZE = 128 * 1024;

    public static final int BOS_MAX_LISTING_LENGTH = 1000;

    public static final int DEFAULT_TAIL_CACHE_TRIGGER_THRESHOLD = 1 * 1024;

    public static final int DEFAULT_TAIL_CACHE_SIZE = 4 * 1024;

    public static final int DEFAULT_BOS_MULTI_UPLOAD_BLOCK_SIZE = 12 * 1024 * 1024;

    public static final int DEFAULT_MULTI_UPLOAD_CONCURRENT_SIZE = 10;

    public static final String BOS_FILESYSTEM_USER_AGENT = "bos-hdfs-sdk/1.0.4-community";

    public static final String BOS_BUCKET_HIERARCHY = "fs.bos.bucket.hierarchy";

    public static final String NEED_DIRECT_LIST = "fs.bos.object.directlist";

    public static final String BOS_BLOCK_SIZE = "fs.bos.block.size";

    public static final String BOS_READAHEAD_SIZE = "fs.bos.readahead.size";

    public static final String BOS_READ_BUFFER_SIZE = "fs.bos.read.buffer.size";

    public static final String NEED_SHOW_DIR_TIME = "fs.bos.object.dir.showtime";

    public static final String NEED_RECURSIVE_SUMMARY = "fs.bos.content.summary.recursive";

    public static final String BOS_CRC32C_CHECKSUM_ENABLE = "fs.bos.crc32c.checksum.enable";

    public static final String TAIL_CACHE_ENABLE = "fs.bos.tail.cache.enable";

    public static final String TAIL_CACHE_SIZE = "fs.bos.tail.cache.size";

    public static final String TAIL_CACHE_TRIGGER_THRESHOLD = "fs.bos.tail.cache.trigger.threshold";

    public static final String BOS_MULTI_UPLOAD_BLOCK_SIZE = "fs.bos.multipart.uploads.block.size";

    public static final String BOS_MULTI_UPLOAD_CONCURRENT_SIZE = "fs.bos.multipart.uploads.concurrent.size";

    public static final String BOS_RENAME_CONCURRENT_SIZE = "fs.bos.rename.concurrent.size";

    public static final String BOS_RENAME_CONCURRENT_MAX_SIZE = "fs.bos.rename.concurrent.max.size";

    public static final String BOS_FILE_META_CACHE_ENABLE = "fs.bos.filemeta.cache.enable";

    public static final String BOS_FILE_META_CACHE_REFRESH = "fs.bos.filemeta.cache.refresh_sec";

    public static final String BOS_FILE_META_CACHE_EXPIRE = "fs.bos.filemeta.cache.expire_sec";

    public static final String BOS_FILE_META_CACHE_SIZE = "fs.bos.filemeta.cache.size";

    public static final String BOS_FILE_META_CACHE_STATS = "fs.bos.filemeta.cache.stats";

}

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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.util.Progressable;

public class BaiduBosFileSystem extends FileSystem {

    public static final Log LOG =
            LogFactory.getLog(BaiduBosFileSystem.class);
    public static final String PATH_DELIMITER = Path.SEPARATOR;
    private static final String FOLDER_SUFFIX = "/";

    private URI uri;
    private BosNativeFileSystemStore store;
    private Path workingDir;
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private boolean crc32cChecksumEnable;

    private long readAheadSize;
    private int readBufferSize;
    private boolean tailCacheEnable;
    private int tailCacheTriggerThreshold;
    private int tailCacheSize;

    public BaiduBosFileSystem() {
    }

    public BaiduBosFileSystem(BosNativeFileSystemStore store) {
        this.store = store;
    }

    private static String pathToKey(Path path) {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Path must be absolute: " + path);
        }
        return path.toUri().getPath().substring(1); // remove initial slash
    }

    private static Path keyToPath(String key) {
        return new Path("/" + key);
    }

    @Override
    public String getScheme() {
        return "bos";
    }

    @Override
    public void close() throws IOException {
        try {
            this.isRunning.set(false);
            this.store.close();
        } finally {
            super.close();
        }
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        if (store == null) {
            store = new BosNativeFileSystemStore();
        }
        store.initialize(uri, conf);
        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir =
                new Path("/user", System.getProperty("user.name")).makeQualified(this);

        this.readAheadSize = getConf().getLong(BaiduBosConfigKeys.BOS_READAHEAD_SIZE, BaiduBosConfigKeys.DEFAULT_BOS_READ_AHEAD_SIZE);
        this.readBufferSize = getConf().getInt(BaiduBosConfigKeys.BOS_READ_BUFFER_SIZE, BaiduBosConfigKeys.DEFAULT_BOS_READ_BUFFER_SIZE);
        this.tailCacheEnable = getConf().getBoolean(BaiduBosConfigKeys.TAIL_CACHE_ENABLE, true);
        this.tailCacheTriggerThreshold = getConf().getInt(BaiduBosConfigKeys.TAIL_CACHE_TRIGGER_THRESHOLD,
                BaiduBosConfigKeys.DEFAULT_TAIL_CACHE_TRIGGER_THRESHOLD);
        this.tailCacheSize = getConf().getInt(BaiduBosConfigKeys.TAIL_CACHE_SIZE, BaiduBosConfigKeys.DEFAULT_TAIL_CACHE_SIZE);
        this.crc32cChecksumEnable = getConf().getBoolean(BaiduBosConfigKeys.BOS_CRC32C_CHECKSUM_ENABLE, false);
        this.isRunning.set(true);
    }

    private void checkOpen() throws IOException {
        if (!isRunning.get()) {
            throw new IOException("Filesystem closed");
        }
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }


//    TODO biemingzhou@baidu.com 由于引入hadoop依赖版本为2.6.0,父类方法没有,后续更新依赖时记得打开重写,否则影响HBASE
//    @Override
    public void setStoragePolicy(final Path src, final String policyName)
            throws IOException {
        LOG.debug("Bos Simulate setStoragePolicy function, path:" + src + " , policyName: " + policyName);
    }

    /** This optional operation is not yet supported. */
    public FSDataOutputStream append(Path f, int bufferSize,
                                     Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        if (!crc32cChecksumEnable) {
            return super.getFileChecksum(f, length);
        }
        LOG.debug("call the checksum for the path:" + f.getName());
        checkOpen();
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        return this.store.getFileChecksum(key);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
            final PathFilter filter)
            throws IOException {
        return new RemoteIterator<LocatedFileStatus>() {
            private final FileStatus[] stats = listStatus(f, filter);
            private int i = 0;

            public boolean hasNext() {
                return i < stats.length;
            }

            public LocatedFileStatus next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more entry in " + f);
                }
                FileStatus result = stats[i++];
                BlockLocation[] locs = result.isFile() ?
                        getFileBlockLocations(result, 0, result.getLen()) :
                        null;
                return new LocatedFileStatus(result, locs);
            }
        };
    }
    
    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
                                     boolean overwrite, int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
        checkOpen();
        if (!overwrite && exists(f)) {
            throw new IOException("File already exists:" + f);
        }

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        return new FSDataOutputStream(store.createFile(key, getConf()), statistics);
    }

    @Override
    @Deprecated
    public boolean delete(Path path) throws IOException {
        return delete(path, true);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        checkOpen();
        FileStatus status = null;
        try {
            status = getFileStatus(f);
        } catch (FileNotFoundException e) {
            return false;
        }
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        if (status.isDirectory()) {
            store.deleteDirs(key, recursive);
        } else {
            store.delete(key);
        }
        if (!store.isHierarchy()) {
            createParent(f);
        }
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        checkOpen();
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        if (key.length() == 0) { // root always exists
            return newDirectory(null, absolutePath);
        }

        try {
            FileMetadata meta = store.retrieveMetadata(key);
            if (meta != null) {
                if (meta.isFolder()) {
                    return meta.getLastModified() == 0 ?
                            newDirectory(null, absolutePath) : newDirectory(meta, absolutePath);
                } else {
                    return newFile(meta, absolutePath);
                }
            }
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException(absolutePath + ": No such file or directory.");
        } catch (IOException e) {
            LOG.error("bos-fs getFileStatus error: ", e);
            throw e;
        }
        throw new FileNotFoundException(absolutePath + ": No such file or directory.");
    }

    @Override
    public URI getUri() {
        return uri;
    }

    /**
     * <p>
     * If <code>f</code> is a file, this method will make a single call to S3.
     * If <code>f</code> is a directory, this method will make a maximum of
     * (<i>n</i> / 1000) + 2 calls to S3, where <i>n</i> is the total number of
     * files and directories contained directly in <code>f</code>.
     * </p>
     */
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        checkOpen();
        boolean needListObject = getConf().getBoolean(BaiduBosConfigKeys.NEED_DIRECT_LIST, false);
        String delimiter = !needListObject ? PATH_DELIMITER : null;
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        URI pathUri = absolutePath.toUri();
        Set<FileStatus> status = new TreeSet<FileStatus>();
        String priorLastKey = null;
        // check weather dir first
        do {
            PartialListing listing = store.list(store.prefixToDir(key), BaiduBosConfigKeys.BOS_MAX_LISTING_LENGTH, priorLastKey,
                    delimiter);
            if (listing != null) {
                for (FileMetadata fileMetadata : listing.getFiles()) {
                    if (fileMetadata.getKey() != null
                            && fileMetadata.getKey().length() > 0
                            && !fileMetadata.getKey().endsWith(FOLDER_SUFFIX)) {
                        Path subpath = keyToPath(fileMetadata.getKey());
                        String relativePath = pathUri.relativize(subpath.toUri()).getPath();
                        status.add(newFile(fileMetadata, new Path(absolutePath, relativePath)));
                    }
                }
                String[] commonPrefixs = listing.getCommonPrefixes();
                if (commonPrefixs != null && commonPrefixs.length > 0) {
                    for (String commonPrefix : commonPrefixs) {
                        if (commonPrefix != null && commonPrefix.length() > 0) {
                            Path subpath = keyToPath(commonPrefix);
                            String relativePath = pathUri.relativize(subpath.toUri()).getPath();
                            Path dirPath = new Path(absolutePath, relativePath);
                            status.add(getBosDirFileStatus(commonPrefix, dirPath));
                        }
                    }
                }
                priorLastKey = listing.getPriorLastKey();
            }
        } while (priorLastKey != null && priorLastKey.length() > 0);
        // if not dir check weather object
        if (status.size() == 0) {
            try {
                FileStatus stat = getFileStatus(f);
                if (stat.isFile()) {
                    status.add(stat);
                }
            } catch (FileNotFoundException e) {
                LOG.debug(" no this object");
            }
        }
        return status.toArray(new FileStatus[0]);
    }

    private FileStatus getBosDirFileStatus(String commonPrefix, Path path) {
        boolean needShowDirTime = getConf().getBoolean(BaiduBosConfigKeys.NEED_SHOW_DIR_TIME, false);
        try {
            if (needShowDirTime) {
                FileMetadata dirMeta = store.retrieveMetadata(commonPrefix);
                if (dirMeta != null) {
                    return newDirectory(dirMeta, path);
                }
            }
        } catch (IOException e) {
            // throw nothing
        }
        return newDirectory(null, path);
    }

    /**
     * non-recursive getContentSummary. just list objects with no "/"
     *
     * @param f path to use
     * @return the {@link ContentSummary} of a given {@link Path}.
     * @throws IOException FileNotFoundException if the path does not resolve
     */
    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
        checkOpen();
        boolean useOrigin = getConf().getBoolean(BaiduBosConfigKeys.NEED_RECURSIVE_SUMMARY, false);
        if (useOrigin) {
            return super.getContentSummary(f);
        }
        // non-recursive list
        FileStatus status = this.getFileStatus(f);
        // f is file
        if (status.isFile()) {
            return new ContentSummary(status.getLen(), 1L, 0L);
        }
        // f is a dir
        long[] summary = new long[]{0L, 0L, 1L};
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        String priorLastKey = null;
        do {
            PartialListing listing = store.list(store.prefixToDir(key), BaiduBosConfigKeys.BOS_MAX_LISTING_LENGTH, priorLastKey, null);
            if (listing != null) {
                for (FileMetadata fileMetadata : listing.getFiles()) {
                    if (fileMetadata.getKey() != null && fileMetadata.getKey().length() > 0) {
                        summary[0] += fileMetadata.getLength();
                        summary[1] += 1;
                    }
                }
                priorLastKey = listing.getPriorLastKey();
            }
        } while (priorLastKey != null && priorLastKey.length() > 0);
        return new ContentSummary(summary[0], summary[1], summary[2]);
    }

    private String getRelativePath(String path) {
        return path.substring(path.indexOf(PATH_DELIMITER) + 1);
    }

    private FileStatus newFile(FileMetadata meta, Path path) {
        return new FileStatus(meta.getLength(), false, 1, getConf().getLong(BaiduBosConfigKeys.BOS_BLOCK_SIZE, BaiduBosConfigKeys.DEFAULT_BOS_BLOCK_SIZE),
                meta.getLastModified(), path.makeQualified(this));
    }

    private FileStatus newDirectory(FileMetadata meta, Path path) {
        return new FileStatus(0, true, 1, getConf().getLong(BaiduBosConfigKeys.BOS_BLOCK_SIZE, BaiduBosConfigKeys.DEFAULT_BOS_BLOCK_SIZE),
                (meta == null ? 0 : meta.getLastModified()), path.makeQualified(this));
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        checkOpen();
        Path absolutePath = makeAbsolute(f);
        List<Path> paths = new ArrayList<Path>();
        do {
            paths.add(0, absolutePath);
            absolutePath = absolutePath.getParent();
        } while (pathToKey(absolutePath).length() != 0);

        boolean result = true;
        for (Path path : paths) {
            result &= mkdir(path);
        }
        return result;
    }

    private boolean mkdir(Path f) throws IOException {
        try {
            FileStatus fileStatus = getFileStatus(f);
            if (!fileStatus.isDir()) {
                throw new IOException(String.format(
                        "Can't make directory for path %s since it is a file", f));

            }
        } catch (FileNotFoundException e) {
            String key = pathToKey(f);
            if (!key.endsWith(FOLDER_SUFFIX)) {
                key = pathToKey(f) + FOLDER_SUFFIX;
            }
            store.storeEmptyFile(key);
        }
        return true;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        checkOpen();

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        FileMetadata fileMetaData = null;
        try {
            fileMetaData = store.retrieveMetadata(key);
        } catch (FileNotFoundException ignore) {
            throw new FileNotFoundException(f.toString());
        }
        if (fileMetaData.isFolder()) {
            throw new FileSystemException("Can not open a folder");
        }
        NativeBOSFsInputStream bosFsInputStream = new NativeBOSFsInputStream(key, fileMetaData);
        bosFsInputStream.setReadahead(this.readAheadSize);
        bosFsInputStream.setTailCacheEnable(this.tailCacheEnable);
        bosFsInputStream.setTailCacheTriggerAndCacheSize(this.tailCacheTriggerThreshold, this.tailCacheSize);
        return new FSDataInputStream(new BufferedFSInputStream(bosFsInputStream, this.readBufferSize));
    }

    // rename() and delete() use this method to ensure that the parent directory
    // of the source does not vanish.
    private void createParent(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            String key = pathToKey(makeAbsolute(parent));
            if (key.length() > 0) {
                try {
                    store.retrieveMetadata(key + FOLDER_SUFFIX);
                } catch (Exception e) {
                    LOG.warn("create parent when rename or delete, common sense the code won't reach here");
                    store.storeEmptyFile(key + FOLDER_SUFFIX);
                }
            }
        }
    }

    private boolean existsAndIsFile(Path f) throws IOException {
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        if (key.length() == 0) {
            return false;
        }

        FileStatus stat = getFileStatus(absolutePath);

        return stat != null && stat.isFile();
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        checkOpen();
        String srcKey = pathToKey(makeAbsolute(src));

        if (srcKey.length() == 0) {
            // Cannot rename root of file system
            return false;
        }
        
        // Figure out the final destination
        String dstKey = pathToKey(makeAbsolute(dst));
        try {
            boolean srcIsFile = store.isHierarchy() || existsAndIsFile(src);
            // in some scenario, multi-rename try to create the same parent simultaneously
            // in bos, this will cause accidental exception.
            //            createParent(src);
            if (srcIsFile) {
                rename(srcKey, dstKey, true);
            } else {
                rename(srcKey, dstKey, false);
            }
            return true;
        } catch (FileNotFoundException e) {
            // Source file does not exist;
            return false;
        }
    }

    private void rename(String srcKey, String dstKey, boolean isFile) throws IOException {
        try {
            store.rename(srcKey, dstKey, isFile);
        } catch (FileNotFoundException notFoundException) {
            throw new IOException(notFoundException);
        }
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    public boolean needDirectlyWrite() {
        return false;
    }

    /**
     * Set the working directory to the given directory.
     */
    @Override
    public void setWorkingDirectory(Path newDir) {
        workingDir = newDir;
    }

    private class ReaderBuffer {
        private static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
        byte[] buffer;
        // position of object stream
        long bytesStartPos = -1;
        long bytesEndPos = -1;
        String key;

        public ReaderBuffer(String key, int bufferSize) {
            this.key = key;
            if (bufferSize <= 0) {
                bufferSize = DEFAULT_BUFFER_SIZE;
            }
            buffer = new byte[Math.min(DEFAULT_BUFFER_SIZE, bufferSize)];
        }

        // pos: position of object stream
        public int writeToBuffer(InputStream in, long pos, int length) {
            int readed = 0;
            if (length > DEFAULT_BUFFER_SIZE) {
                LOG.debug("oversize for " + key);
                return 0;
            }

            if (bytesEndPos != -1) {
                this.resetBuffer();
                LOG.debug("reset old buffer [" + bytesStartPos + " , " + bytesEndPos + "] for " + key);
            }
            bytesStartPos = pos;
            try {
                int writePos = 0;
                int curReaded = 0;
                while (readed < length) {
                    curReaded = in.read(buffer, writePos, length - writePos);
                    if (curReaded < 0) {
                        break;
                    }
                    writePos += curReaded;
                    readed += curReaded;
                }
            } catch (EOFException e) {
                LOG.debug("EOF readed: " + readed);
            } catch (IOException e) {
                LOG.debug("IOException");
                return 0;
            }
            bytesEndPos = bytesStartPos + readed;
            return readed;
        }

        // read one byte
        public int read(long pos) throws IOException {
            if (bytesEndPos == -1) {
                throw new IOException("no buffer or miss cache for " + key);
            }
            if (!isHit(pos)) {
                return -1;
            }

            return buffer[(int) (pos - bytesStartPos)];
        }

        public int read(long pos, byte[] buf, int off, int len) throws IOException {
            if (bytesEndPos == -1) {
                throw new IOException("no buffer or miss cache for " + key);
            }
            if (!isHit(pos)) {
                return -1;
            }

            int readed = 0;
            for (int i = (int) (pos - bytesStartPos); i < buffer.length && readed < len; i++) {
                buf[off + readed] = buffer[i];
                readed++;
            }
            return readed;
        }

        // hdfs may read over content length, don't care read len
        public boolean isHit(long pos) {
            return pos >= bytesStartPos && pos < bytesEndPos;
        }

        private void resetBuffer() {
            bytesEndPos = -1;
        }
    }

    private class NativeBOSFsInputStream extends FSInputStream implements CanSetReadahead {
        private static final String NEW_OFFSET = "read from new offset";
        private static final String FAILURE_RECOVER = "failure recovery";
        private volatile boolean closed;
        private final String key;
        private final long contentLength;
        private InputStream in = null;
        private long pos = 0;
        private FileMetadata fileMetaData;
        private long readahead = BaiduBosConfigKeys.DEFAULT_BOS_READ_AHEAD_SIZE;
        private long readaheadRaw = BaiduBosConfigKeys.DEFAULT_BOS_READ_AHEAD_SIZE;
        private long expectNextReadPos = 0;
        private boolean isRandomIO = true;
        private int tailCacheTriggerThreshold = BaiduBosConfigKeys.DEFAULT_TAIL_CACHE_TRIGGER_THRESHOLD;
        private int tailCacheSize = BaiduBosConfigKeys.DEFAULT_TAIL_CACHE_SIZE;

        /**
         * Read some data to buffer when content_length - targetPos < tailCacheTriggerThreshold && tailCacheEnable.
         *
         */
        private ReaderBuffer readerBuffer = null;

        /**
         * The switch of cache tail data.
         */
        private boolean tailCacheEnable = false;
        /**
         * This is the actual position within the object, used by
         * lazy seek to decide whether to seek on the next read or not.
         */
        private long nextReadPos = 0;

        /**
         * The start of the content range of the last request.
         */
        private long contentRangeStart = 0;

        /**
         * The end of the content range of the last request.
         * This is an absolute value of the range, not a length field.
         */
        private long contentRangeFinish = 0;

        public NativeBOSFsInputStream(String key, FileMetadata fileMetaData) {
            this.key = key;
            this.fileMetaData = fileMetaData;
            this.contentLength = fileMetaData.getLength();
        }

        public synchronized int read() throws IOException {
            checkNotClosed();

            if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
                return -1;
            }

            int byteRead;

            try {
                if (readerBuffer != null && readerBuffer.isHit(nextReadPos)) {
                    byteRead = readerBuffer.read(nextReadPos);
                    if (byteRead >= 0) {
                        nextReadPos++;
                    }
                    LOG.debug("hit cache, read 1 byte from buffer, pos: " + nextReadPos + ", key:" + key);
                } else {
                    lazySeek(nextReadPos, 1);
                    byteRead = in.read();
                    if (byteRead >= 0) {
                        pos++;
                        nextReadPos++;
                    }
                }
            } catch (EOFException e) {
                return -1;
            } catch (IOException e) {
                onReadFailure(e, 1);
                byteRead = in.read();
                if (byteRead >= 0) {
                    pos++;
                    nextReadPos++;
                }
            } finally {
                if (remainingInCurrentRequest() <= 0) {
                    closeStream("for connection leakaging");
                }
            }
                
            
            if (statistics != null && byteRead >= 0) {
                statistics.incrementBytesRead(1);
            }
            expectNextReadPos = nextReadPos;
            return byteRead;
        }

        public synchronized int read(byte[] buf, int off, int len) throws IOException {
            checkNotClosed();

            if (len == 0) {
                return 0;
                
            }

            if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
                return -1;
            }
            
            try {
                if (readerBuffer == null || !readerBuffer.isHit(nextReadPos)) {
                    lazySeek(nextReadPos, len);
                }
            } catch (EOFException e) {
                // the end of the file has moved
                return -1;
            }

            int bytesRead;
            try {
                if (readerBuffer != null && readerBuffer.isHit(nextReadPos)) {
                    bytesRead = readerBuffer.read(nextReadPos, buf, off, len);
                    if (bytesRead > 0) {
                        nextReadPos += bytesRead;
                    }
                    LOG.debug("hit cache, read " + bytesRead + " byte from cache, pos:" + nextReadPos + ", key:" + key);
                } else {
                    bytesRead = in.read(buf, off, len);
                    if (bytesRead > 0) {
                        pos += bytesRead;
                        nextReadPos += bytesRead;
                    }
                }
            } catch (EOFException e) {
                onReadFailure(e, len);
                // the base implementation swallows EOFs.
                return -1;
            } catch (IOException e) {
                onReadFailure(e, len);
                bytesRead = in.read(buf, off, len);
                if (bytesRead > 0) {
                    pos += bytesRead;
                    nextReadPos += bytesRead;
                }
            } catch (Exception e) {
                int intervalSeconds = 3;
                int retry = Math.max(getConf().getInt("fs.bos.maxRetries", 4), 0) + 1;
                while (true) {
                    try {
                        LOG.info("Read exception occur: retry at most " + retry + " times.");
                        onReadFailure(e, len);
                        bytesRead = in.read(buf, off, len);
                        if (bytesRead > 0) {
                            pos += bytesRead;
                            nextReadPos += bytesRead;
                        }
                        break;
                    } catch (EOFException eof) {
                        onReadFailure(eof, len);
                        return -1;
                    } catch (IOException ioe) {
                        onReadFailure(ioe, len);
                        bytesRead = in.read(buf, off, len);
                        if (bytesRead > 0) {
                            pos += bytesRead;
                            nextReadPos += bytesRead;
                        }
                        break;
                    } catch (Exception ex) {
                        if (retry <= 0) {
                            throw new IOException("Retry " + retry +
                                    " times to read still exception: " + ex.getMessage());
                        }

                        try {
                            TimeUnit.SECONDS.sleep(intervalSeconds);
                        } catch (InterruptedException ite) {
                            // Just retry, so catch the exceptions thrown by sleep
                        }
                        intervalSeconds *= 2;
                        retry--;
                    }
                }
            } finally {
                if (remainingInCurrentRequest() <= 0) {
                    closeStream("for connection leakaging");
                }
            }

            if (statistics != null && bytesRead >= 0) {
                statistics.incrementBytesRead(bytesRead);
            }

            expectNextReadPos = nextReadPos;

            return bytesRead;
        }

        public void close() throws IOException {
            if (!closed) {
                closed = true;
                
                // close or abort the stream
                closeStream("close() operation");
                // this is actually a no-op
                super.close();
            }
        }

        public synchronized void seek(long targetPos) throws IOException {
            checkNotClosed();
            
            // Do not allow negative seek
            if (targetPos < 0 || targetPos > contentLength) {
                throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + targetPos);
            }
            
            if (this.contentLength <= 0) {
                return;
            }

            // Lazy seek
            nextReadPos = targetPos;
        }

        public synchronized long getPos() throws IOException {
            return nextReadPos;
        }

        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        // readahead : conf("fs.bos.readahead.size") or content-length(default)
        public synchronized void setReadahead(Long readahead) {
            this.readahead = readahead;
            this.readaheadRaw = this.readahead;
        }

        public synchronized void setTailCacheEnable(boolean enable) {
            this.tailCacheEnable = enable;
        }

        public synchronized void setTailCacheTriggerAndCacheSize(int triggerThreshold, int cacheSize) {
            if (triggerThreshold > cacheSize) {
                LOG.fatal("triggerThreshold should not be more than cacheSize");
                triggerThreshold = BaiduBosConfigKeys.DEFAULT_TAIL_CACHE_TRIGGER_THRESHOLD;
                cacheSize = BaiduBosConfigKeys.DEFAULT_TAIL_CACHE_SIZE;
            }
            this.tailCacheTriggerThreshold = triggerThreshold;
            this.tailCacheSize = cacheSize;
        }
        
        /**
         * Verify that the input stream is open. Non blocking; this gives
         * the last state of the volatile {@link #closed} field.
         * @throws IOException if the connection is closed.
         */
        private void checkNotClosed() throws IOException {
            if (closed) {
                throw new IOException(key + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
            }
        }
        
        /**
         * Perform lazy seek and adjust stream to correct position for reading.
         *
         * @param targetPos position from where data should be read
         * @param len length of the content that needs to be read
         */
        private void lazySeek(long targetPos, long len) throws IOException {
            // For lazy seek
            try {
                seekInStream(targetPos);
            } catch (Exception e) {
                closeStream("seekInStream failed");
            }
            // re-open at specific location if needed
            if (in == null) {
                LOG.debug("re-open at specific locaition: " + targetPos + ", key:" + key);
                reopen(NEW_OFFSET, targetPos, len);
            }
        }
        
        /**
         * Adjust the stream to a specific position.
         *
         * @param targetPos target seek position
         * @throws IOException
         */
        private void seekInStream(long targetPos) throws IOException {
            long diff = targetPos - pos;
            checkNotClosed();
            
            if (in == null) {
                return;
            }
            // compute how much more to skip
            // long diff = targetPos - pos;
            if (diff > 0) {
                // if seek then reopen the stream

                // forward seek -this is where data can be skipped
                int available = in.available();
                // always seek at least as far as what is available
                long forwardSeekRange = Math.max(readahead, available);
                // work out how much is actually left in the stream
                // then choose whichever comes first: the range or the EOF
                long remainingInCurrentRequest = remainingInCurrentRequest();

                long forwardSeekLimit = Math.min(remainingInCurrentRequest, forwardSeekRange);

                boolean skipForward = remainingInCurrentRequest > 0 && diff <= forwardSeekLimit;
               
                if (skipForward) {
                    // the forward seek range is within the limits
                    LOG.debug("Forward seek on " 
                              + key + " of " + diff + "bytes");
                    
                    long skipped = in.skip(diff);
                    if (skipped > 0) {
                        pos += skipped;
                        // as these bytes have been read, they are included in the counter
                        statistics.incrementBytesRead(diff);
                    }

                    if (pos == targetPos) {
                        // all is well
                        return;
                    } else {
                        // log a warning; continue to attempt to re-open
                        LOG.warn("Failed to seek on " 
                                 + key + " to " + targetPos 
                                 + ". Current position " + pos);
                    }
                }
            } else if (diff < 0) {
                // backwards seek
                LOG.warn("seek on " 
                         + key + " backwards " + diff 
                         + ". Current position " + pos);
            } else {
                // targetPos == pos
                if (remainingInCurrentRequest() > 0) {
                    // if there is data left in the stream, keep going
                    return;
                }
            }
            // if the code reaches here, the stream needs to be reopened.
            // close the stream; if read the object will be opened at the new pos
            closeStream("seekInStream()");
            pos = targetPos;
        }
        
        /**
         * Bytes left in the current request.
         * Only valid if there is an active request.
         * @return how many bytes are left to read in the current GET.
         */
        public synchronized long remainingInCurrentRequest() {
            return this.contentRangeFinish - this.pos;
        }
        
        /**
         * Close a stream
         *
         * This does not set the {@link #closed} flag.
         * @param reason reason for stream being closed; used in messages
         */
        private void closeStream(String reason) {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    LOG.debug("When closing " 
                              + key + " stream for " + reason + e);
                }
            }
            long remaining = remainingInCurrentRequest();
            LOG.debug("Stream " + key 
                      + " : " + reason 
                      + "; remaining=" + remaining
                      + " streamPos=" + pos 
                      + "," 
                      + " nextReadPos=" + nextReadPos
                      + "," 
                      + " request range " + contentRangeStart
                      + "-" + contentRangeFinish);
            
            in = null;
        }
        
        /**
         * Handle an IOE on a read by attempting to re-open the stream.
         * The filesystem's readException count will be incremented.
         * @param ioe exception caught.
         * @param length length of data being attempted to read
         * @throws IOException any exception thrown on the re-open attempt.
         */
        private void onReadFailure(Exception ioe, int length) throws IOException {
            LOG.info("Got exception while trying to read from stream "
                    + key + " trying to recover from position: " + pos);
            LOG.info("While trying to read from stream " 
                      + key 
                      + " exception:" + ioe);
            if (pos >= contentLength) {
                pos = nextReadPos;
            }
            reopen(FAILURE_RECOVER, pos, length);
        }
        
        /**
         * Opens up the stream at specified target position and for given length.
         *
         * @param reason reason for reopen
         * @param targetPos target position
         * @param length length requested
         * @throws IOException on any failure to open the object
         */
        private synchronized void reopen(String reason, long targetPos, long length) throws IOException {
            if (in != null) {
                closeStream("reopen(" + reason + ")");
            }

            isRandomIO = targetPos != expectNextReadPos;
            if (!this.isRandomIO) {
                readahead = targetPos == 0 ? readahead : readahead * 4;
                LOG.debug("is seq IO");
            } else {
                readahead = readaheadRaw;
            }

            contentRangeFinish = calculateRequestLimit(targetPos, length, contentLength, readahead);

            LOG.debug("reopen(" + key
                    + ") for " + reason
                    + " range[" + targetPos
                    + "-" + contentRangeFinish
                    + "], length=" + length
                    + ","
                    + " streamPosition=" + pos
                    + ", nextReadPosition=" + nextReadPos
                    + ", readahead=" + readahead);
            try {
                if (tailCacheEnable &&
                        contentRangeFinish - targetPos < this.tailCacheTriggerThreshold &&
                        contentRangeFinish - targetPos < tailCacheSize &&
                        contentLength >  tailCacheSize &&
                        reason.equals(NEW_OFFSET)) {
                    if (readerBuffer != null && readerBuffer.isHit(targetPos)) {
                        return;
                    } else if (readerBuffer == null) {
                        readerBuffer = new ReaderBuffer(this.key, tailCacheSize);
                    }
                    long startPos = contentRangeFinish - tailCacheSize;
                    in = store.retrieve(key, startPos, contentRangeFinish, fileMetaData);
                    int readed = readerBuffer.writeToBuffer(in, startPos, tailCacheSize);
                    LOG.debug("write buffer, [" + startPos + " , " + (startPos + readed) + "], key:" + key);
                    // in = null;
                    contentRangeStart = startPos;
                    this.pos = startPos + readed;
                } else {
                    in = store.retrieve(key, targetPos, contentRangeFinish, fileMetaData);
                    contentRangeStart = targetPos;
                    this.pos = targetPos;
                }
            } catch (Exception e) {
                LOG.info("native store retrieve failed reason : " + e.getMessage());
                closeStream("native store retrieve failed");
            }
            if (in == null) {
                throw new IOException("Null IO stream from reopen of (" + reason +  ") " + key);
            }
        }

        /**
         * Calculate the limit for a get request,
         *
         * @param targetPos position of the read
         * @param length length of bytes requested; if less than zero "unknown"
         * @param contentLength total length of file
         * @param readahead current readahead value
         * @return the absolute value of the limit of the request.
         */
        private long calculateRequestLimit(long targetPos,
                long length,
                long contentLength,
                long readahead) {
            /*long rangeLimit;

              rangeLimit = (length < 0) ? contentLength : targetPos + Math.max(readahead, length);

            // cannot read past the end of the object
            rangeLimit = Math.min(contentLength, rangeLimit);
            return rangeLimit;*/


            if (length < 0) {
                return contentLength;
            }

            return Math.min(contentLength, targetPos + Math.max(readahead, length));
        }
    }

    @Override
    public String getCanonicalServiceName() {
        // Does not support Token
        return null;
    }
}

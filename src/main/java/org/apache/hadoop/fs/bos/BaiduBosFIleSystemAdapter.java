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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BaiduBosFIleSystemAdapter extends DelegateToFileSystem {

    public BaiduBosFIleSystemAdapter(URI theUri, Configuration conf) throws IOException, URISyntaxException {
        super(theUri, new BaiduBosFileSystem(), conf, "bos", false);
    }

    protected BaiduBosFIleSystemAdapter(URI theUri, FileSystem theFsImpl,
                                        Configuration conf, String supportedScheme,
                                        boolean authorityRequired) throws IOException, URISyntaxException {
        super(theUri, new BaiduBosFileSystem(), conf, "bos", false);
    }

    @Override
    public int getUriDefaultPort() {
        // return Constants.S3A_DEFAULT_PORT;
        return super.getUriDefaultPort();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("bos:{");
        sb.append("URI =").append(fsImpl.getUri());
        sb.append("; fsImpl=").append(fsImpl);
        sb.append('}');
        return sb.toString();
    }

}

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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * BOS implementation of AbstractFileSystem.
 * This impl delegates to the BosFileSystem
 */
public class BOS extends DelegateToFileSystem {

    public BOS(URI uri, Configuration conf) throws IOException, URISyntaxException {
        super(uri, new BaiduBosFileSystem(), conf, "bos", false);
    }

    @Override
    public int getUriDefaultPort() {
        // return -1 can pass AbstractFileSystem checkPath
        return -1;
    }
}

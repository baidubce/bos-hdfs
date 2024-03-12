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

import org.apache.hadoop.fs.FileChecksum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

public class BOSCRC32CCheckSum extends FileChecksum {
    private static final String ALGORITHM_NAME = "COMPOSITE-CRC";

    private int crc32c = 0;

    public BOSCRC32CCheckSum() {}

    public BOSCRC32CCheckSum(String crc32cUInt32) {
        try {
            BigInteger bigInteger = new BigInteger(crc32cUInt32);
            this.crc32c = bigInteger.intValue();
        } catch (NumberFormatException e) {
            this.crc32c = 0;
        }
    }

    /**
     * Writes big-endian representation of {@code value} into {@code buf}
     * starting at {@code offset}. buf.length must be greater than or
     * equal to offset + 4.
     */
    public static void writeInt(byte[] buf, int offset, int value)
            throws IOException {
        if (offset + 4 > buf.length) {
            throw new IOException(String.format(
                    "writeInt out of bounds: buf.length=%d, offset=%d",
                    buf.length, offset));
        }
        buf[offset + 0] = (byte) ((value >>> 24) & 0xff);
        buf[offset + 1] = (byte) ((value >>> 16) & 0xff);
        buf[offset + 2] = (byte) ((value >>> 8) & 0xff);
        buf[offset + 3] = (byte) (value & 0xff);
    }

    /**
     * int turn to bytes
     * @return 4-byte array holding the big-endian representation of
     * {@code value}.
     */
    public static byte[] intToBytes(int value) {
        byte[] buf = new byte[4];
        try {
            writeInt(buf, 0, value);
        } catch (IOException ioe) {
            // Since this should only be able to occur from code bugs within this
            // class rather than user input, we throw as a RuntimeException
            // rather than requiring this method to declare throwing IOException
            // for something the caller can't control.
            throw new RuntimeException(ioe);
        }
        return buf;
    }

    @Override
    public String getAlgorithmName() {
        return BOSCRC32CCheckSum.ALGORITHM_NAME;
    }

    @Override
    public int getLength() {
        return Integer.SIZE / Byte.SIZE;
    }

    @Override
    public byte[] getBytes() {
        return intToBytes(crc32c);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.crc32c);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.crc32c = dataInput.readInt();
    }

    @Override
    public String toString() {
        return getAlgorithmName() + ":" + String.format("0x%08x", crc32c);
    }
}

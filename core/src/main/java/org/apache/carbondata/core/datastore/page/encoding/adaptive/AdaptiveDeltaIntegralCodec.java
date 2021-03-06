/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.page.encoding.adaptive;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageValueConverter;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.format.Encoding;

/**
 * Codec for integer (byte, short, int, long) data type and floating data type (in case of
 * scale is 0).
 * This codec will calculate delta of page max value and page value,
 * and do type casting of the diff to make storage minimum.
 */
public class AdaptiveDeltaIntegralCodec extends AdaptiveCodec {

  private ColumnPage encodedPage;
  private long max;

  public AdaptiveDeltaIntegralCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats) {
    super(srcDataType, targetDataType, stats);
    if (srcDataType == DataTypes.BYTE) {
      this.max = (byte) stats.getMax();
    } else if (srcDataType == DataTypes.SHORT) {
      this.max = (short) stats.getMax();
    } else if (srcDataType == DataTypes.INT) {
      this.max = (int) stats.getMax();
    } else if (srcDataType == DataTypes.LONG || srcDataType == DataTypes.TIMESTAMP) {
      this.max = (long) stats.getMax();
    } else if (srcDataType == DataTypes.DOUBLE) {
      this.max = (long) (double) stats.getMax();
    } else if (DataTypes.isDecimal(srcDataType)) {
      this.max = ((BigDecimal) stats.getMax()).unscaledValue().longValue();
    } else {
      // this codec is for integer type only
      throw new UnsupportedOperationException(
          "unsupported data type for Delta compress: " + srcDataType);
    }
  }

  @Override
  public String getName() {
    return "DeltaIntegralCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new ColumnPageEncoder() {
      final Compressor compressor = CompressorFactory.getInstance().getCompressor();

      @Override
      protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
        if (encodedPage != null) {
          throw new IllegalStateException("already encoded");
        }
        encodedPage = ColumnPage.newPage(input.getColumnSpec(), targetDataType,
            input.getPageSize());
        input.convertValue(converter);
        byte[] result = encodedPage.compress(compressor);
        encodedPage.freeMemory();
        return result;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new ColumnPageEncoderMeta(inputPage.getColumnSpec(), targetDataType,
            inputPage.getStatistics(), compressor.getName());
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        encodings.add(Encoding.ADAPTIVE_DELTA_INTEGRAL);
        return encodings;
      }

    };
  }

  @Override public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    return new ColumnPageDecoder() {
      @Override public ColumnPage decode(byte[] input, int offset, int length)
          throws MemoryException, IOException {
        ColumnPage page = null;
        if (DataTypes.isDecimal(meta.getSchemaDataType())) {
          page = ColumnPage.decompressDecimalPage(meta, input, offset, length);
        } else {
          page = ColumnPage.decompress(meta, input, offset, length);
        }
        return LazyColumnPage.newPage(page, converter);
      }
    };
  }

  private ColumnPageValueConverter converter = new ColumnPageValueConverter() {
    @Override
    public void encode(int rowId, byte value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, short value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, int value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, long value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, max - value);
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, float value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, (long) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, double value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, (long) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public long decodeLong(byte value) {
      return max - value;
    }

    @Override
    public long decodeLong(short value) {
      return max - value;
    }

    @Override
    public long decodeLong(int value) {
      return max - value;
    }

    @Override
    public double decodeDouble(byte value) {
      return max - value;
    }

    @Override
    public double decodeDouble(short value) {
      return max - value;
    }

    @Override
    public double decodeDouble(int value) {
      return max - value;
    }

    @Override
    public double decodeDouble(long value) {
      return max - value;
    }

    @Override
    public double decodeDouble(float value) {
      // this codec is for integer type only
      throw new RuntimeException("internal error");
    }

    @Override
    public double decodeDouble(double value) {
      // this codec is for integer type only
      throw new RuntimeException("internal error");
    }
  };
}

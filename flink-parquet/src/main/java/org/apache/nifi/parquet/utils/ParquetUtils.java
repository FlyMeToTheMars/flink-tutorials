package org.apache.nifi.parquet.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetUtils  implements Serializable {

  public static final String ROW_GROUP_SIZE = "row-group-size";
//            .displayName("Row Group Size")

  public static final String PAGE_SIZE = "page-size";
//            .displayName("Page Size";

  public static final String DICTIONARY_PAGE_SIZE = "dictionary-page-size";
//            .displayName("Dictionary Page Size";

  public static final String MAX_PADDING_SIZE = "max-padding-size";
//            .displayName("Max Padding Size";

  public static final String ENABLE_DICTIONARY_ENCODING = "enable-dictionary-encoding";
//            .displayName("Enable Dictionary Encoding";

  public static final String ENABLE_VALIDATION = "enable-validation";
//            .displayName("Enable Validation";

  public static final String WRITER_VERSION = "writer-version";
//            .displayName("Writer Version";

  public static final String AVRO_READ_COMPATIBILITY = "avro-read-compatibility";
//            .displayName("Avro Read Compatibility";

  public static final String AVRO_ADD_LIST_ELEMENT_RECORDS = "avro-add-list-element-records";
//            .displayName("Avro Add List Element Records";

  public static final String AVRO_WRITE_OLD_LIST_STRUCTURE = "avro-write-old-list-structure";
//            .displayName("Avro Write Old List Structure";


  public static final List<String> COMPRESSION_TYPES = getCompressionTypes();

  private static List<String> getCompressionTypes() {
    final List<String> compressionTypes = new ArrayList<>();
    for (CompressionCodecName compressionCodecName : CompressionCodecName.values()) {
      final String name = compressionCodecName.name();
      compressionTypes.add(name);
    }
    return Collections.unmodifiableList(compressionTypes);
  }

  // NOTE: This needs to be named the same as the compression property in AbstractPutHDFSRecord
  public static final String COMPRESSION_TYPE_PROP_NAME = "compression-type";
  public static final String OVERWRITE = "overwrite";

  /**
   * Creates a ParquetConfig instance from the given PropertyContext.
   *
   * @param context the PropertyContext from a component
   * @return the ParquetConfig
   */
  public static ParquetConfig createParquetConfig(final Properties context) {
    final ParquetConfig parquetConfig = new ParquetConfig();

    // Required properties
    boolean overwrite = true;
    if (context.containsKey(OVERWRITE)) {
      overwrite = BooleanUtils.toBoolean(context.getProperty(OVERWRITE));
    }

    final ParquetFileWriter.Mode mode =
        overwrite ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE;
    parquetConfig.setWriterMode(mode);

    final String compressionTypeValue = context
        .getProperty(ParquetUtils.COMPRESSION_TYPE_PROP_NAME,
            CompressionCodecName.UNCOMPRESSED.name());
    final CompressionCodecName codecName = CompressionCodecName.valueOf(compressionTypeValue);
    parquetConfig.setCompressionCodec(codecName);

    // Optional properties

    if (context.containsKey(ROW_GROUP_SIZE)) {
      try {
        final int rowGroupSize = NumberUtils.toInt(context.getProperty(ROW_GROUP_SIZE));
        if (rowGroupSize > 0) {
          parquetConfig.setRowGroupSize(rowGroupSize);
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Invalid data size for " + ROW_GROUP_SIZE, e);
      }
    }

    if (context.containsKey(PAGE_SIZE)) {
      try {
        final int pageSize = NumberUtils.toInt(context.getProperty(PAGE_SIZE));
        if (pageSize > 0) {
          parquetConfig.setPageSize(pageSize);
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid data size for " + PAGE_SIZE, e);
      }
    }

    if (context.containsKey(DICTIONARY_PAGE_SIZE)) {
      try {
        final Integer dictionaryPageSize = NumberUtils
            .toInt(context.getProperty(DICTIONARY_PAGE_SIZE));
        if (dictionaryPageSize != null) {
          parquetConfig.setDictionaryPageSize(dictionaryPageSize);
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Invalid data size for " + DICTIONARY_PAGE_SIZE, e);
      }
    }

    if (context.containsKey(MAX_PADDING_SIZE)) {
      try {
        final int maxPaddingSize = NumberUtils.toInt(context.getProperty(MAX_PADDING_SIZE));
        if (maxPaddingSize > 0) {
          parquetConfig.setMaxPaddingSize(maxPaddingSize);
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Invalid data size for " + MAX_PADDING_SIZE, e);
      }
    }

    if (context.containsKey(ENABLE_DICTIONARY_ENCODING)) {
      final boolean enableDictionaryEncoding = BooleanUtils
          .toBoolean(context.getProperty(ENABLE_DICTIONARY_ENCODING));
      parquetConfig.setEnableDictionaryEncoding(enableDictionaryEncoding);
    }

    if (context.containsKey(ENABLE_VALIDATION)) {
      final boolean enableValidation = BooleanUtils
          .toBoolean(context.getProperty(ENABLE_VALIDATION));
      parquetConfig.setEnableValidation(enableValidation);
    }

    if (context.containsKey(WRITER_VERSION)) {
      final String writerVersionValue = context.getProperty(WRITER_VERSION);
      parquetConfig.setWriterVersion(ParquetProperties.WriterVersion.valueOf(writerVersionValue));
    }

    if (context.containsKey(AVRO_READ_COMPATIBILITY)) {
      final boolean avroReadCompatibility = BooleanUtils
          .toBoolean(context.getProperty(AVRO_READ_COMPATIBILITY));
      parquetConfig.setAvroReadCompatibility(avroReadCompatibility);
    }

    if (context.containsKey(AVRO_ADD_LIST_ELEMENT_RECORDS)) {
      final boolean avroAddListElementRecords = BooleanUtils
          .toBoolean(context.getProperty(AVRO_ADD_LIST_ELEMENT_RECORDS));
      parquetConfig.setAvroAddListElementRecords(avroAddListElementRecords);
    }

    if (context.containsKey(AVRO_WRITE_OLD_LIST_STRUCTURE)) {
      final boolean avroWriteOldListStructure = BooleanUtils
          .toBoolean(context.getProperty(AVRO_WRITE_OLD_LIST_STRUCTURE));
      parquetConfig.setAvroWriteOldListStructure(avroWriteOldListStructure);
    }

    return parquetConfig;
  }

  public static void applyCommonConfig(final ParquetWriter.Builder<?, ?> builder,
      final ParquetConfig parquetConfig) {
//    builder.withConf(conf);
    builder.withCompressionCodec(parquetConfig.getCompressionCodec());

    // Optional properties

    if (parquetConfig.getRowGroupSize() != null) {
      builder.withRowGroupSize(parquetConfig.getRowGroupSize());
    }

    if (parquetConfig.getPageSize() != null) {
      builder.withPageSize(parquetConfig.getPageSize());
    }

    if (parquetConfig.getDictionaryPageSize() != null) {
      builder.withDictionaryPageSize(parquetConfig.getDictionaryPageSize());
    }

    if (parquetConfig.getMaxPaddingSize() != null) {
      builder.withMaxPaddingSize(parquetConfig.getMaxPaddingSize());
    }

    if (parquetConfig.getEnableDictionaryEncoding() != null) {
      builder.withDictionaryEncoding(parquetConfig.getEnableDictionaryEncoding());
    }

    if (parquetConfig.getEnableValidation() != null) {
      builder.withValidation(parquetConfig.getEnableValidation());
    }

    if (parquetConfig.getWriterVersion() != null) {
      builder.withWriterVersion(parquetConfig.getWriterVersion());
    }

    if (parquetConfig.getWriterMode() != null) {
      builder.withWriteMode(parquetConfig.getWriterMode());
    }

//    applyCommonConfig(conf, parquetConfig);
  }

  public static void applyCommonConfig(Configuration conf, ParquetConfig parquetConfig) {
    if (parquetConfig.getAvroReadCompatibility() != null) {
      conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY,
          parquetConfig.getAvroReadCompatibility().booleanValue());
    }

    if (parquetConfig.getAvroAddListElementRecords() != null) {
      conf.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS,
          parquetConfig.getAvroAddListElementRecords().booleanValue());
    }

    if (parquetConfig.getAvroWriteOldListStructure() != null) {
      conf.setBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE,
          parquetConfig.getAvroWriteOldListStructure().booleanValue());
    }
  }
}

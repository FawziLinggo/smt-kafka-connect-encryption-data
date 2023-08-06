package dev.snowmancode.kafka.connect.smt;
import dev.snowmancode.kafka.connect.smt.model.AESEncryptionCustom;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public abstract class CustomDecrypt < R extends ConnectRecord < R >> implements Transformation < R > {
    private static final Logger logger = Logger.getLogger(CustomDecrypt.class.getName());

    public static final String OVERVIEW_DOC = "Decrypt the value of the record using AES encryption. ";
    interface ConfigName {
        String DATA_DECRYPTION_FIELD_NAME = "data.decryption.field.name";
        String DATA_DECRYPTION_KEY = "data.decryption.key";
    }

        public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.DATA_DECRYPTION_FIELD_NAME, ConfigDef.Type.STRING, "data", ConfigDef.Importance.HIGH,
                    "The field name of the data field in the record")
            .define(ConfigName.DATA_DECRYPTION_KEY, ConfigDef.Type.STRING, "s3cr3tK3y1234569", ConfigDef.Importance.HIGH,
                    "The secret key must have a length of 16");

    private static final String PURPOSE = "decrypt the value of the record using AES encryption";

    private String fieldName;

    private String key;

    private Cache < Schema, Schema > schemaUpdateCache;
    private final byte[] iv = new byte[16];

     public void configure(Map< String, ? > props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.DATA_DECRYPTION_FIELD_NAME);
        key = config.getString(ConfigName.DATA_DECRYPTION_KEY);
        schemaUpdateCache = new SynchronizedCache < > (new LRUCache < > (16));
     }



     @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
        try {
            return applySchemaless(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    } else {
        try {
            return applyWithSchema(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
  }
     private R applySchemaless(R record) throws Exception {
         final Map < String, Object > value = requireMap(operatingValue(record), PURPOSE);

         final Map < String, Object > updatedValue = new HashMap < > (value);

         String decryptedText = AESEncryptionCustom.decrypt(value.get(fieldName).toString(), key, iv);

         updatedValue.put(fieldName, decryptedText);


         return newRecord(record, null, updatedValue);
     }

     private R applyWithSchema(R record) throws Exception {
         final Struct value = requireStruct(operatingValue(record), PURPOSE);

         Schema updatedSchema = schemaUpdateCache.get(value.schema());
         if (updatedSchema == null) {
          updatedSchema = makeUpdatedSchema(value.schema());
          schemaUpdateCache.put(value.schema(), updatedSchema);
        }
         final Struct updatedValue = new Struct(updatedSchema);

         for (Field field: value.schema().fields()) {
                if (!field.name().equals(fieldName)) {
                    updatedValue.put(field.name(), value.get(field));
                }
         }
         logger.info("Password: " + key + " IV: " + Arrays.toString(iv));



         String decryptedText = AESEncryptionCustom.decrypt(value.get(fieldName).toString(), key, iv);


         updatedValue.put(fieldName, decryptedText);

         return newRecord(record, updatedSchema, updatedValue);
     }



  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field: schema.fields()) {
        if (!field.name().equals(fieldName)) {
          builder.field(field.name(), field.schema());
        }
    }

    builder.field(fieldName, Schema.STRING_SCHEMA);

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key < R extends ConnectRecord < R >> extends CustomDecrypt < R > {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }
public static class Value < R extends ConnectRecord < R >> extends CustomDecrypt < R > {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}
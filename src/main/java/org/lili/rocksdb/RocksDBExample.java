package org.lili.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.Arrays;
import java.util.List;

public class RocksDBExample {

    private static final String dbPath = "./rocksdb-data/";
    private static final String cfdbPath = "./rocksdb-data-cf/";

    static {
        RocksDB.loadLibrary();
    }

    public void testDefaultColumnFamily() {
        System.out.println("testDefaultColumnFamily begin...");
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath)) {
                //Simple key value
                byte[] key = "Hello".getBytes();
                rocksDB.put(key, "World".getBytes());

                System.out.println(new String(rocksDB.get(key)));

                rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

                //Query primary key through list
                List keys = Arrays.asList(key, "SecondKey".getBytes(), "missKey".getBytes());
                List values = rocksDB.multiGetAsList(keys);
                for (int i = 0; i < keys.size(); i++) {
                    System.out.println("multiGet " + new String(keys.get(i) + ":" + values.get(i)));
                }

                //Print all [key - value]
                RocksIterator iter = rocksDB.newIterator();
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
                }

                //Delete a key
                rocksDB.delete(key);
                System.out.println("after remove key:" + new String(key));

                iter = rocksDB.newIterator();
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        RocksDBExample test = new RocksDBExample();
        test.testDefaultColumnFamily();
    }

}

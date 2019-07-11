package com.bovery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author lyb
 */
public class HadoopDemo {

    public static FileSystem fileSystem;

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopDemo.class);

    private static void initFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(configuration);
    }



}

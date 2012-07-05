/**
 * Copyright 2011 Michael Cutler <m@cotdp.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cotdp.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Extends the basic FileInputFormat class provided by Apache Hadoop to accept ZIP files. It should be noted that ZIP
 * files are not 'splittable' and each ZIP file will be processed by a single Mapper.
 */
public class ZipFileInputFormat
    extends FileInputFormat<Text, BytesWritable>
{
    /** See the comments on the setLenient() method */
    private static boolean isLenient = false;
    
    /**
     * ZIP files are not splitable
     */
    @Override
    protected boolean isSplitable( JobContext context, Path filename )
    {
        return false;
    }

    /**
     * Create the ZipFileRecordReader to parse the file
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader( InputSplit split, TaskAttemptContext context )
        throws IOException, InterruptedException
    {
        return new ZipFileRecordReader();
    }
    
    /**
     * 
     * @param lenient
     */
    public static void setLenient( boolean lenient )
    {
        isLenient = lenient;
    }
    
    public static boolean getLenient()
    {
        return isLenient;
    }
}

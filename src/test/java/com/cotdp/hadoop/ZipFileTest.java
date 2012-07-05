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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * 
 */
public class ZipFileTest
    extends TestCase
{
    private static final Log LOG = LogFactory.getLog(ZipFileTest.class);
    
    /** Generate a working directory based on the Class name */
    Path workingPath = new Path("/tmp/" + this.getClass().getName());
    
    /** Input files are loaded into here */
    Path inputPath = new Path(workingPath + "/Input");
    
    /** Default configuration */
    Configuration conf = new Configuration();
    
    /** Used to prevent setUp() re-initialising more than once */
    private static boolean isInitialised = false;

    /**
     * Standard JUnit stuff 
     * @param testName
     */
    public ZipFileTest( String testName )
    {
        super( testName );
    }

    /**
     * Standard JUnit stuff
     * @return
     */
    public static Test suite()
    {
        return new TestSuite( ZipFileTest.class );
    }
    
    /**
     * Prepare the FileSystem and copy test files
     */
    @Override
    protected void setUp()
        throws Exception
    {
        // One-off initialisation
        if ( isInitialised == false )
        {
            LOG.info( "setUp() called, preparing FileSystem for tests" );
            
            // 
            FileSystem fs = FileSystem.get(conf);
            
            // Delete our working directory if it already exists
            LOG.info( "   ... Deleting " + workingPath.toString() );
            fs.delete(workingPath, true);
            
            // Copy the test files
            LOG.info( "   ... Copying files" );
            fs.mkdirs(inputPath);
            copyFile(fs, "zip-01.zip");
            copyFile(fs, "zip-02.zip");
            copyFile(fs, "zip-03.zip");
            copyFile(fs, "zip-04.dat");
            copyFile(fs, "random.dat");
            copyFile(fs, "encrypted.zip");
            copyFile(fs, "corrupt.zip");
            fs.close();
            
            //
            isInitialised = true;
        }
        
        // Reset ZipFileInputFormat leniency (false)
        ZipFileInputFormat.setLenient( false );
    }
    
    /**
     * This Mapper class checks the filename ends with the .txt extension, cleans
     * the text and then applies the simple WordCount algorithm.
     *
     */
    public static class MyMapper
        extends Mapper<Text, BytesWritable, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable( 1 );
        private Text word = new Text();

        public void map( Text key, BytesWritable value, Context context )
            throws IOException, InterruptedException
        {
            // NOTE: the filename is the *full* path within the ZIP file
            // e.g. "subdir1/subsubdir2/Ulysses-18.txt"
            String filename = key.toString();
            LOG.info( "map: " + filename );
            
            // We only want to process .txt files
            if ( filename.endsWith(".txt") == false )
                return;
            
            // Prepare the content 
            String content = new String( value.getBytes(), "UTF-8" );
            content = content.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
            
            // Tokenize the content
            StringTokenizer tokenizer = new StringTokenizer( content );
            while ( tokenizer.hasMoreTokens() )
            {
                word.set( tokenizer.nextToken() );
                context.write( word, one );
            }
        }
    }
    
    /**
     * Reducer for the ZipFile test, identical to the standard WordCount example
     */
    public static class MyReducer
        extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce( Text key, Iterable<IntWritable> values, Context context )
            throws IOException, InterruptedException
        {
            int sum = 0;
            for ( IntWritable val : values )
            {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /**
     * This test operates on a single file
     * 
     * Expected result: success
     * 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public void testSingle()
        throws IOException, ClassNotFoundException, InterruptedException
 
    {
        LOG.info( "============================================================" );
        LOG.info( "==                Running testSingle()                    ==" );
        LOG.info( "============================================================" );
        
        // Standard stuff
        Job job = new Job(conf);
        job.setJobName(this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        // 
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //
        ZipFileInputFormat.setInputPaths(job, new Path(inputPath, "zip-01.zip"));
        TextOutputFormat.setOutputPath(job, new Path(workingPath, "Output_Single"));
        
        //
        assertTrue( job.waitForCompletion(true) );
    }

    /**
     * This test operates on a Path containing files that will cause the Job to fail
     * 
     * Expected result: failure
     * 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public void testMultiple()
        throws IOException, ClassNotFoundException, InterruptedException
 
    {
        LOG.info( "============================================================" );
        LOG.info( "==                Running testMultiple()                  ==" );
        LOG.info( "============================================================" );

        // Standard stuff
        Job job = new Job(conf);
        job.setJobName(this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        // 
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //
        ZipFileInputFormat.setInputPaths(job, inputPath);
        TextOutputFormat.setOutputPath(job, new Path(workingPath, "Output_Multiple"));
        
        //
        assertFalse( job.waitForCompletion(true) );
    }
    

    /**
     * This test is identical to testMultiple() however the ZipFileInputFormat is set to
     * be lenient, errors that cause testMultiple() to fail will be quietly ignored here.
     * 
     * Expected result: success
     * 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public void testMultipleLenient()
        throws IOException, ClassNotFoundException, InterruptedException
 
    {
        LOG.info( "============================================================" );
        LOG.info( "==                Running testMultipleLenient()           ==" );
        LOG.info( "============================================================" );

        // Standard stuff
        Job job = new Job(conf);
        job.setJobName(this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        // 
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //
        ZipFileInputFormat.setLenient( true );
        ZipFileInputFormat.setInputPaths(job, inputPath);
        TextOutputFormat.setOutputPath(job, new Path(workingPath, "Output_MultipleLenient"));
        
        //
        assertTrue( job.waitForCompletion(true) );
    }
    
    /**
     * ZipInputStream doesn't support encrypted entries thus this will fail.
     * 
     * Expected result: failure
     * 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public void testEncryptedZip()
        throws IOException, ClassNotFoundException, InterruptedException
 
    {
        LOG.info( "============================================================" );
        LOG.info( "==                Running testEncryptedZip()              ==" );
        LOG.info( "============================================================" );

        // Standard stuff
        Job job = new Job(conf);
        job.setJobName(this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        // 
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //
        ZipFileInputFormat.setInputPaths(job, new Path(inputPath, "encrypted.zip"));
        TextOutputFormat.setOutputPath(job, new Path(workingPath, "Output_Encrypted"));
        
        //
        assertFalse( job.waitForCompletion(true) );
    }

    
    /**
     * This test explicitly tries to read a file containing random noise as a ZIP file,
     * the expected result is a quiet failure. The Job shouldn't fail if non-ZIP data is
     * encountered.
     * 
     * Expected result: (quiet) failure
     * 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public void testNonZipData()
        throws IOException, ClassNotFoundException, InterruptedException
 
    {
        LOG.info( "============================================================" );
        LOG.info( "==                Running testNonZipData()                ==" );
        LOG.info( "============================================================" );
        
        // Standard stuff
        Job job = new Job(conf);
        job.setJobName(this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        // 
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //
        ZipFileInputFormat.setInputPaths(job, new Path(inputPath, "random.dat"));
        TextOutputFormat.setOutputPath(job, new Path(workingPath, "Output_NonZipData"));
        
        //
        assertTrue( job.waitForCompletion(true) );
    }
    
    /**
     * This test refers to a corrupt (truncated) ZIP file, upon reaching the corruption
     * the Job will fail and no output will be written through the Reducer.
     * 
     * Expected result: failure
     * 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public void testCorruptZip()
        throws IOException, ClassNotFoundException, InterruptedException
 
    {
        LOG.info( "============================================================" );
        LOG.info( "==                Running testCorruptZip()                ==" );
        LOG.info( "============================================================" );
        
        // Standard stuff
        Job job = new Job(conf);
        job.setJobName(this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        // 
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //
        ZipFileInputFormat.setInputPaths(job, new Path(inputPath, "corrupt.zip"));
        TextOutputFormat.setOutputPath(job, new Path(workingPath, "Output_Corrupt"));
        
        //
        assertFalse( job.waitForCompletion(true) );
    }
    
    /**
     * This test refers to a corrupt (truncated) ZIP file, upon reaching the corruption
     * the Mapper will ignore the corrupt entry and close the ZIP file. All previous
     * output will be treated as normal and passed through the Reducer. 
     * 
     * Expected result: success
     * 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public void testCorruptZipLenient()
        throws IOException, ClassNotFoundException, InterruptedException
 
    {
        LOG.info( "============================================================" );
        LOG.info( "==                Running testCorruptZipLenient()         ==" );
        LOG.info( "============================================================" );
        
        // Standard stuff
        Job job = new Job(conf);
        job.setJobName(this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        // 
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // The output files will contain "Word [TAB] Count"
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //
        ZipFileInputFormat.setLenient( true );
        ZipFileInputFormat.setInputPaths(job, new Path(inputPath, "corrupt.zip"));
        TextOutputFormat.setOutputPath(job, new Path(workingPath, "Output_CorruptLenient"));
        
        //
        assertTrue( job.waitForCompletion(true) );
    }
    
    /**
     * Simple utility function to copy files into HDFS
     * 
     * @param fs
     * @param name
     * @throws IOException
     */
    private void copyFile(FileSystem fs, String name)
        throws IOException
    {
        LOG.info( "copyFile: " + name );
        InputStream is = this.getClass().getResourceAsStream( "/" + name );
        OutputStream os = fs.create( new Path(inputPath, name), true );
        IOUtils.copyBytes( is, os, conf );
        os.close();
        is.close();
    }
    
}

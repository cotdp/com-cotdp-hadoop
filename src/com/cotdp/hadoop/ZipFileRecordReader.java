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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ZipFileRecordReader extends RecordReader<Text,BytesWritable>
{
	private FSDataInputStream fsin;
	private ZipInputStream zip;
	private Text currentKey;
	private BytesWritable currentValue;
	private boolean isFinished = false;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
	{
		FileSplit split = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);
        fsin = fs.open(path);
		zip = new ZipInputStream(fsin);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		ZipEntry entry = zip.getNextEntry();
		if ( entry == null )
		{
			isFinished = true;
			return false;
		}
		currentKey = new Text( entry.getName() );
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] temp = new byte[8192];
		while ( true )
		{
			int bytesRead = zip.read(temp, 0, 8192);
			if ( bytesRead > 0 )
				bos.write(temp, 0, bytesRead);
			else
				break;
		}
		zip.closeEntry();
		currentValue = new BytesWritable( bos.toByteArray() );
		return true;
	}	

	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		return isFinished ? 1 : 0;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException
	{
		return currentKey;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException
	{
		return currentValue;
	}

	@Override
	public void close() throws IOException
	{
		try { zip.close(); } catch (Exception e) { }
		try { fsin.close(); } catch (Exception e) { }
	}
}


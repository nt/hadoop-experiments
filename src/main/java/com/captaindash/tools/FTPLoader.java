package com.captaindash.tools;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtil;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FTPLoader {
	
	public static class DownloadFTPFileMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException ,InterruptedException {			
			URI file;
			try {
				file = new URI(value.toString().split(" ")[0]);
			} catch (URISyntaxException e) {
				e.printStackTrace();
				return;
			}
			
			FTPClient ftp = new FTPClient();
			ftp.connect(file.getAuthority());
			ftp.login("anonymous", "");
			
			String fout = value.toString().split(" ")[1]+file.getPath();
			FileSystem fs = FileSystem.get(URI.create(fout), context.getConfiguration());
			OutputStream out = fs.create(new Path(fout));
			
			//ftp.sendCommand("cd "%)
			
			ftp.cwd(new Path(file.getPath()).getParent().toString());
			//ftp.completePendingCommand();
			
			InputStream is = ftp.retrieveFileStream(new Path(file.getPath()).getName());
			if(is==null) {
				System.out.println("InputStream null for: "+file);
				return;
			}
			System.out.println("Downloading "+file);
			IOUtil.copy(is, out);
			IOUtil.shutdownStream(is);
			IOUtil.shutdownStream(out);
			context.write(value, new Text("ok"));
			ftp.disconnect();
		}
	}
	
	public static List<String> listRecursive(FTPClient ftp, String directory) throws Exception {
		System.out.println("Inspecting "+directory);
		LinkedList<String> files = new LinkedList<String>();
		for(FTPFile f:ftp.listFiles(directory)){
			if(f.isFile()) files.add(directory+"/"+f.getName());
			if(f.isDirectory()) files.addAll(listRecursive(ftp, directory+"/"+f.getName()));
		}
		return files;
	}

	public static void main(String[] args) throws Exception {
		
		// Download file list
		FTPClient ftp = new FTPClient();
		URI source = new URI(args[0]);
		
		ftp.connect(source.getAuthority());
		
		ftp.login("anonymous", "");
		List<String> files = listRecursive(ftp, source.getPath());
		ftp.disconnect();
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create("/tmp/filelist"), conf);
		OutputStream out = fs.create(new Path("/tmp/filelist"));
		
		for(String s:files) {
			IOUtil.copy(source.getScheme()+"://"+source.getHost()+s+" "+args[1]+"\n", out);
		}
		
		IOUtil.shutdownStream(out);
		
		// Download Files in a map reduce job
		Job j = new Job(conf);

		j.setJarByClass(FTPLoader.class);
		
		FileInputFormat.setInputPaths(j, "/tmp/filelist");
		FileInputFormat.setMaxInputSplitSize(j, 1000);
		
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		
		j.setMapperClass(DownloadFTPFileMapper.class);
		
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
	
}

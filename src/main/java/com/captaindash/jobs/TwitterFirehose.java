package com.captaindash.jobs;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.io.IOUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;


public class TwitterFirehose {
	
	DefaultHttpClient httpclient = new DefaultHttpClient();
	
	InputStream getInputStream(String user, String pass) throws IllegalStateException, IOException {
		httpclient.getCredentialsProvider().setCredentials(
                new AuthScope("stream.twitter.com", 443),
                new UsernamePasswordCredentials(user, pass));

        HttpGet httpget = new HttpGet("https://stream.twitter.com/1/statuses/sample.json");

        System.out.println("executing request" + httpget.getRequestLine());
        HttpResponse response = httpclient.execute(httpget);
        HttpEntity entity = response.getEntity();

        System.out.println("----------------------------------------");
        System.out.println(response.getStatusLine());
        if (entity != null) {
            System.out.println("Response content length: " + entity.getContentLength());
        }
        return entity.getContent();
	}
	
	void close(){
		httpclient.getConnectionManager().shutdown();
	}
	
	public static void main(String[] args) throws ClientProtocolException, IOException {
		int N = Integer.parseInt(args[0]);
		TwitterFirehose tf = new TwitterFirehose();
		String dst = args[3];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		
		OutputStream out = fs.create(new Path(dst));
		InputStream is = tf.getInputStream(args[1], args[2]);
		
		for(int i=0;i<N/2;i++) {
			IOUtil.copy(is, out, 2000000);
        	//if((i % (N/50)) == 0)
        		System.out.println(i+"/"+(N*2));
		}
		
		IOUtil.shutdownStream(is);
		IOUtil.shutdownStream(out);
		
		out.close();
		tf.close();
		
		/*// In this version N represents the number of lines
		BufferedReader br = new BufferedReader(new InputStreamReader(tf.getInputStream(args[1], args[2])));
            for(int i=0;i<N;i++){
            	String line = br.readLine();
            	if(line==null)
            		break;
            	out.write(line.getBytes());
            	out.write("\n".getBytes());
            	if((i % (N/100)) == 0)
            		System.out.println(i+"/"+N);
            }
            out.close();
            tf.close();
		*/
	}
	
}

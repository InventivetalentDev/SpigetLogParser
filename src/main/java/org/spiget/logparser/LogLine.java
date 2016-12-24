package org.spiget.logparser;

import lombok.Data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
public class LogLine {

	public static SimpleDateFormat dateFormat;

	private String address;
	private Date   time;
	private String method;
	private String url;
	private String path;
	private String parameters;
	private String apiVersion;
	private int    status;
	private int    bytesSent;
	private String referrer;
	private String userAgent;

	public LogLine update(String address, String time, String method, String url, String status, String bytesSent, String referrer, String userAgent) throws ParseException {
		this.address = address;
		this.method = method;
		this.url = url;
		if ("-".equals(referrer)) { referrer = null; }
		this.referrer = referrer;
		this.userAgent = userAgent;

		this.time = dateFormat.parse(time);
		this.status = Integer.parseInt(status);
		this.bytesSent = Integer.parseInt(bytesSent);

		if (url.contains("?")) {
			String[] split = url.split("\\?");
			this.path = split[0];
			this.parameters = split[1];
		} else {
			this.path = url;
			this.parameters = "";
		}

		String[] versionSplit = url.split("/");
		apiVersion = versionSplit[1];

		// Strip API version
		this.path = this.path.substring(("/" + apiVersion).length());

		return this;
	}

}

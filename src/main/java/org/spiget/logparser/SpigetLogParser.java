package org.spiget.logparser;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.log4j.Log4j2;
import org.spiget.database.DatabaseClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Log4j2
public class SpigetLogParser {

	public JsonObject     config;
	public DatabaseClient databaseClient;
	public Pattern        logPattern;

	public LogLine    currentLine = new LogLine();
	public JsonObject parsedData  = new JsonObject();

	public SpigetLogParser init() throws IOException {
		this.config = new JsonParser().parse(new FileReader("config.json")).getAsJsonObject();

		this.logPattern = Pattern.compile(this.config.get("log").getAsJsonObject().get("regex").getAsString(), Pattern.CASE_INSENSITIVE);
		LogLine.dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

		this.parsedData.addProperty("total", 0);
		this.parsedData.add("userAgents", new JsonObject());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					log.info("Disconnecting database...");
					databaseClient.disconnect();
				} catch (IOException e) {
					log.warn("Failed to disconnect from database", e);
				}
			}
		});

		if (config.get("database.enabled").getAsBoolean()) {
			log.info("Initializing & testing database connection...");
			long testStart = System.currentTimeMillis();
			try {
				this.databaseClient = new DatabaseClient(
						config.get("database.name").getAsString(),
						config.get("database.host").getAsString(),
						config.get("database.port").getAsInt(),
						config.get("database.user").getAsString(),
						config.get("database.pass").getAsString().toCharArray(),
						config.get("database.db").getAsString());
				databaseClient.connect(config.get("database.timeout").getAsInt());
				databaseClient.collectionCount();
				log.info("Connection successful (" + (System.currentTimeMillis() - testStart) + "ms)");
			} catch (Exception e) {
				log.fatal("Connection failed after " + (System.currentTimeMillis() - testStart) + "ms", e);
				log.fatal("Aborting.");
				System.exit(-1);
				return null;
			}
		} else {
			log.info("Database is disabled");
		}

		return this;
	}

	public void parse() throws IOException, ParseException {
		//TODO...
		//...

		File currentLog = new File("D:\\downloads\\access.log");
		try (BufferedReader reader = new BufferedReader(new FileReader(currentLog))) {
			String line;
			while ((line = reader.readLine()) != null) {
				System.out.println(parseLine(line));
			}
		}
	}

	public LogLine parseLine(String line) throws ParseException {
		System.out.println(line);
		if (line.isEmpty()) { return null; }

		Matcher matcher = logPattern.matcher(line);
		if (!matcher.find()) {
			return null;
		}

		String address = matcher.group("address");
		String time = matcher.group("timestamp");
		String method = matcher.group("method");
		String url = matcher.group("url");
		String status = matcher.group("status");
		String bytesSent = matcher.group("bytesSent");
		String referrer = matcher.group("referrer");
		String userAgent = matcher.group("userAgent");

		if ("500".equals(status)) {
			log.warn("Found Response Code 500 for " + url);
			log.warn(line);
		}

		return (this.currentLine = currentLine.update(address, time, method, url, status, bytesSent, referrer, userAgent));
	}

}

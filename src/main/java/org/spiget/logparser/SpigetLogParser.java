package org.spiget.logparser;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.spiget.database.DatabaseClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

@Log4j2
public class SpigetLogParser {

	public JsonObject     config;
	public DatabaseClient databaseClient;
	public Pattern        logPattern;

	public LogLine           currentLine = new LogLine();
	public Set<String>       ips         = new HashSet<>();
	public Map<String, File> logFiles    = new HashMap<>();

	public Stats              globalStats  = new Stats();
	public Map<String, Stats> versionStats = new HashMap<>();

	public void dumpResult() {
		System.out.println("Global: " + new Gson().toJson(globalStats));

		for (Map.Entry<String, Stats> entry : versionStats.entrySet()) {
			System.out.println(entry.getKey() + ": " + new Gson().toJson(entry.getValue()));
		}
	}

	public SpigetLogParser init() throws IOException {
		this.config = new JsonParser().parse(new FileReader("config.json")).getAsJsonObject();

		this.logPattern = Pattern.compile(this.config.get("log").getAsJsonObject().get("regex").getAsString(), Pattern.CASE_INSENSITIVE);
		LogLine.dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

		versionStats.put("v1", new Stats());
		versionStats.put("v2", new Stats());

		if (config.get("database.enabled").getAsBoolean()) {
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

	public void downloadLogs() {
		log.info("Downloading logs...");

		JsonObject auth = config.get("log").getAsJsonObject().get("auth").getAsJsonObject();
		String authToken = auth.get("token").getAsString();
		String authPass = auth.get("pass").getAsString();

		Server[] servers = new Gson().fromJson(config.get("log").getAsJsonObject().get("servers"), Server[].class);
		for (Server server : servers) {
			try {
				long downloadStart = System.currentTimeMillis();
				log.info("Downloading log from server " + server.getName() + "...");

				URL url = new URL(server.getUrl());
				URLConnection connection = url.openConnection();
				connection.addRequestProperty("X-Auth-Token", authToken);
				connection.addRequestProperty("X-Auth-Pass", authPass);

				if (connection instanceof HttpURLConnection) {
					HttpURLConnection httpConnection = (HttpURLConnection) connection;
					InputStream inputStream;
					try {
						inputStream = httpConnection.getInputStream();
					} catch (IOException exception) {
						inputStream = httpConnection.getErrorStream();
					}
					if (httpConnection.getResponseCode() != 200) {
						log.warn("Failed to download log from " + server.getName() + " (" + server.getUrl() + ")" + ". Response Code " + httpConnection.getResponseCode());
						try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
							String line;
							while ((line = reader.readLine()) != null) {
								log.warn(line);
							}
						}
						continue;
					}

					File tempFile = File.createTempFile("spiget-" + server.getName() + "-", null);
					ReadableByteChannel byteChannel = Channels.newChannel(inputStream);
					FileOutputStream outputStream = new FileOutputStream(tempFile);
					outputStream.getChannel().transferFrom(byteChannel, 0, Long.MAX_VALUE);

					outputStream.close();
					byteChannel.close();

					long downloadEnd = System.currentTimeMillis();

					logFiles.put(server.getName(), tempFile);
					log.info("Download successful (" + tempFile + ") " + ((downloadEnd - downloadStart) / 1000.0) + "s");
				}
			} catch (IOException e) {
				log.log(Level.ERROR, "Exception while downloading log from " + server.getName(), e);
			}
		}

		if (logFiles.isEmpty()) {
			log.warn("Could not download any logs. Exiting.");
			System.exit(0);
		} else {
			log.info("Downloaded logs.");
		}
	}

	public void parse() throws IOException, ParseException {
		log.info("Parsing logs...");
		for (String server : logFiles.keySet()) {
			log.info("Parsing " + server + "...");
			parseFile(server, logFiles.get(server));
		}
	}

	public void parseFile(String server, File file) throws IOException, ParseException {
		// MongoDB
		server = server.replace(".", "_");

		long parseStart = System.currentTimeMillis();
		long lineCounter = 0;

		GZIPInputStream inputStream = new GZIPInputStream(new FileInputStream(file));
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			String line;
			while ((line = reader.readLine()) != null) {
				LogLine logLine = parseLine(line);
				if (logLine == null) {
					//					log.warn("Failed to parse line:  " + line);
					continue;
				}
				lineCounter++;

				// Global
				globalStats.total++;
				globalStats.increaseUserAgent(logLine.getUserAgent());
				globalStats.increasePath(logLine.getPath());
				globalStats.increaseMethod(logLine.getMethod());
				globalStats.increaseServer(server);

				// Version-Specific
				Stats versionStats = this.versionStats.get(logLine.getApiVersion());
				if (versionStats == null) {
					log.error("Missing Stats object for version '" + logLine.getApiVersion() + "'");
				} else {
					versionStats.total++;
					versionStats.increaseUserAgent(logLine.getUserAgent());
					versionStats.increasePath(logLine.getPath());
					versionStats.increaseMethod(logLine.getMethod());
					versionStats.increaseServer(server);
				}

				if (!ips.contains(logLine.getAddress())) {
					globalStats.unique++;
					if (versionStats != null) { versionStats.unique++; }

					ips.add(logLine.getAddress());
				}
			}
		}

		long parseEnd = System.currentTimeMillis();

		log.info("Parsed " + lineCounter + " lines in " + ((parseEnd - parseStart) / 1000.0) + "s");
	}

	public LogLine parseLine(String line) throws ParseException {
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

		if (!url.startsWith("/v")) {// Some other path that's not part of on API
			return null;
		}

		if ("500".equals(status)) {
			log.warn("Found Response Code 500 for " + url);
			log.warn(line + "\n");
		}
		if (!"200".equals(status)) {
			// Don't return anything, so we don't log invalid requests (or redirects)
			return null;
		}

		try {
			currentLine = currentLine.update(address, time, method, url, status, bytesSent, referrer, userAgent);
		} catch (Exception e) {
			log.warn("Line update failed for " + line, e);
		}

		// Parse User-Agents
		handleSpecialUserAgents(currentLine);
		simplifyPath(currentLine);
		return currentLine;
	}

	public LogLine handleSpecialUserAgents(LogLine line) {
		if (line.getUserAgent() == null) {
			line.setUserAgent("unknown");
			return line;
		}

		if (line.getReferrer() != null) {
			// Internal Requests
			if (line.getReferrer().startsWith("https://spiget.org")) {
				line.setUserAgent("Spiget");
				return line;
			}
		}

		// Browsers
		String lowerCaseUserAgent = line.getUserAgent().toLowerCase();
		if (lowerCaseUserAgent.contains("mozilla") || lowerCaseUserAgent.contains("chrome") || lowerCaseUserAgent.contains("opera") || lowerCaseUserAgent.contains("applewebkit")) {
			line.setUserAgent("default");
			return line;
		}

		// Extra regexes
		for (JsonElement object : config.get("log").getAsJsonObject()
				.get("format").getAsJsonObject()
				.get("truncateRegex")
				.getAsJsonArray()) {
			String regex = object.getAsString();
			Matcher matcher = Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(line.getUserAgent());
			if (matcher.find()) {
				line.setUserAgent(matcher.group(1));
			}
		}

		// MongoDB
		line.setUserAgent(line.getUserAgent().replace(".", "_"));

		return line;
	}

	public LogLine simplifyPath(LogLine line) {
		for (JsonElement object : config.get("log").getAsJsonObject()
				.get("format").getAsJsonObject()
				.get("simplifiedPathRegex").getAsJsonObject()
				.get(line.getApiVersion())
				.getAsJsonArray()) {
			String regex = object.getAsString();
			Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

			String original = line.getPath();
			String replaced = replaceGroups(pattern, original, "x");

			if (!original.equals(replaced)) {
				line.setPath(replaced);
				// Break here, so we don't unnecessarily replace more than needed
				break;
			}

		}

		// MongoDB
		line.setPath(line.getPath().replace(".", "_"));

		return line;
	}

	public void saveToDatabase() {
		Gson gson = new Gson();

		JsonObject data = new JsonObject();
		data.addProperty("timestamp", System.currentTimeMillis() / 1000);
		data.add("global", gson.toJsonTree(globalStats));

		JsonObject versions = new JsonObject();
		for (Map.Entry<String, Stats> entry : versionStats.entrySet()) {
			versions.add(entry.getKey(), gson.toJsonTree(entry.getValue()));
		}
		data.add("versions", versions);

		System.out.println(data);

		if (config.get("database.enabled").getAsBoolean()) {
			databaseClient.insertMetricsData(data);
		}
	}

	public void cleanup() {
		logFiles.values().forEach(File::delete);
	}

	public String replaceGroups(Pattern pattern, String source, String replacement) {
		Matcher m = pattern.matcher(source);
		if (m.find()) {
			String newString = "";
			int lastEnd = 0;

			// Build the string manually instead of just replacing all groups over and over,
			// because the match-position moves with every replaced word
			for (int i = 1; i < m.groupCount() + 1; i++) {
				if (m.group(i).equals(replacement)) {
					continue;
				}

				newString += source.substring(lastEnd, m.start(i));
				newString += replacement;
				lastEnd = m.end(i);
			}
			newString += source.substring(lastEnd, source.length());

			return newString;
		}
		return source;
	}

}

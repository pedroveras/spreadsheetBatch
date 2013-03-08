import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Scanner;

import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.Link;
import com.google.gdata.data.batch.BatchOperationType;
import com.google.gdata.data.batch.BatchUtils;
import com.google.gdata.data.spreadsheet.CellEntry;
import com.google.gdata.data.spreadsheet.CellFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetFeed;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;
import com.mysql.jdbc.PreparedStatement;

/**
 * 
 * @author Pedro Henriquw
 * @email pedroveras@gmail.com
 * 
 */
public class Spreadsheet {

	private SpreadsheetService service;
	private String spreadsheetKey;
	private FeedURLFactory factory;
	private Connection c;
	private PreparedStatement pstm;

	public Spreadsheet(SpreadsheetService service, String spreadsheetKey) {
		this.service = service;
		this.factory = FeedURLFactory.getDefault();
		this.spreadsheetKey = spreadsheetKey;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Scanner s = new Scanner(System.in);
		String hostname = null;
		String username = null;
		String password = null;
		String database = null;
		String query = null;
		String spreadsheetKey = null;
		String userGoogle = null;
		String passGoogle = null;

		System.out.println("MySql Hostname:");
		hostname = s.nextLine();

		System.out.println("MySql database:");
		database = s.nextLine();

		System.out.println("MySql Username:");
		username = s.nextLine();

		System.out.println("MySql Password:");
		password = s.nextLine();

		System.out.println("SQL");
		query = s.nextLine();

		System.out.println("Spreadsheet key:");
		spreadsheetKey = s.nextLine();

		System.out.println("Google username:");
		userGoogle = s.nextLine();

		System.out.println("Google password:");
		passGoogle = s.nextLine();

		Spreadsheet spreadsheet = new Spreadsheet(new SpreadsheetService("Spreadsheet"), spreadsheetKey);
		spreadsheet.login(userGoogle, passGoogle);
		ResultSet rs = spreadsheet.connectAndLoad(hostname, database, username,password, query);
		
		spreadsheet.process(rs);
		
	}

	public ResultSet connectAndLoad(String hostname, String database,
			String username, String password, String sql) {
		ResultSet rs = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			c = DriverManager.getConnection("jdbc:mysql://" + hostname
					+ ":3306/" + database + "?user=" + username + "&password="
					+ password);
			pstm = (PreparedStatement) c.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			rs = pstm.executeQuery();
			
			return rs;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return rs;
	}

	public void login(String username, String password) {
		try {
			service.setUserCredentials(username, password);
		} catch (AuthenticationException e) {
			e.printStackTrace();
		}
	}

	public void process(ResultSet rs) {
		ResultSetMetaData rsMetaData = null;
		try {
			URL worksheetFeedlistFeed = factory.getWorksheetFeedUrl(spreadsheetKey, "private", "full");
			WorksheetFeed feed = service.getFeed(worksheetFeedlistFeed,	WorksheetFeed.class);
			WorksheetEntry worksheet = feed.getEntries().get(0);
			rsMetaData = rs.getMetaData();
			int numberOfColumns = rsMetaData.getColumnCount();
			
			// set the number of row to 1, deleting other rows, keeping only the title row
			worksheet.setRowCount(1);
			worksheet.update();
			int numberOfRows = 0;
			
			if (rs.last()) {
				numberOfRows = rs.getRow();
				rs.beforeFirst();
			}
			
			feed = service.getFeed(worksheetFeedlistFeed, WorksheetFeed.class);
			worksheet = feed.getEntries().get(0);
			
			// set the number of rows according to the number of number of rows returned from DB
			worksheet.setRowCount(numberOfRows+1);
     		worksheet.update();

			int i = 1;
			int j = 2;
			CellFeed batchRequest = new CellFeed();			
			int totalRows = 0;
			CellFeed cellFeed = service.getFeed(worksheet.getCellFeedUrl(), CellFeed.class); 
			Link batchLink = cellFeed.getLink(Link.Rel.FEED_BATCH, Link.Type.ATOM);
			service.setHeader("If-Match", "*");
			
			while (rs.next()) {
				while (i <= numberOfColumns) {
					 String id = "R"+j+"C"+i;
					 CellEntry batchEntry = new CellEntry(j, i, id);
		             batchEntry.setId(String.format("%s/%s", worksheet.getCellFeedUrl().toString(), id));
		             batchEntry.changeInputValueLocal(rs.getString(i));
		             BatchUtils.setBatchId(batchEntry,id);
		             BatchUtils.setBatchOperationType(batchEntry, BatchOperationType.UPDATE);  
		             batchRequest.getEntries().add(batchEntry);
		             
		             if (totalRows >= 50 || rs.isLast()) {
		            	 service.batch(new URL(batchLink.getHref()), batchRequest);
		            	 batchRequest = new CellFeed(); 
		            	 totalRows = 0;
		             }
		             
		             i++;
				}
				
				i = 1;
				j++;
				totalRows++;
			}
			 service.batch(new URL(batchLink.getHref()), batchRequest);

			rs.close();
			pstm.close();
			c.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ServiceException e) {
			e.printStackTrace();
		}

	}

}

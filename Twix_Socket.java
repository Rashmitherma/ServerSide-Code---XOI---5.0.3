import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import com.twix.*;

public class Twix_Socket extends Thread
	{
	private boolean KeepRunning = true;
	private SimpleDateFormat TimeStamper;
	
	// Constants
	public static final int HOST_PORT = 2600;
	//public static final int HOST_PORT = 2600;
	public static final String VERSION = "3.0.0";
	public static final String SOCKET_VERSION = "2.0.0";
		// Server Socket Timeout
	public static final int MAX_TIMEOUT = 60000;
	
	// Active Logins Mapping
		// Key: Secret Key automatically generated
		// Value: Login Contents class that contains username, empno, etc
	private Map<String, Connect.LoginContents> ActiveLogins;
	
	// Database Connection Variables
	private Connection db;
	private Connection Therma_DW;
	private DBConnectionInfo DBConInfo;
	private Object dbLock;
	
	private ServerSocket server;
	private ReconnectThread Reconnection;
	
	public static void main(String argv[]) throws Exception
		{
		if( argv.length != 8 )
			System.out.println("Twix Mobile Server takes 8 parameters." +
					"\n\ttwix_mobile.jar [Database URL] [Database port] " +
					"[Database Name] [username] [password] " +
					"[Datawarehouse Name] [DW username] [DW password]");
		else
			new Twix_Socket(argv);
		}
	
	public Twix_Socket(String argv[]) throws Exception
		{
		// Ensure the Running Variable is properly sec
		KeepRunning = true;
		TimeStamper = new SimpleDateFormat("yyyy-MM-dd hh:mm a");
		
		// Setup the client map
		ActiveLogins = new TreeMap<String, Connect.LoginContents>();
		dbLock = new Object();
		DBConInfo = new DBConnectionInfo(argv);
		
		// Connect to the database and prepare the statements
		Reconnection = new ReconnectThread(this, DBConInfo);
		Reconnection.start();
		try
			{
			server = new ServerSocket(HOST_PORT);
			System.out.println(TimeStamper.format(new Date()) + " - Starting Twix Communicator Server...");
			System.out.println("Twix Communicator Server v" + SOCKET_VERSION + " listening on port " + HOST_PORT);
			System.out.println("\tCompatible with Twix Client v" + VERSION);
			this.start();
			}
		catch( IOException e )
			{
			System.out.println("Server Port already in use. Disconnecting Database...");
			DisconnectAllDBs();
			ShutDownServer();
			}
		catch( Exception e )
			{
			System.out.println("Failed to start the server port. Generic Catch. Shutting Down Server...");
			System.out.println("\tError: " + e.getMessage());
			e.printStackTrace();
			DisconnectAllDBs();
			ShutDownServer();
			}
		
		}

	public void run()
		{
		new CleanupThread(ActiveLogins);
		while (KeepRunning)
			{
			try
				{
				System.out.println("Waiting for connections.");
				Socket client = server.accept();
				client.setSoTimeout(MAX_TIMEOUT);
				client.setSoLinger(true, 0);
				client.setKeepAlive(true);
				System.out.println("Accepted a connection from: "
						+ client.getInetAddress());
				new Connect(client, db, Therma_DW, ActiveLogins );
				}
			catch (SQLException e)
				{
				System.out.println(TimeStamper.format(new Date()) +
						" - SQL Server Connection Lost. Attempting to Reconnect...");
				if( Reconnection == null )
					{
					Reconnection = new ReconnectThread(this, DBConInfo);
					Reconnection.start();
					}
				}
			catch(Exception e)
				{
				e.printStackTrace();
				}
			}
		}
	
	public void ShutDownServer()
		{
		KeepRunning = false;
		if( Reconnection != null )
			{
			Reconnection.StopRunning();
			Reconnection = null;
			}
		}
	
	public void EndReconnectionThread()
		{
		Reconnection = null;
		}
	
	public boolean IsRunning()
		{
		return KeepRunning;
		}
	
	public void DisconnectAllDBs()
		{
		DisconnectDB(db);
		DisconnectDB(Therma_DW);
		db = null;
		Therma_DW = null;
		}
	
	private synchronized void DisconnectDB(Connection db)
		{
		if( db != null )
			{
			try
				{ db.close(); }
			catch (SQLException e)
				{ e.printStackTrace(); }
			}
		}
	
	public boolean DBConnectionsValid()
		{
		try
			{ return (db != null && db.isValid(10)) && (Therma_DW != null && Therma_DW.isValid(10)); }
		catch ( SQLException e )
			{ return false; }
		}
	
	public void SetDBConnections(Connection db, Connection Therma_DW)
		{
		synchronized( dbLock )
			{
			this.db = db;
			this.Therma_DW = Therma_DW;
			}
		}
	}

class DBConnectionInfo
	{
	public String db_URL;
	public String db_port;
	
	public String db_path;
	public String db_name;
	public String db_username;
	public String db_password;
	
	public String DW_path;
	public String DW_name;
	public String DW_username;
	public String DW_password;
	
	public String db_class;
	
	public DBConnectionInfo(String argv[])
		{
		// Database Parameters
		db_URL		= argv[0];
		db_port		= argv[1];
		db_name		= argv[2];
		db_username	= argv[3];
		db_password	= argv[4];
		
		// Data Warehouse Parameters. Used to fetch customer billing info
		DW_name		= argv[5];
		DW_username	= argv[6];
		DW_password	= argv[7];
		
		db_class = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		db_path = "jdbc:sqlserver://" + db_URL + ":" + db_port + ";DatabaseName=" + db_name + ";";
		DW_path = "jdbc:sqlserver://" + db_URL + ":" + db_port + ";DatabaseName=" + DW_name + ";";
		}
	}

class ReconnectThread extends Thread
	{
	private Twix_Socket TwixServer;
	private DBConnectionInfo DBConInfo;
	private Connection db;
	private Connection Therma_DW;
	private boolean KeepRunning = true;
	private SimpleDateFormat TimeStamper;
	boolean StatedConnectionFailed = false;
	
	public ReconnectThread(Twix_Socket TwixServer, DBConnectionInfo DBConInfo)
		{
		this.KeepRunning = true;
		this.TwixServer = TwixServer;
		this.DBConInfo = DBConInfo;
		TimeStamper = new SimpleDateFormat("yyyy-MM-dd hh:mm a");
		StatedConnectionFailed = false;
		}
	
	public void run()
		{
		TwixServer.DisconnectAllDBs();
		while( this.KeepRunning && TwixServer != null && TwixServer.IsRunning() )
			{
			ConnectDB();
			if( DBConnectionsValid() )
				{
				TwixServer.SetDBConnections(db, Therma_DW);
				this.StopRunning();
				System.out.println(TimeStamper.format(new Date()) + " - Successfully connected to Databases.");
				break;
				}
			try
				{ sleep(5000); }
			catch ( InterruptedException e )
				{ e.printStackTrace(); }
			}
		TwixServer.EndReconnectionThread();
		}
	
	private void ConnectDB()
		{
		try
			{
			Class.forName(DBConInfo.db_class);
			db = DriverManager.getConnection(DBConInfo.db_path, DBConInfo.db_username, DBConInfo.db_password);
			System.out.println( "Connected to SQL Server. Catalog: " + db.getCatalog() );
			
			Therma_DW = DriverManager.getConnection(DBConInfo.DW_path, DBConInfo.DW_username, DBConInfo.DW_password);
			System.out.println( "Connected to Datawarehouse Server. Catalog: " + Therma_DW.getCatalog() );
			}
		catch (Exception e)
			{
			if( !StatedConnectionFailed )
				{
				StatedConnectionFailed = true;
				String timestamp = TimeStamper.format(new Date());
				if( db == null )
					System.err.println(timestamp + " - Error Connecting to TwixDB. See stack trace for details.");
				if (Therma_DW == null )
					System.err.println(timestamp + " - Error Connecting to Therma Datawarehouse. See stack trace for details.");
				}
			db = null;
			Therma_DW = null;
			}
		}
	
	public void StopRunning()
		{
		KeepRunning = false;
		}
	
	public boolean DBConnectionsValid()
		{
		try
			{ return (db != null && db.isValid(10)) && (Therma_DW != null && Therma_DW.isValid(10)); }
		catch ( SQLException e )
			{ return false; }
		}
	}

class CleanupThread extends Thread
	{
	public static final long TIME_INTERVAL = 10000l;
	
	private Map<String, Connect.LoginContents> ActiveLogins;
	
	public CleanupThread(Map<String, Connect.LoginContents> AL)
		{
		ActiveLogins = AL;
		this.start();
		}
	
	public void run()
		{
		while(ActiveLogins != null)
			{
			try
				{
				synchronized (ActiveLogins)
					{
					Set<Entry<String, Connect.LoginContents>> logins = ActiveLogins.entrySet();
					Entry<String, Connect.LoginContents> entry;
					Connect.LoginContents loginContents;
					// Prevent Concurrent Modifications by building a list of ID's to remove
					ArrayList<String> removalEntries = new ArrayList<String>();
					
					for( Iterator<Entry<String, Connect.LoginContents>> i = logins.iterator(); i.hasNext(); )
						{
						entry = i.next();
						if( entry != null )
							{
							loginContents = entry.getValue();
							if( loginContents != null )
								{
								if( !loginContents.StillValid() )
									removalEntries.add(entry.getKey());
									//ActiveLogins.remove(entry.getKey());
								}
							}
						}
					
					int size = removalEntries.size();
					for( int i = 0; i < size; i++ )
						{
						ActiveLogins.remove(removalEntries.get(i));
						}
					}
				
				sleep(TIME_INTERVAL);
				}
			catch (ConcurrentModificationException ec )
				{
				System.err.println("ConcurrentModificationException: Attempted to read the ActiveLogins more than once.");
				ec.printStackTrace();
				}
			catch ( InterruptedException e )
				{
				return;
				}
			}
		}
	}

class Connect extends Thread
	{
	private SimpleDateFormat DateFormatter;
	private String empno;
	private String techEmail = null;
	private String SecretKey = null;
	
	private ServerResponse response;
	
	private Connection db;
	private Connection Therma_DW;
	private Prep_Statements prep_stmts;
	
	private Socket client = null;
	private ObjectInputStream ois = null;
	private ObjectOutputStream oos = null;
	
	private boolean ResultsReady = false;
	private int ResultsStatus;
	
	public Connect()
		{
		}
	
	public Connect(Socket clientSocket, Connection database, Connection datawarehouse,
			Map<String, LoginContents> AL) throws SQLException
		{
		db = database;
		Therma_DW = datawarehouse;
		
		client = clientSocket;
		ActiveLogins = AL;
		DateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		
		try
			{
			ois = new ObjectInputStream(client.getInputStream());
			oos = new ObjectOutputStream(client.getOutputStream());
			}
		catch (Exception e1)
			{
			try
				{ client.close(); }
			catch (Exception e)
				{ System.out.println(e.getMessage()); }
			return;
			}
		
		if( db == null || !db.isValid(10))
			{
			try
				{
				// Throw away the request, the SQL Server is down
				ois.readObject();
				
				System.err.println("SQL Connection has been lost. Returning result to client." );
				response = new ServerResponse(empno, ServerResponse.IOEXCEPTION, null);
				oos.writeObject(response);
				oos.flush();
				
				try{oos.flush();}catch(Exception e){}
				try{client.shutdownInput();}catch(Exception e){}
				try{client.shutdownOutput();}catch(Exception e){}
				try{ois.close();}catch(Exception e){}
				try{oos.close();}catch(Exception e){}
				try{client.close();}catch(Exception e){}
				}
			catch (Exception e)
				{
				e.printStackTrace();
				}
			throw new SQLException("Database Connection is not Valid");
			}
		else
			prep_stmts = new Prep_Statements(db);
		
		this.start();
		}
	
	public void run()
		{
		ProcessRequest();
		
		try{oos.flush();}catch(Exception e){}
		try{client.shutdownInput();}catch(Exception e){}
		try{client.shutdownOutput();}catch(Exception e){}
		try{ois.close();}catch(Exception e){}
		try{oos.close();}catch(Exception e){}
		try{client.close();}catch(Exception e){}
		}
	
	private String GenerateTimestamp()
		{
		String ret = "";
		try
			{
			ret = DateFormatter.format(new Date());
			}
		catch (Exception e)
			{
			e.printStackTrace();
			}
		
		return ret;
		}
	
	private boolean requireUpdate(String clientVersion)
		{
		return (clientVersion == null) || (Twix_Socket.VERSION.compareTo(clientVersion) != 0);
		}
	
	private String getDayOfMonthSuffix(final int n)
		{
		if (n < 1 || n > 31)
			{
			throw new IllegalArgumentException("illegal day of month: " + n);
			}
		if (n >= 11 && n <= 13)
			{
			return "th";
			}
		switch (n % 10)
			{
			case 1:
				return "st";
			case 2:
				return "nd";
			case 3:
				return "rd";
			default:
				return "th";
			}
		}
	
	// Tablet Object logging
	private void tabletLogData(String MEID, Object o) throws SQLException
		{
		// Set Tablet Data Statement
		String sql = "UPDATE tabletLog SET data = ?, " +
										"empno = ? " +
						"WHERE tabletMEID = ? " +
				
				"IF @@ROWCOUNT=0 " +
					"INSERT INTO tabletLog ( tabletMEID, empno, data ) " +
						" VALUES( ?, ?, ?)";
		PreparedStatement tablet_set_data = db.prepareStatement(sql);
		
		try
			{
			// The upload_package object
			if( o != null )
				{
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);
				oos.writeObject( o );
				byte[] bytes = baos.toByteArray();
				
				tablet_set_data.setBytes(1, bytes);
				tablet_set_data.setBytes(6, bytes);
				}
			else
				{
				tablet_set_data.setNull(1, Types.NULL);
				tablet_set_data.setNull(6, Types.NULL);
				}
			// Bind the MEID
			tablet_set_data.setString(3, MEID);
			tablet_set_data.setString(4, MEID);
			
			// Bind the EmpNo
			tablet_set_data.setString(2, empno);
			tablet_set_data.setString(5, empno);
			
			tablet_set_data.execute();
			}
		catch (Exception e)
			{
			printError(e);
			}
		
		}
	
	private ClientRequest fetchTabletLogData(String MEID) throws SQLException
		{
		ClientRequest request = null;
		
		// Fetch Tablet Data Statement
		String sql = "SELECT data FROM tabletLog " +
								"WHERE tabletMEID = ? ";
		PreparedStatement tablet_fetch_data = db.prepareStatement(sql);
		
		try
			{
			tablet_fetch_data.setString(1, MEID);
			
			ResultSet result = tablet_fetch_data.executeQuery();
			if( result.next() )
				{
				InputStream is = result.getBinaryStream(1);
				if( is != null )
					{
					ObjectInputStream ois = new ObjectInputStream(is);
				
					request = (ClientRequest) ois.readObject();
					}
				}
			if( result != null && !result.isClosed() )
				{
				result.close();
				}
			
			}
		catch (SQLException e)
			{
			printError(e);
			empno = null;
			}
		catch (IOException e)
			{
			printError(e);
			request = null;
			}
		catch (ClassNotFoundException e)
			{
			printError(e);
			request = null;
			}
		
		return request;
		}
	
	private void printError(Exception e)
		{
		System.err.print("ERROR: Setting tablet log data. Exception: " + e.getCause() + " Message: " + e.getMessage() );
		e.printStackTrace();
		}
	
	/********************************************************************************
	 ********* Site Search **********************************************************
	 ********************************************************************************/
	private ArrayList<ServerResponse.SearchData> siteSearch( SiteSearch search ) throws SQLException
		{
		ArrayList<ServerResponse.SearchData> results = new ArrayList<ServerResponse.SearchData>();
		if( !search.isEmpty() )
			{
			ServerResponse.SearchData data;
			String address, address2;
			String params = "";
			int size = search.knownServiceAddressIds.size();
			for( int i = 0; i < size; i++ )
				{
				params += "?";
				if( i < size-1 )
					params += ", ";
				}
			
			String sql = "SELECT TOP 20 serviceAddressid, siteName, address1, address2, " +
							"city, state, zip, buildingNo FROM serviceAddress " +
									"WHERE siteName LIKE '%" + search.siteName + "%' " +
										"AND (address1 LIKE '%" + search.address + "%' " +
										"OR address2 LIKE '%" + search.address + "%') " +
										"AND city LIKE '%" + search.city + "%' " +
										"AND buildingNo LIKE '%" + search.buildingNo + "%'";
			
			if( params.length() > 0 )
				sql += " AND serviceAddressId NOT IN (" + params + ")";
			
			PreparedStatement stmt = db.prepareStatement(sql);
			for( int i = 0; i < size; i++ )
				{
				stmt.setInt(i+1, search.knownServiceAddressIds.get(i));
				}
			
			
			ResultSet result = stmt.executeQuery();
			while( result.next() )
				{
				// Create the Service Address Data
				data = new ServerResponse.SearchData();
				data.serviceAddressId	= result.getInt(1);
				data.siteName			= result.getString(2);
				
				// Combine the Address
				address = result.getString(3);
				if( address == null )
					address = "";
				address2 = result.getString(4);
				if( (address2 != null) && (address.length() > 0) )
					address += ", " + address2;
				data.address			= address;
				
				// Rest of Data
				data.city				= result.getString(5);
				data.state				= result.getString(6);
				data.zip				= result.getString(7);
				data.buildingNo			= result.getString(8);
				
				results.add(data);
				}
			
			if( result != null && !result.isClosed() )
				result.close();
			}
		
		return results;
		}
	
	// Updated Server Logic
	private Map<String, LoginContents> ActiveLogins;
	
	/**
	 * Removes all previous logins with the same empno. This way, the employee can only have ONE valid login
	 * 	with a secret key.
	 * 
	 * WARNING: This method MUST be called inside of a synchronized block. This method does NOT block
	 * 				access by itself.
	 * @param empno - The employee number to remove.
	 */
	private void RemovePreviousEntries(String empno)
		{
		try
			{
			Set<Entry<String, Connect.LoginContents>> logins = ActiveLogins.entrySet();
			Entry<String, Connect.LoginContents> entry;
			Connect.LoginContents loginContents;
			// Prevent Concurrent Modifications by building a list of ID's to remove
			ArrayList<String> removalEntries = new ArrayList<String>();
			
			for( Iterator<Entry<String, Connect.LoginContents>> i = logins.iterator(); i.hasNext(); )
				{
				entry = i.next();
				if( entry != null )
					{
					loginContents = entry.getValue();
					if( loginContents != null )
						{
						if( loginContents.isEmpno(empno) )
							removalEntries.add(entry.getKey());
						}
					}
				}
			
			int size = removalEntries.size();
			for( int i = 0; i < size; i++ )
				{
				ActiveLogins.remove(removalEntries.get(i));
				}
			}
		catch (ConcurrentModificationException ec )
			{
			System.err.println("ConcurrentModificationException: Attempted to read the ActiveLogins more than once.");
			ec.printStackTrace();
			}
		}
	
	// Main Server Driving Function - Now built to handle multiple types of requests.
	private void ProcessRequest()
		{
		try
			{
			ClientRequest request = (ClientRequest) ois.readObject();
			if( request != null )
				System.out.println( GenerateTimestamp() + " Processing Client Data for '" + request.username + "'");
			
			int LoginStatus = ProcessLogin(request);
			boolean SuccessfulLogin = LoginStatus == 0;
			boolean RequireUpdate = requireUpdate(request.app_version);
			
			// Test user login and update requirements
			if( SuccessfulLogin && !RequireUpdate )
				{
				try
					{
					switch( request.action )
						{
						case ClientRequest.SYNC_UPLOAD:
						case ClientRequest.SYNC_DOWNLOAD:
							ProcessSync(request);
							break;
						case ClientRequest.SITE_SEARCH:
							ProcessSiteSearch(request);
							break;
						case ClientRequest.SITE_DOWNLOAD:
							ProcessSiteDownload(request);
							break;
						case ClientRequest.SYNC_TIME:
							ProcessSyncTime(request);
							break;
						case ClientRequest.ASSIGN_MECHANIC:
							ProcessDispatchTech( request, (DispatchRequest)request.Package);
							break;
						default:
							System.err.println("Error: Unknown Client Request: '" + request.action + "'" );
							response = new ServerResponse(empno, ServerResponse.IOEXCEPTION, null);
							response.email = techEmail;
							response.SecretKey = SecretKey;
							response.syncTime = GetSyncTime();
							oos.writeObject(response);
							oos.flush();
							break;
						}
					LogConnection(true, request.username);
					}
				catch (IOException e)
					{
					System.err.println("Client Connection Lost.");
					LogConnection(false, request.username);
					}
				catch (SQLException e)
					{
					if( request != null )
						System.err.println( GenerateTimestamp() + " ERROR: SQL Exception for '" + request.username + "'");
					response = new ServerResponse(empno, ServerResponse.TRANSACTION_FAILED, null);
					response.email = techEmail;
					response.SecretKey = SecretKey;
					response.syncTime = GetSyncTime();
					oos.writeObject(response);
					oos.flush();
					e.printStackTrace();
					}
				}
			else if( SuccessfulLogin && RequireUpdate && request.action == ClientRequest.DOWNLOAD_UPDATE )
				{
				if( request != null )
					System.out.println( GenerateTimestamp() + " Update Process for '" + request.username + "'");
				ProcessUpdate(request);
				}
			else
				{
				int result = ServerResponse.LOGIN_FAILED;
				if( SuccessfulLogin && RequireUpdate )
					{
					result = ServerResponse.REQ_UPDATE;
					if( request.pkg != null )
						tabletLogData( request.MEID, request );
					}
				else
					{
					if( request != null )
						System.out.println( GenerateTimestamp() + " Failed login attempt for '" + request.username + "'");
					}
				
				response = new ServerResponse(empno, result, null);
				if( LoginStatus == -1 )
					response.UserMessage = "The Twix Mobile Service is currently down for maintainence. Please try again later.";
				oos.writeObject(response);
				oos.flush();
				}
			}
		catch( Exception e )
			{
			e.printStackTrace();
			}
		
		// Streams are ended at the run() level
		}
	
	private void ProcessSync(ClientRequest request) throws SQLException, IOException, ClassNotFoundException
		{
		// Set the Logging Type
		ClientRequest requestDownload;
		if( request.action == ClientRequest.SYNC_UPLOAD )
			{
			if( request.username != null )
				System.out.println( GenerateTimestamp() + " Logging Data for '" + request.username + "'");
			
			// Stage the Data in the database for processing
			ServerResponse stageResponse = new ServerResponse(empno, ServerResponse.SUCCESS, null);
			LogDataThread logDataThread = new LogDataThread(this, db, request.MEID, request, empno);
			keepAliveLooper(oos, ois, stageResponse);
			
			// Send the Staging Data response
			stageResponse.email = techEmail;
			stageResponse.SecretKey = SecretKey;
			oos.writeObject(stageResponse);
			oos.flush();
			
			// Fetch the Second Request
			requestDownload = (ClientRequest) ois.readObject();
			}
		else
			{
			if( request != null )
				System.out.println( GenerateTimestamp() + " Handling Download-Only Request for '" + request.username + "'");
			requestDownload = request;
			}
		
		ServerResponse responseDownload = new ServerResponse(empno, ServerResponse.TRANSACTION_FAILED, null);
		ServerResultsThread resultThread = new ServerResultsThread(this, db, Therma_DW, empno, prep_stmts,
				requestDownload, responseDownload);
		// Blocks transmission of the actual response until the data is generated
		keepAliveLooper(oos, ois, responseDownload);
		responseDownload.result = ResultsStatus;
		
		responseDownload.email = techEmail;
		responseDownload.SecretKey = SecretKey;
		responseDownload.syncTime = GetSyncTime();
		oos.writeObject(responseDownload);
		oos.flush();
		}
	
	private String GetSyncTime()
		{
		Calendar c = Calendar.getInstance();
		SimpleDateFormat formatter = new SimpleDateFormat("MMMM d'" + getDayOfMonthSuffix(c.get(Calendar.DAY_OF_MONTH)) + "', yyyy h:mm:ss a");
		return formatter.format(c.getTime());
		}
	
	private void ProcessSiteSearch(ClientRequest request) throws SQLException, IOException
		{
		int result = ServerResponse.TRANSACTION_FAILED;
		ArrayList<ServerResponse.SearchData> searchResponse = null;
		try
			{
			searchResponse = siteSearch(request.search);
			result = ServerResponse.SUCCESS;
			}
		catch (Exception e)
			{
			result = ServerResponse.TRANSACTION_FAILED;
			}
		
		ServerResponse responseDownload = new ServerResponse(empno, result, null);
		responseDownload.searchResponse = searchResponse;
		
		responseDownload.email = techEmail;
		responseDownload.SecretKey = SecretKey;
		responseDownload.syncTime = GetSyncTime();
		oos.writeObject(responseDownload);
		oos.flush();
		}
	
	private void ProcessSiteDownload(ClientRequest request) throws SQLException, IOException
		{
		int result = ServerResponse.TRANSACTION_FAILED;
		Package_Download download = null;
		try
			{
			Twix_ServerResults results = new Twix_ServerResults(db, Therma_DW, empno, prep_stmts, request );
			results.buildResponse( request.siteRequest );
			download = results.pkg;
			result = ServerResponse.SUCCESS;
			}
		catch (Exception e)
			{
			result = ServerResponse.TRANSACTION_FAILED;
			}
		
		ServerResponse responseDownload = new ServerResponse(empno, result, null);
		responseDownload.pkg = download;
		responseDownload.email = techEmail;
		responseDownload.SecretKey = SecretKey;
		responseDownload.syncTime = GetSyncTime();
		oos.writeObject(responseDownload);
		oos.flush();
		}
	
	private void ProcessUpdate(ClientRequest request) throws IOException, SQLException
		{
		// 1. Fetch Data
		ClientRequest originalRequest = fetchTabletLogData(request.MEID);
		// 2. Process Data
		int process_results = ServerResponse.SUCCESS;
		Twix_ServerProcessor processorResults = null;
		if( originalRequest != null )
			{
			processorResults = new Twix_ServerProcessor(originalRequest, empno, db, prep_stmts);
			process_results = processorResults.process_result;
			
			if( process_results == ServerResponse.SUCCESS )
				tabletLogData(request.MEID, null);
			}
		
		ServerResponse responseDownload = new ServerResponse(empno, process_results, null);
		System.out.println( "Fetching Latest Twix_Agent APK.\n" );
		FileInputStream fis = null;
		
		try
			{
			File f = new File("Twix_Agent.apk");
			fis = new FileInputStream(f);
			responseDownload.UpdateFile = new byte[(int) f.length()];
			fis.read(responseDownload.UpdateFile);
			responseDownload.result = ServerResponse.SUCCESS;
			}
		catch(IOException eio)
			{
			System.out.println( "ERROR Reading Twix_Agent.apk. " +
					"Please make sure it is in the same directory at twix_server.\n" );
			eio.printStackTrace();
			responseDownload.result = ServerResponse.IOEXCEPTION;
			}
		finally
			{
			if( fis != null )
				fis.close();
			}
		
		responseDownload.email = techEmail;
		responseDownload.SecretKey = SecretKey;
		responseDownload.syncTime = GetSyncTime();
		oos.writeObject(responseDownload);
		oos.flush();
		}
	
	private void ProcessDispatchTech(ClientRequest clientRequest, DispatchRequest request) throws IOException, SQLException
		{
		int requestResult = ServerResponse.TRANSACTION_FAILED;
		int mechanicIndex = 0;
		boolean dispatchValid = true;
		String sql = "SELECT mechanic1, mechanic2, mechanic3, mechanic4, mechanic5, mechanic6, mechanic7 " +
				"FROM dispatch WHERE dispatchId = ?";
		try
			{
			db.setAutoCommit(false);
			PreparedStatement stmt = db.prepareStatement(sql);
			stmt.setInt(1, request.DispatchId);
			
			ResultSet result = stmt.executeQuery();
			if( result.next() )
				{
				String mech;
				for( int i = 0; i < 7; i++ )
					{
					mech = result.getString(i+1);
					if( (mech != null) && (request.Empno.contentEquals(mech)) )
						{
						dispatchValid = false;
						break;
						}
					}
				
				if( dispatchValid )
					{
					for( int i = 0; i < 7; i++ )
						{
						if( ColumnAvailable(result.getString(i+1)) )
							{
							mechanicIndex = i+1;
							break;
							}
						}
					}
				
				}
			if( result != null && !result.isClosed() )
				result.close();
			
			if( mechanicIndex > 2 )
				{
				sql = "UPDATE dispatch SET mechanic" + mechanicIndex + " = ? " +
						"WHERE dispatchId = ?";
				
				stmt = db.prepareStatement(sql);
				stmt.setString(1, request.Empno);
				stmt.setInt(2, request.DispatchId);
				stmt.execute();
				}
			
			// Commit Changes
			db.commit();
			
			// Set the Result Response to the Client
			if( mechanicIndex > 2 )
				requestResult = ServerResponse.SUCCESS;
			else if( !dispatchValid )
				requestResult = ServerResponse.DISPATCH_ALREADY_ASSIGNED;
			else
				requestResult = ServerResponse.DISPATCH_SLOT_NOT_AVAILABLE;
			
			}
		catch (Exception e)
			{
			requestResult = ServerResponse.TRANSACTION_FAILED;
			try {db.rollback();}
			catch ( SQLException e1 ) {e1.printStackTrace();}
			e.printStackTrace();
			}
		finally
			{
			try {db.setAutoCommit(true);}
			catch (SQLException e1) {e1.printStackTrace();}
			}
		
		
		// Build the Package Download response data
		Package_Download download = null;
		//if( requestResult == ServerResponse.SUCCESS )
			//{
			// Build the Resulting Dispatch
			Twix_ServerResults dataResults = new Twix_ServerResults(db, Therma_DW, empno, prep_stmts, clientRequest );
			dataResults.buildDispatchResponse(request.DispatchId);
			download = dataResults.pkg;
			//}
		
		
		// Respond to the client with the results of assigning the tech
		ServerResponse responseDownload = new ServerResponse(empno, requestResult, download);
		responseDownload.email = techEmail;
		responseDownload.SecretKey = SecretKey;
		responseDownload.syncTime = GetSyncTime();
		oos.writeObject(responseDownload);
		oos.flush();
		}
	
	private void ProcessSyncTime(ClientRequest request)
		{
		
		}
	
	private boolean ColumnAvailable(String s)
		{
		if( s != null )
			if( s.length() > 0 )
				return false;
		return true;
		}
	
	/**
	 * Processes the Login Information for the for the Client Request
	 * 
	 * @param request - The Client's Request
	 * @return Whether or not the login was successful. If the user was already logged in
	 * 	or the user name and password was correct, returns true; Otherwise, false;
	 */
	private int ProcessLogin(ClientRequest request)
		{
		int ret = 1;
		String sql = "SELECT available FROM app_groups WHERE app_group = 'TB'";
		try
			{
			PreparedStatement application_available = db.prepareStatement(sql);
			ResultSet available_result = application_available.executeQuery();
			if( available_result.next() )
				{
				String avail_flag = available_result.getString(1);
				if(avail_flag != null && avail_flag.contentEquals("Y"))
					ret = 0;
				else
					ret = -1;
				}
			else
				ret = -1;
			}
		catch ( SQLException e1 )
			{
			ret = -1;
			e1.printStackTrace();
			}
		
		// Fetch the LoginID Object
		LoginContents loginID = null;
		if( request.SecretKey != null )
			{
			synchronized (ActiveLogins)
				{
				loginID = ActiveLogins.get(request.SecretKey);
				}
			}
		
		// Test the LoginID Object or test the username and password
		if( loginID != null && loginID.StillValid() && loginID.username.contentEquals(request.username) )
			{
			empno = loginID.Empno;
			techEmail = loginID.Email;
			SecretKey = request.SecretKey;
			loginID.updateTimestamp();
			if( ret == 0 || loginID.isIT() )
				ret = 0;
			}
		else
			{
			// Login Statement
			sql = "SELECT empno, Email, misc_flags FROM users WHERE login_name = ? AND password = ? ";
			try
				{
				PreparedStatement login_stmt = db.prepareStatement(sql);
				
				login_stmt.setString(1, request.username);
				login_stmt.setString(2, request.password);
				
				ResultSet result = login_stmt.executeQuery();
				if( result.next() )
					{
					empno = result.getString(1);
					techEmail = result.getString(2);
					String misc_flags = result.getString(3);
					boolean isIT = misc_flags != null && misc_flags.length() == 3 && misc_flags.charAt(1) == 'Y';
					SecretKey = GenerateSecretKey();
					loginID = new LoginContents(request.username, empno, techEmail, isIT);
					synchronized (ActiveLogins)
						{
						RemovePreviousEntries(empno);
						ActiveLogins.put(SecretKey, loginID);
						}
					if( ret == 0 || isIT )
						ret = 0;
					}
				else
					ret = 1;
				if( result != null && !result.isClosed() )
					result.close();
				}
			catch (Exception e)
				{
				e.printStackTrace();
				empno = null;
				techEmail = null;
				SecretKey = null;
				ret = 1;
				}
			}
		
		return ret;
		}
	
	private String GenerateSecretKey()
		{
		Random rand = new Random();
		String ret = Integer.toHexString(rand.nextInt());
		int counter = 0;
		while( ActiveLogins.containsKey(ret))
			{
			counter++;
			ret = Integer.toHexString(rand.nextInt());
			}
		
		System.out.println( GenerateTimestamp() + " Generated Secret Key in " + counter + " iterations.");
		return ret;
		}
	
	public class LoginContents
		{
		private final long TIMEOUT_MS = 7200000;
		//private final long TIMEOUT_MS = 1800000l;
		//private final long TIMEOUT_MS = 10000l;
		private String Empno;
		private String username;
		private String Email;
		private long timestamp;
		private boolean isIT;
		
		public LoginContents(String usernameIn, String EmpnoIn, String EmailIn, boolean isIT)
			{
			username = usernameIn;
			Empno = EmpnoIn;
			Email = EmailIn;
			this.isIT = isIT;
			updateTimestamp();
			}
		
		public void updateTimestamp()
			{
			timestamp = System.currentTimeMillis();
			}
		
		public boolean StillValid()
			{
			return timestamp + TIMEOUT_MS > System.currentTimeMillis();
			}
		
		public boolean isEmpno(String empIn)
			{
			if( Empno == null || empIn == null )
				return false;
			return Empno.contentEquals(empIn);
			}
		
		public boolean isIT()
			{
			return isIT;
			}
		}
	
	public void SetReady(int Status)
		{
		ResultsStatus = Status;
		ResultsReady = true;
		}
	
	/**
	 * Blocks sending the true response until the ServerResponse results have been posted.
	 * 	Uses the ResultsReady boolean to mark that results are set and ready to transmit;
	 * 
	 * @param oos - Socket Object Output Stream
	 * @param ois - Socket Object Output Stream
	 * @throws OptionalDataException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private void keepAliveLooper(ObjectOutputStream oos, ObjectInputStream ois, Object block) throws OptionalDataException, ClassNotFoundException, IOException
		{
		long starttime;
		long totallatency = 0;
		int counter = 0;
		Object curResponse = null;
		while( !ResultsReady && (curResponse == null || curResponse instanceof KeepAlivePacket) )
			{
			// Send Packet
			starttime = System.currentTimeMillis();
			oos.writeObject( new KeepAlivePacket() );
			
			// Receive Packet
			curResponse = ois.readObject();
			if( curResponse instanceof KeepAlivePacket )
				{
				counter++;
				totallatency += (System.currentTimeMillis() - starttime);
				}
			}
		
		// Prevent multiple modifications of the object
		if( block != null )
			{
			synchronized(block)
				{
				ResultsReady = false;
				}
			}
		else
			ResultsReady = false;
		
		String latency;
		if( counter >= 0 )
			latency = "" +(double)((double)totallatency/((double)counter));
		else
			latency = "No Keep alive packets.";
		System.out.println("Keep Alive Iterations: " + counter + ". Average Latency(Including Serialization and Round Trip): " + latency + "ms");
		}
	
	private void LogConnection(boolean successful, String empno)
		{
		try
			{
			String sql = "INSERT INTO Z_ConnectionStatistics(username, Date, Successful) " +
								"VALUES(?, getDate(), ?)";
			PreparedStatement stat_stmt = db.prepareStatement(sql);
			if( empno != null )
				stat_stmt.setString(1, empno);
			else
				stat_stmt.setString(1, "Unknown");
			
			if( successful )
				stat_stmt.setString(2, "Y");
			else
				stat_stmt.setString(2, "N");
			stat_stmt.execute();
			}
		catch (Exception e)
			{
			e.printStackTrace();
			}
		}
	}

class LogDataThread extends Thread
	{
	private Connect parent;
	private Connection db;
	private String MEID;
	private String empno;
	private Object toLog;
	
	public LogDataThread(Connect parent, Connection db, String MEID, Object toLog, String empno)
		{
		this.parent = parent;
		this.db = db;
		this.MEID = MEID;
		this.empno = empno;
		this.toLog = toLog;
		
		this.start();
		}
	
	public void run()
		{
		int Status = ServerResponse.SUCCESS;
		try
			{
			tabletLogData();
			}
		catch ( SQLException e )
			{
			Status = ServerResponse.TRANSACTION_FAILED;
			e.printStackTrace();
			}
		
		if( parent != null )
			parent.SetReady(Status);
		}
	
	private void tabletLogData() throws SQLException
		{
		// Set Tablet Data Statement
		String sql = "UPDATE tabletLog SET data = ?, " +
										"empno = ? " +
						"WHERE tabletMEID = ? " +
				
				"IF @@ROWCOUNT=0 " +
					"INSERT INTO tabletLog ( tabletMEID, empno, data ) " +
						" VALUES( ?, ?, ? )";
		PreparedStatement tablet_set_data = db.prepareStatement(sql);
		
		try
			{
			// The upload_package object
			if( toLog != null )
				{
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);
				oos.writeObject( toLog );
				byte[] bytes = baos.toByteArray();
				
				tablet_set_data.setBytes(1, bytes);
				tablet_set_data.setBytes(6, bytes);
				}
			else
				{
				tablet_set_data.setNull(1, Types.NULL);
				tablet_set_data.setNull(6, Types.NULL);
				}
			// Bind the MEID
			tablet_set_data.setString(3, MEID);
			tablet_set_data.setString(4, MEID);
			
			// Bind the EmpNo
			tablet_set_data.setString(2, empno);
			tablet_set_data.setString(5, empno);
			
			tablet_set_data.execute();
			}
		catch (Exception e)
			{
			e.printStackTrace();
			}
		
		}
	}

class ServerResultsThread extends Thread
	{
	private Connect parent;
	
	private Connection db;
	private Connection Therma_DW;
	private String empno;
	private Prep_Statements prep_statements;
	private ClientRequest requestDownload;
	private ServerResponse response;
	
	public ServerResultsThread(Connect parent, Connection db, Connection Therma_DW, String empno,
			Prep_Statements prep_statements, ClientRequest requestDownload, ServerResponse response)
		{
		this.parent = parent;
		
		this.db = db;
		this.Therma_DW = Therma_DW;
		this.empno = empno;
		this.prep_statements = prep_statements;
		
		this.requestDownload = requestDownload;
		this.response = response;
		
		this.start();
		}
	
	public void run()
		{
		int Status = ServerResponse.SUCCESS;
		
		// 1. Fetch Data
		ClientRequest originalRequest = null;
		try
			{
			originalRequest = fetchTabletLogData(requestDownload.MEID);
			}
		catch ( SQLException e1 )
			{
			Status = ServerResponse.TRANSACTION_FAILED;
			e1.printStackTrace();
			
			if( parent != null )
				parent.SetReady(Status);
			return;
			}
		// 2. Process Data
		Twix_ServerProcessor processorResults = null;
		if( originalRequest != null )
			{
			if( originalRequest != null )
				System.out.println( GenerateTimestamp() + " Processing Logged Data for '" + originalRequest.username + "'");
			processorResults = new Twix_ServerProcessor(originalRequest, empno, db, prep_statements);
			Status = processorResults.process_result;
			}
		
		
		if( Status == ServerResponse.SUCCESS )
			{
			try
				{
				archiveData(requestDownload.MEID);
				}
			catch ( SQLException e1 )
				{
				Status = ServerResponse.TRANSACTION_FAILED;
				e1.printStackTrace();
				
				if( parent != null )
					parent.SetReady(Status);
				return;
				}
			
			if( originalRequest != null )
				{
				try
					{
					tabletLogData(originalRequest.MEID, null);
					}
				catch ( SQLException e1 )
					{
					Status = ServerResponse.TRANSACTION_FAILED;
					e1.printStackTrace();
					
					if( parent != null )
						parent.SetReady(Status);
					return;
					}
				}
			
			Package_Download download = null;
			FormPackage formPackage = null;
			FormDataPackage formDataPackage = null;
			AttributePackage attrPackage = null;
			Twix_ServerResults dataResults = new Twix_ServerResults(db, Therma_DW, empno, prep_statements, requestDownload );
			try
				{
				dataResults.buildResonse();
				}
			catch ( SQLException e )
				{
				Status = ServerResponse.TRANSACTION_FAILED;
				e.printStackTrace();
				if( parent != null )
					parent.SetReady(Status);
				return;
				}
			download = dataResults.pkg;
			formPackage = dataResults.FormPkg;
			formDataPackage = dataResults.FormDataPkg;
			attrPackage = dataResults.AttrPkg;
			if( processorResults != null )
				{
				download.serviceTagId_map = processorResults.serviceTagId_map;
				download.serviceTagUnitId_map = processorResults.serviceTagUnitId_map;
				}
			
			synchronized(response)
				{
				response.pkg = download;
				response.FormPackage = formPackage;
				response.FormDataPackage = formDataPackage;
				response.AttrPackage = attrPackage;
				}
			}
		
		if( parent != null )
			parent.SetReady(Status);
		}
	
	private ClientRequest fetchTabletLogData(String MEID) throws SQLException
		{
		ClientRequest request = null;
		
		// Fetch Tablet Data Statement
		String sql = "SELECT data FROM tabletLog " +
								"WHERE tabletMEID = ? ";
		PreparedStatement tablet_fetch_data = db.prepareStatement(sql);
		
		try
			{
			tablet_fetch_data.setString(1, MEID);
			
			ResultSet result = tablet_fetch_data.executeQuery();
			if( result.next() )
				{
				InputStream is = result.getBinaryStream(1);
				if( is != null )
					{
					ObjectInputStream ois = new ObjectInputStream(is);
				
					request = (ClientRequest) ois.readObject();
					}
				}
			if( result != null && !result.isClosed() )
				{
				result.close();
				}
			
			}
		catch (SQLException e)
			{
			e.printStackTrace();
			empno = null;
			}
		catch (IOException e)
			{
			e.printStackTrace();
			request = null;
			}
		catch (ClassNotFoundException e)
			{
			e.printStackTrace();
			request = null;
			}
		
		return request;
		}
	
	private String GenerateTimestamp()
		{
		SimpleDateFormat DateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		String ret = "";
		try
			{
			ret = DateFormatter.format(new Date());
			}
		catch (Exception e)
			{
			e.printStackTrace();
			}
		
		return ret;
		}
	
	private void archiveData(String MEID) throws SQLException
		{
		String sql = "INSERT INTO tabletLog_archive " +
						"SELECT tabletMEID, empno, getDate() as archiveDate, data FROM tabletLog " +
					"WHERE tabletMEID = ? AND data IS NOT NULL";
		PreparedStatement archive_data = db.prepareStatement(sql);
		archive_data.setString(1, MEID);
		
		archive_data.execute();
		
		tabletLogData(MEID, null);
		}
	
	private void tabletLogData(String MEID, Object o) throws SQLException
		{
		// Set Tablet Data Statement
		String sql = "UPDATE tabletLog SET data = ?, " +
										"empno = ? " +
						"WHERE tabletMEID = ? " +
				
				"IF @@ROWCOUNT=0 " +
					"INSERT INTO tabletLog ( tabletMEID, empno, data ) " +
						" VALUES( ?, ?, ?)";
		PreparedStatement tablet_set_data = db.prepareStatement(sql);
		
		try
			{
			// The upload_package object
			if( o != null )
				{
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);
				oos.writeObject( o );
				byte[] bytes = baos.toByteArray();
				
				tablet_set_data.setBytes(1, bytes);
				tablet_set_data.setBytes(6, bytes);
				}
			else
				{
				tablet_set_data.setNull(1, Types.NULL);
				tablet_set_data.setNull(6, Types.NULL);
				}
			// Bind the MEID
			tablet_set_data.setString(3, MEID);
			tablet_set_data.setString(4, MEID);
			
			// Bind the EmpNo
			tablet_set_data.setString(2, empno);
			tablet_set_data.setString(5, empno);
			
			tablet_set_data.execute();
			}
		catch (Exception e)
			{
			e.printStackTrace();
			}
		
		}
	}

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.twix.*;
import com.twix.FormDataPackage.FormData;

public class Twix_ServerProcessor
	{
	public int process_result;
	
	private ClientRequest request;
	private Connection db;
	private String empno;
	private Prep_Statements prep_stmts;
	
	private static final String TIMECARD_TERMINATED = "Attempted to insert a timecard for a terminated employee.";
	private static final String TIMECARD_PAY_PERIOD = "Timecard pay period not found or multiple pay periods match the given date.";
	private static final String TIMECARD_APPROVED_COMPLETE = "Timecard pay period was already complete or approved.";
	private static final String TIMECARD_MULTI_HEADERS = "Timecard pay period has multiple headers.";
	private static final String TIMECARD_INVALID_DATE = "Timecard labor date is in an invalid format.";
	
	private List<TimeCard> tc_list;
	
	private Set<DispatchInfo> dispatchIds;
	private Set<ServiceAddressInfo> serviceAddressIds;
	private Set<Integer> serviceTagIdList;
	//private Set<Integer> serviceAddressIds;
	private Set<Integer> equipmentIds;
	private Set<Integer> equipmentCategoryIds;
	
	private Set<Integer> serviceAddressContactIds;
	private Set<Integer> safetyChecklistIds;
	
	private Set<Integer> serviceTagIds;
	
	// Used for the Xref tables             
	private Set<Integer> newClosedTags;
	
	// New ID Resolutions
	private List<Pair> new_equipmentIds;
	private List<Pair> new_serviceAddressIds;
	private List<Pair> new_equipmentCategoryIds;
	
	public  class DispatchInfo implements Comparable<DispatchInfo>
		{
		int dispatchId;
		int serviceAddressId;
		String batchNo;
		String jobNo;
		String serviceType;
		
		String siteName;
		String tenant;
		
		@Override
		public boolean equals(Object info)
			{
			if( info.getClass() == DispatchInfo.class )
				return ( ((DispatchInfo)info).dispatchId == this.dispatchId );
			else if( info.getClass() == Integer.class )
				return ( ((Integer)info) == this.dispatchId );
			else
				// FUUUUUU
				return false;
			}
		

		@Override
		public int compareTo(DispatchInfo arg0)
			{
			if( arg0.dispatchId == this.dispatchId )
				return 0;
			else if( arg0.dispatchId < this.dispatchId)
				return 1;
			else
				return -1;
			}
			
		}
	
	public class ServiceAddressInfo implements Comparable<ServiceAddressInfo>
		{
		int serviceAddressId;
		String siteName;
		
		@Override
		public boolean equals(Object info)
			{
			if( info.getClass() == ServiceAddressInfo.class )
				return ( ((ServiceAddressInfo)info).serviceAddressId == this.serviceAddressId );
			else if( info.getClass() == Integer.class )
				return ( ((Integer)info) == this.serviceAddressId );
			else
				// FUUUUUU
				return false;
			}
		
		@Override
		public int compareTo(ServiceAddressInfo arg0)
			{
			if( arg0.serviceAddressId == this.serviceAddressId )
				return 0;
			else if( arg0.serviceAddressId < this.serviceAddressId)
				return 1;
			else
				return -1;
			}
		
		}
	
	// ServiceTag Map and Service Tag Unit Map for resolving ids on photos and receipts
	public List<Pair> serviceTagId_map;
	public List<Pair> serviceTagUnitId_map;
	public Map<Integer, Integer> ServiceTagId_Translation;
	public Map<Integer, Integer> ServiceTagUnitId_Translation;
	
	private Date start_time;
	private Date end_time;
	
	// Identity Resolvers
	public Map<Long, Long> Resolved_FormDataIds;
	
	// Sync Processor Constructor
	Twix_ServerProcessor( ClientRequest originalRequest, String employee, Connection database, Prep_Statements stmts )
		{
		process_result = ServerResponse.TRANSACTION_FAILED;
		start_time = new Date();
		
		request = originalRequest;
		empno = employee;
		db = database;
		prep_stmts = stmts;
		
		if( request.pkg != null )
			{
			tc_list = new ArrayList<TimeCard>();
			new_equipmentIds = new ArrayList<Pair>();
			new_serviceAddressIds = new ArrayList<Pair>();
			new_equipmentCategoryIds = new ArrayList<Pair>();
			
			serviceTagId_map = new ArrayList<Pair>();
			serviceTagUnitId_map = new ArrayList<Pair>();
			
			ServiceTagId_Translation = new HashMap<Integer,Integer>();
			ServiceTagUnitId_Translation = new HashMap<Integer,Integer>();
			
			try
				{
				db.setAutoCommit(false);
				
				setupValidateLists();
				
				// Remember to close each statement
				System.out.println("Starting to process equipment.");
				processEquipment();
				
				System.out.println("Starting to process Contacts.");
				processContacts();
				
				System.out.println("Starting to process Open Tags.");
				processOpenTags();
				
				System.out.println("Starting to process Tag Groups.");
				processGroups();
				
				System.out.println("Processing Timecard Entries.");
				insertTimeCard();
				
				System.out.println("Processing Form Data.");
				ProcessFormData();
				
				InsertResolvedIdMap("Resolved_OpenServiceTag", ServiceTagId_Translation);
				InsertResolvedIdMap("Resolved_OpenServiceTagUnit", ServiceTagUnitId_Translation);
				
				db.commit();
				process_result = ServerResponse.SUCCESS;
				}
			catch (Exception e)
				{
				try
					{
					String timestamp = "";
					try
						{
						SimpleDateFormat fmt = new SimpleDateFormat("MM/dd/yyyy kk:mm a");
						timestamp = fmt.format(start_time);
						}
					catch (Exception err)
						{
						timestamp = "Bad Timestamp";
						}
					process_result = ServerResponse.TRANSACTION_FAILED;
					db.rollback();
					System.out.println("ERROR: Sync Transaction Failed. Employee: '" + empno + "'");
					System.err.println(timestamp + "-- ERROR: Sync Transaction Failed. Employee: '" + empno + "'");
					e.printStackTrace();
					insertSyncTransactionFail(e);
					}
				catch (SQLException e1)
					{
					e1.printStackTrace();
					}
				}
			finally
				{
				try
					{
					db.setAutoCommit(true);
					}
				catch (SQLException e1)
					{
					e1.printStackTrace();
					}
				}
			
			}
		end_time = new Date();
		long difference = end_time.getTime() - start_time.getTime();
		int seconds = (int) (difference / 1000) % 60 ;
		System.out.println("Processing time: Sec: " + seconds + " Ms: " + (difference - ((difference / 1000)) ) );
		}
	
	private void setupValidateLists() throws SQLException
		{
		Package_Upload pkg = request.pkg;
		int size; int size2;
		int cur;
		
		// Create the sets of Ids. These lists will contain all the IDs from the upload data that needs to be verified
		Set<Integer> dispatchIdList = new TreeSet<Integer>();
		Set<Integer> serviceAddressIdList = new TreeSet<Integer>();
		Set<Integer> equipmentIdList = new TreeSet<Integer>();
		Set<Integer> equipmentCategoryIdList = new TreeSet<Integer>();
		Set<Integer> contactIdList = new TreeSet<Integer>();
		serviceTagIdList = new TreeSet<Integer>();
		
		OpenServiceTag tag;
		Equipment eq;
		ServiceAddressContact contact;
		
		// Fetch all the Ids in Open Tags
		size = pkg.openTags.size();
		for( int i = 0; i < size; i++ )
			{
			tag = pkg.openTags.get(i);
			cur = tag.dispatchId;		if( cur > 0 ) dispatchIdList.add( cur );
			cur = tag.serviceAddressId;	if( cur > 0 ) serviceAddressIdList.add( cur );
			cur = tag.serviceTagId;		if( cur > 0 ) serviceTagIdList.add( tag.serviceTagId );
			
			size2 = tag.units.size();
			for( int j = 0; j < size2; j++ )
				{
				cur = tag.units.get(j).equipmentId;
				if( cur > 0 )
					equipmentIdList.add( cur );
				}
			
			if( tag.blue != null )
				{
				size2 = tag.blue.units.size();
				for( int j = 0; j < size2; j++ )
					{
					cur = tag.blue.units.get(j).equipmentId;
					if( cur > 0 )
						equipmentIdList.add( cur );
					}
				}
			}
		
		// Fetch all the Ids in Equipment
		size = pkg.equipment.size();
		for( int i = 0; i < size; i++ )
			{
			eq = pkg.equipment.get(i);
			cur = eq.equipmentId;
			if( cur > 0 )
				equipmentIdList.add(cur);
			
			equipmentCategoryIdList.add(eq.equipmentCategoryId);
			
			serviceAddressIdList.add(eq.serviceAddressId);
			}
		
		// Fetch all the Ids in Service Address Contacts
		size = pkg.contact.size();
		for( int i = 0; i < size; i++ )
			{
			contact = pkg.contact.get(i);
			cur = contact.contactId;
			
			if( cur > 0 )
				contactIdList.add(cur);
			
			serviceAddressIdList.add(contact.serviceAddressId);
			}
		
		
		// Fetch all the verified lists
		dispatchIds 				= fetchDispatchInfo( dispatchIdList );
		serviceAddressIds 			= fetchServiceAddressInfo( serviceAddressIdList );
		//serviceAddressIds 			= fetchAllIds("serviceAddress", "serviceAddressId", serviceAddressIdList);
		equipmentIds 				= fetchAllIds("equipment", "equipmentId", equipmentIdList );
		equipmentCategoryIds 		= fetchAllIds("equipmentCategory", "equipmentCategoryId", equipmentCategoryIdList);
		
		serviceAddressContactIds 	= fetchAllIds("serviceAddressContact", "contactId", contactIdList);
		safetyChecklistIds 			= fetchAllIds("safetyChecklist", "safetyChecklistId", null);
		
		serviceTagIds 				= fetchAllIds("serviceTag", "serviceTagId", serviceTagIdList);
		
		// Used for the Xref tables
		newClosedTags 				= new TreeSet<Integer>();
		}
	
	private Set<Integer> fetchAllIds(String tableName, String Id, Set<Integer> idList) throws SQLException
		{
		Set<Integer> set = new TreeSet<Integer>();
		
		if( idList != null && idList.size() <= 0 )
			return set;
		
		String sql = "SELECT " + Id + " FROM " + tableName;
		if( idList != null )
			sql += " WHERE " + Id + " IN (" + buildParams(idList.size()) + ")";
		
		PreparedStatement stmt = db.prepareStatement(sql);
		if( idList != null )
			{
			int index = 1;
			for( Iterator<Integer> i = idList.iterator(); i.hasNext(); )
				stmt.setInt(index++, i.next() );
			}
		ResultSet result = stmt.executeQuery();
		if( result.next() )
			{
			do
				{
				set.add( result.getInt(1) );
				}
			while( result.next() );
			}
		if( result != null && !result.isClosed() )
			{
			result.close();
			}
		
		stmt.close();
		
		return set;
		}
	
	private Set<DispatchInfo> fetchDispatchInfo( Set<Integer> idList ) throws SQLException
		{
		Set<DispatchInfo> ret = new TreeSet<DispatchInfo>();
		
		if( idList.size() <= 0 )
			return ret;
		
		DispatchInfo info;
		String sql = "SELECT d.dispatchId, d.serviceAddressId, d.batchNo, d.jobNo, d.contractType, " +
				"(SELECT sa.siteName FROM serviceAddress as sa WHERE sa.serviceAddressId = d.serviceAddressId), " +
				"d.tenant " +
				"FROM dispatch as d " +
					"WHERE d.dispatchId IN ( " + buildParams(idList.size()) + " )";
		PreparedStatement stmt = db.prepareStatement(sql);
		int index = 1;
		for( Iterator<Integer> i = idList.iterator(); i.hasNext(); )
			stmt.setInt(index++, i.next() );
		
		ResultSet result = stmt.executeQuery();
		while( result.next() )
			{
			index = 1;
			info = new DispatchInfo();
			info.dispatchId			= result.getInt(index++);
			info.serviceAddressId	= result.getInt(index++);
			info.batchNo			= result.getString(index++);
			info.jobNo				= result.getString(index++);
			info.serviceType		= result.getString(index++);
			info.siteName			= result.getString(index++);
			info.tenant				= result.getString(index++);
			
			ret.add(info);
			}
		
		return ret;
		}
	
	private Set<ServiceAddressInfo> fetchServiceAddressInfo( Set<Integer> idList ) throws SQLException
		{
		Set<ServiceAddressInfo> ret = new TreeSet<ServiceAddressInfo>();
		
		if( idList.size() <= 0 )
			return ret;
		
		ServiceAddressInfo info;
		String sql = "SELECT sa.serviceAddressId, sa.siteName " +
				"FROM serviceAddress as sa " +
					"WHERE sa.serviceAddressId IN ( " + buildParams(idList.size()) + " )";
		PreparedStatement stmt = db.prepareStatement(sql);
		int index = 1;
		for( Iterator<Integer> i = idList.iterator(); i.hasNext(); )
			stmt.setInt(index++, i.next() );
		
		ResultSet result = stmt.executeQuery();
		while( result.next() )
			{
			index = 1;
			info = new ServiceAddressInfo();
			info.serviceAddressId	= result.getInt(index++);
			info.siteName			= result.getString(index++);
			
			ret.add(info);
			}
		
		return ret;
		}
	
	// Equipment Processing Functions
	private void processEquipment() throws SQLException
		{
		int size = request.pkg.equipment.size();
		Equipment eq;
		
		for( int i = 0; i < size; i++ )
			{
			eq = request.pkg.equipment.get(i);
			
			// Validate the Equipment Category Id
			if( !equipmentCategoryIds.contains(eq.equipmentCategoryId) )
				{
				Pair p = new Pair();
				p.id = eq.equipmentCategoryId;
				eq.equipmentCategoryId = insertUnknown("equipmentCategory", "categoryDesc");
				p.newid = eq.equipmentCategoryId;
				new_equipmentCategoryIds.add(p);
				}
			
			boolean found = false;
			// Test the ServiceAddressId
			if( eq.serviceAddressId > 0 )
				{
				for( Iterator<ServiceAddressInfo> it = serviceAddressIds.iterator(); it.hasNext(); )
					{
					if( it.next().serviceAddressId == eq.serviceAddressId )
						{
						found = true;
						break;
						}
					}
				}
			if( !found )
				{
				Pair p = new Pair();
				p.id = eq.serviceAddressId;
				eq.serviceAddressId = insertUnknown("serviceAddress", "siteName");
				p.newid = eq.serviceAddressId;
				new_serviceAddressIds.add(p);
				}
			
			// Update
			if( eq.equipmentId > 0 && equipmentIds.contains( eq.equipmentId ) )
				{
				updateEquipment( eq, prep_stmts.equipment_update );
				}
			else // Insert
				{
				insertNewEquipment(eq, prep_stmts.equipment_insert);
				equipmentIds.add(eq.equipmentId);
				}
			
			}
		
		}
	
	private void deleteEquipmentDetail(int equipmentId) throws SQLException
		{
		// Delete Belts
		prep_stmts.belt_drop.setInt(1, equipmentId);
		prep_stmts.belt_drop.execute();
		
		// Delete Sheaves
		prep_stmts.sheave_drop.setInt(1, equipmentId);
		prep_stmts.sheave_drop.execute();
		
		// Delete Fans
		prep_stmts.fan_drop.setInt(1, equipmentId);
		prep_stmts.fan_drop.execute();
		
		// Delete Filters
		prep_stmts.filter_drop.setInt(1, equipmentId);
		prep_stmts.filter_drop.execute();
		
		// Delete Compressors
		prep_stmts.compressor_drop.setInt(1, equipmentId);
		prep_stmts.compressor_drop.execute();
		
		// Delete RefCircuits
		prep_stmts.refcircuit_drop.setInt(1, equipmentId);
		prep_stmts.refcircuit_drop.execute();
		}
	
	private void updateEquipment( Equipment eq, PreparedStatement stmt) throws SQLException
		{
		deleteEquipmentDetail(eq.equipmentId);
		validateEquipment(eq);
		
		int index = 1;
		stmt.setInt( index++, eq.equipmentCategoryId);
		stmt.setInt( index++, eq.serviceAddressId);
		stmt.setString( index++, eq.unitNo);
		stmt.setString( index++, eq.barCodeNo);
		stmt.setString( index++, eq.manufacturer);
		stmt.setString( index++, eq.model);
		stmt.setString( index++, eq.productIdentifier);
		stmt.setString( index++, eq.serialNo);
		stmt.setString( index++, eq.voltage);
		stmt.setString( index++, eq.economizer);
		stmt.setFloat( index++, eq.capacity);
		stmt.setString( index++, eq.capacityUnits);
		stmt.setString( index++, eq.refrigerantType);
		stmt.setString( index++, eq.areaServed);
		stmt.setString( index++, eq.mfgYear);
		stmt.setString( index++, eq.dateInService);
		stmt.setString( index++, eq.dateOutService);
		stmt.setString( index++, eq.notes);
		stmt.setString( index++, eq.verifiedByEmpno);
		
		stmt.setInt( index++, eq.equipmentId);
		stmt.execute();
		
		insertEquipmentDetail(eq.equipmentId, eq.fans, eq.filters, eq.refCircuits);
		}
	
	private void validateEquipment(Equipment eq)
		{
		// Validate the data size and prevent truncation errors by SQL Server
		if( (eq.unitNo != null) && (eq.unitNo.length() > 20) )
			eq.unitNo = eq.unitNo.substring(0, 20);
		
		if( (eq.barCodeNo != null) && (eq.barCodeNo.length() > 50) )
			eq.barCodeNo = eq.barCodeNo.substring(0, 50);
		
		if( (eq.manufacturer != null) && (eq.manufacturer.length() > 50) )
			eq.manufacturer = eq.manufacturer.substring(0, 50);
		
		if( (eq.model != null) && (eq.model.length() > 50) )
			eq.model = eq.model.substring(0, 50);
		
		if( (eq.productIdentifier != null) && (eq.productIdentifier.length() > 50) )
			eq.productIdentifier = eq.productIdentifier.substring(0, 50);
		
		if( (eq.serialNo != null) && (eq.serialNo.length() > 50) )
			eq.serialNo = eq.serialNo.substring(0, 50);
		
		if( (eq.voltage != null) && (eq.voltage.length() > 15) )
			eq.voltage = eq.voltage.substring(0, 15);
		
		if( (eq.economizer != null) && (eq.economizer.length() > 1) )
			eq.economizer = eq.economizer.substring(0, 1);
		
		if( (eq.capacityUnits != null) && (eq.capacityUnits.length() > 10) )
			eq.capacityUnits = eq.capacityUnits.substring(0, 10);
		
		if( (eq.refrigerantType != null) && (eq.refrigerantType.length() > 50) )
			eq.refrigerantType = eq.refrigerantType.substring(0, 50);
		
		if( (eq.areaServed != null) && (eq.areaServed.length() > 50) )
			eq.areaServed = eq.areaServed.substring(0, 50);
		
		if( (eq.mfgYear != null) && (eq.mfgYear.length() > 4) )
			eq.mfgYear = eq.mfgYear.substring(0, 4);
		
		if( (eq.notes != null) && (eq.notes.length() > 5000) )
			eq.notes = eq.notes.substring(0, 5000);
		
		if( (eq.capacity > 9999999.99f) )
			eq.capacity = 9999999.99f;
		}
	
	private void insertNewEquipment(Equipment eq, PreparedStatement stmt) throws SQLException
		{
		int index = 1;
		Pair p = new Pair();
		p.id = eq.equipmentId;
		validateEquipment(eq);
		
		stmt.setInt( index++, eq.equipmentCategoryId);
		stmt.setInt( index++, eq.serviceAddressId);
		stmt.setString( index++, eq.unitNo);
		stmt.setString( index++, eq.barCodeNo);
		stmt.setString( index++, eq.manufacturer);
		stmt.setString( index++, eq.model);
		stmt.setString( index++, eq.productIdentifier);
		stmt.setString( index++, eq.serialNo);
		stmt.setString( index++, eq.voltage);
		stmt.setString( index++, eq.economizer);
		stmt.setFloat( index++, eq.capacity);
		stmt.setString( index++, eq.capacityUnits);
		stmt.setString( index++, eq.refrigerantType);
		stmt.setString( index++, eq.areaServed);
		stmt.setString( index++, eq.mfgYear);
		stmt.setString( index++, eq.dateInService);
		stmt.setString( index++, eq.dateOutService);
		stmt.setString( index++, eq.notes);
		stmt.setString( index++, eq.verifiedByEmpno);
		
		stmt.execute();
		ResultSet result = stmt.getGeneratedKeys();
		
		if( result.next() )
			{
			eq.equipmentId = result.getInt(1);
			p.newid = eq.equipmentId;
			new_equipmentIds.add(p);
			}
		if( result != null && !result.isClosed() )
			result.close();
		
		insertEquipmentDetail(eq.equipmentId, eq.fans, eq.filters, eq.refCircuits);
		}
	
	private void insertEquipmentDetail(	int equipmentId,
										List<Fan> fanList,
										List<Filter> filterList,
										List<RefCircuit> refCircuitList ) throws SQLException
		{
		int size, size2, id;
		ResultSet result;
		Fan fan;
			Sheave sheave;
		Filter filter;
		RefCircuit ref;
			Compressor comp;
		
		// Insert the Fans
		size = fanList.size();
		for( int i = 0; i < size; i++)
			{
			fan = fanList.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (fan.number != null) && (fan.number.length() > 2) )
				fan.number = fan.number.substring(0, 2);
			if( (fan.partType != null) && (fan.partType.length() > 20) )
				fan.partType = fan.partType.substring(0, 20);
			
			prep_stmts.fan_insert.setInt(1, equipmentId);
			prep_stmts.fan_insert.setString(2, fan.number);
			prep_stmts.fan_insert.setString(3, fan.partType);
			prep_stmts.fan_insert.execute();
			result = prep_stmts.fan_insert.getGeneratedKeys();
			
			if( result.next() )
				{
				id = result.getInt(1);
				prep_stmts.belt_insert.setInt(1, id);
				prep_stmts.sheave_insert.setInt(1, id);
				}
			if( result != null && !result.isClosed() )
				result.close();
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (fan.beltSize != null) && (fan.beltSize.length() > 50) )
				fan.beltSize = fan.beltSize.substring(0, 50);
			prep_stmts.belt_insert.setString(2, fan.beltSize);
			prep_stmts.belt_insert.setInt(3, fan.beltQty);
			prep_stmts.belt_insert.execute();
			
			size2 = fan.sheaves.size();
			for( int j = 0; j < size2; j++ )
				{
				sheave = fan.sheaves.get(j);
				
				// Validate the data size and prevent truncation errors by SQL Server
				if( (sheave.type != null) && (sheave.type.length() > 20) )
					sheave.type = sheave.type.substring(0, 20);
				if( (sheave.number != null) && (sheave.number.length() > 10) )
					sheave.number = sheave.number.substring(0, 10);
				if( (sheave.manufacturer != null) && (sheave.manufacturer.length() > 50) )
					sheave.manufacturer = sheave.manufacturer.substring(0, 50);
				prep_stmts.sheave_insert.setString(2, sheave.type);
				prep_stmts.sheave_insert.setString(3, sheave.number);
				prep_stmts.sheave_insert.setString(4, sheave.manufacturer);
				prep_stmts.sheave_insert.execute();
				}
			}
		
		// Insert Filters
		size = filterList.size();
		for( int i = 0; i < size; i++ )
			{
			filter = filterList.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (filter.type != null) && (filter.type.length() > 50) )
				filter.type = filter.type.substring(0, 50);
			if( (filter.filterSize != null) && (filter.filterSize.length() > 20) )
				filter.filterSize = filter.filterSize.substring(0, 20);
			
			prep_stmts.filter_insert.setInt(1, equipmentId);
			prep_stmts.filter_insert.setString(2, filter.type);
			prep_stmts.filter_insert.setInt(3, filter.quantity);
			prep_stmts.filter_insert.setString(4, filter.filterSize);
			prep_stmts.filter_insert.execute();
			}
		
		// Insert the Refrigeration Circuits
		size = refCircuitList.size();
		for( int i = 0; i < size; i++ )
			{
			ref = refCircuitList.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (ref.circuitNo != null) && (ref.circuitNo.length() > 4) )
				ref.circuitNo = ref.circuitNo.substring(0, 4);
			
			prep_stmts.refcircuit_insert.setInt(1, equipmentId);
			prep_stmts.refcircuit_insert.setString(2, ref.circuitNo);
			prep_stmts.refcircuit_insert.setFloat(3, ref.lbsRefrigerant);
			prep_stmts.refcircuit_insert.execute();
			
			result = prep_stmts.refcircuit_insert.getGeneratedKeys();
			if( result.next() )
				{
				id = result.getInt(1);
				prep_stmts.compressor_insert.setInt(1, id);
				}
			if( result != null && !result.isClosed() )
				result.close();
			
			size2 = ref.compressors.size();
			for( int j = 0; j < size2; j++ )
				{
				comp = ref.compressors.get(j);
				
				// Validate the data size and prevent truncation errors by SQL Server
				if( (comp.compressorNo != null) && (comp.compressorNo.length() > 4) )
					comp.compressorNo = comp.compressorNo.substring(0, 4);
				if( (comp.manufacturer != null) && (comp.manufacturer.length() > 50) )
					comp.manufacturer = comp.manufacturer.substring(0, 50);
				if( (comp.model != null) && (comp.model.length() > 50) )
					comp.model = comp.model.substring(0, 50);
				if( (comp.serialNo != null) && (comp.serialNo.length() > 50) )
					comp.serialNo = comp.serialNo.substring(0, 50);
				
				prep_stmts.compressor_insert.setString(2, comp.compressorNo );
				prep_stmts.compressor_insert.setString(3, comp.manufacturer);
				prep_stmts.compressor_insert.setString(4, comp.model);
				prep_stmts.compressor_insert.setString(5, comp.serialNo);
				prep_stmts.compressor_insert.setString(6, comp.dateInService);
				prep_stmts.compressor_insert.setString(7, comp.dateOutService);
				prep_stmts.compressor_insert.execute();
				}
			}
		
		}
	
	// Service Address Contact processing functions
	private void processContacts() throws SQLException
		{
		int size = request.pkg.contact.size();
		ServiceAddressContact contact;
		
		for( int i = 0; i < size; i++ )
			{
			contact = request.pkg.contact.get(i);
			
			boolean found = false;
			// Test the ServiceAddressId
			if( contact.serviceAddressId > 0 )
				{
				for( Iterator<ServiceAddressInfo> it = serviceAddressIds.iterator(); it.hasNext(); )
					{
					if( it.next().serviceAddressId == contact.serviceAddressId )
						{
						found = true;
						break;
						}
					}
				}
			if( !found )
				{
				Pair p = new Pair();
				p.id = contact.serviceAddressId;
				contact.serviceAddressId = insertUnknown("serviceAddress", "siteName");
				p.newid = contact.serviceAddressId;
				new_serviceAddressIds.add(p);
				}
			
			validateContact(contact);
			if( (contact.contactId > 0) && (serviceAddressContactIds.contains(contact.contactId)) )
				{
				int index = 1;
				prep_stmts.contact_update.setInt( index++, contact.serviceAddressId);
				prep_stmts.contact_update.setString( index++, contact.contactName);
				prep_stmts.contact_update.setString( index++, contact.phone1);
				prep_stmts.contact_update.setString( index++, contact.phone1Type);
				prep_stmts.contact_update.setString( index++, contact.phone2);
				prep_stmts.contact_update.setString( index++, contact.phone2Type);
				prep_stmts.contact_update.setString( index++, contact.email);
				prep_stmts.contact_update.setString( index++, contact.contactType);
				prep_stmts.contact_update.setString( index++, contact.ext1);
				prep_stmts.contact_update.setString( index++, contact.ext2);
				prep_stmts.contact_update.setString( index++, contact.updatedBy);
				prep_stmts.contact_update.setString( index++, contact.updatedDate);
				
				prep_stmts.contact_update.setInt( index++, contact.contactId);
				prep_stmts.contact_update.execute();
				}
			else
				{
				int index = 1;
				prep_stmts.contact_insert.setInt( index++, contact.serviceAddressId);
				prep_stmts.contact_insert.setString( index++, contact.contactName);
				prep_stmts.contact_insert.setString( index++, contact.phone1);
				prep_stmts.contact_insert.setString( index++, contact.phone1Type);
				prep_stmts.contact_insert.setString( index++, contact.phone2);
				prep_stmts.contact_insert.setString( index++, contact.phone2Type);
				prep_stmts.contact_insert.setString( index++, contact.email);
				prep_stmts.contact_insert.setString( index++, contact.contactType);
				prep_stmts.contact_insert.setString( index++, contact.ext1);
				prep_stmts.contact_insert.setString( index++, contact.ext2);
				prep_stmts.contact_insert.setString( index++, contact.updatedBy);
				prep_stmts.contact_insert.setString( index++, contact.updatedDate);
				
				prep_stmts.contact_insert.execute();
				}
			
			}
		}
	
	private void validateContact(ServiceAddressContact ct)
		{
		// Validate the data size and prevent truncation errors by SQL Server
		if( (ct.contactName != null) && (ct.contactName.length() > 50) )
			ct.contactName = ct.contactName.substring(0, 50);
		
		if( (ct.phone1 != null) && (ct.phone1.length() > 25) )
			ct.phone1 = ct.phone1.substring(0, 25);
		
		if( (ct.phone1Type != null) && (ct.phone1Type.length() > 20) )
			ct.phone1Type = ct.phone1Type.substring(0, 20);
		
		if( (ct.phone2 != null) && (ct.phone2.length() > 25) )
			ct.phone2 = ct.phone2.substring(0, 25);
		
		if( (ct.phone2Type != null) && (ct.phone2Type.length() > 20) )
			ct.phone2Type = ct.phone2Type.substring(0, 20);
		
		if( (ct.email != null) && (ct.email.length() > 50) )
			ct.email = ct.email.substring(0, 50);
		
		if( (ct.contactType != null) && (ct.contactType.length() > 50) )
			ct.contactType = ct.contactType.substring(0, 50);
		
		if( (ct.ext1 != null) && (ct.ext1.length() > 10) )
			ct.ext1 = ct.ext1.substring(0, 10);
		
		if( (ct.ext2 != null) && (ct.ext2.length() > 10) )
			ct.ext2 = ct.ext2.substring(0, 10);
		}
	
	// Open Tag Processing Functions
	private void processOpenTags() throws SQLException
		{
		// Drop the old open tags
		dropOpenTags();
		
		int size = request.pkg.openTags.size();
		OpenServiceTag tag;
		Pair pair;
		
		
		for( int i = 0; i < size; i++ )
			{
			tag = request.pkg.openTags.get(i);
			
			validateOpenTag(tag);
			if( tag.serviceTagId <= 0 )
				{
				pair = new Pair();
				pair.id = tag.serviceTagId;
				tag.serviceTagId = insertNextTag();
				pair.newid = tag.serviceTagId;
				serviceTagId_map.add(pair);
				ServiceTagId_Translation.put(pair.id, pair.newid);
				}
			
			if ( !serviceTagIds.contains(tag.serviceTagId) )
				{
				if( tag.submit )
					insertClosedTag( tag );
				else
					insertOpenTag(tag);
				}
			else
				{
				System.err.println("Error: ServiceTagID already exists. ID: '"
								+ tag.serviceTagId + "' Open: '" + !tag.submit + "'" + " Syncing Employee: " + empno
								+ " Tag owned by: '" + tag.empno + "'");
				}
			
			}
		
		}
	
	private void dropOpenTags() throws SQLException
		{
		// First Delete all tables that have no children
		// Delete the openServiceLabors
		prep_stmts.openServiceLabor_drop.setString(1, request.MEID);
		prep_stmts.openServiceLabor_drop.execute();
		
		// Delete the openServiceMaterials
		prep_stmts.openServiceMaterial_drop.setString(1, request.MEID);
		prep_stmts.openServiceMaterial_drop.execute();
		
		// Delete the openServiceRefrigerant
		prep_stmts.openServiceRefrigerant_drop.setString(1, request.MEID);
		prep_stmts.openServiceRefrigerant_drop.execute();
		
		
		// Delete the openPMChecklists
		prep_stmts.openServicePMChecklist_drop.setString(1, request.MEID);
		prep_stmts.openServicePMChecklist_drop.execute();
		
		// Delete the openBlueUnits
		prep_stmts.openBlueUnit_drop.setString(1, request.MEID);
		prep_stmts.openBlueUnit_drop.execute();
		
		// Delete the openSafetyChecklists
		prep_stmts.openSafetyChecklist_drop.setString(1, request.MEID);
		prep_stmts.openSafetyChecklist_drop.execute();
		
		// Delete the openSafetyChecklistItems
		prep_stmts.openSafetyChecklistItem_drop.setString(1, request.MEID);
		prep_stmts.openSafetyChecklistItem_drop.execute();
		
		
		// Delete the Second level tables
		// Delete the openServiceTagUnits
		prep_stmts.openServiceTagUnit_drop.setString(1, request.MEID);
		prep_stmts.openServiceTagUnit_drop.execute();
		
		// Delete the openBlues
		prep_stmts.openBlue_drop.setString(1, request.MEID);
		prep_stmts.openBlue_drop.execute();
		
		
		// Finally delete the open tags, the highest level
		// Delete the openServiceTags
		prep_stmts.openServiceTag_drop.setString(1, request.MEID);
		prep_stmts.openServiceTag_drop.execute();
		
		// After deleting the open tags, we can add all ids to our validation list
		serviceTagIds.addAll( fetchAllIds("openServiceTag", "serviceTagId", serviceTagIdList) );
		}
	
	private void validateOpenTag(OpenServiceTag tag)
		{
		// Validate the data size and prevent truncation errors by SQL Server
			// Dispatch Validation
		if( (tag.serviceType != null) && (tag.serviceType.length() > 20) )
			tag.serviceType = tag.serviceType.substring(0, 20);
		
		if( (tag.batchNo != null) && (tag.batchNo.length() > 10) )
			tag.batchNo = tag.batchNo.substring(0, 10);
		
		if( (tag.jobNo != null) && (tag.jobNo.length() > 12) )
			tag.jobNo = tag.jobNo.substring(0, 12);
		
		// Alt Billing Validation
		if( (tag.billTo != null) && (tag.billTo.length() > 100) )
			tag.billTo = tag.billTo.substring(0, 100);
		
		if( (tag.billAddress1 != null) && (tag.billAddress1.length() > 100) )
			tag.billAddress1 = tag.billAddress1.substring(0, 100);
		
		if( (tag.billAddress2 != null) && (tag.billAddress2.length() > 100) )
			tag.billAddress2 = tag.billAddress2.substring(0, 100);
		
		if( (tag.billAddress3 != null) && (tag.billAddress3.length() > 100) )
			tag.billAddress3 = tag.billAddress3.substring(0, 100);
		
		if( (tag.billAddress4 != null) && (tag.billAddress4.length() > 100) )
			tag.billAddress4 = tag.billAddress4.substring(0, 100);
		
		if( (tag.billAttn != null) && (tag.billAttn.length() > 100) )
			tag.billAttn = tag.billAttn.substring(0, 100);
		
		// Site Validation
		if( (tag.siteName != null) && (tag.siteName.length() > 100) )
			tag.siteName = tag.siteName.substring(0, 100);
		
		if( (tag.tenant != null) && (tag.tenant.length() > 100) )
			tag.tenant = tag.tenant.substring(0, 100);
		
		if( (tag.address1 != null) && (tag.address1.length() > 100) )
			tag.address1 = tag.address1.substring(0, 100);
		
		if( (tag.address2 != null) && (tag.address2.length() > 100) )
			tag.address2 = tag.address2.substring(0, 100);
		
		if( (tag.city != null) && (tag.city.length() > 50) )
			tag.city = tag.city.substring(0, 50);
		
		if( (tag.state != null) && (tag.state.length() > 10) )
			tag.state = tag.state.substring(0, 10);
		
		if( (tag.zip != null) && (tag.zip.length() > 10) )
			tag.zip = tag.zip.substring(0, 10);
		
		if( (tag.buildingNo != null) && (tag.buildingNo.length() > 10) )
			tag.buildingNo = tag.buildingNo.substring(0, 10);
		
		}
	
	private void insertOpenTag(OpenServiceTag tag) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.openServiceTag_insert;
		boolean found = false;
		
		// Test the DispatchId
		if( tag.dispatchId > 0 )
			{
			for( Iterator<DispatchInfo> i = dispatchIds.iterator(); i.hasNext(); )
				{
				if( i.next().dispatchId == tag.dispatchId )
					{
					found = true;
					break;
					}
				}
			}
		if( !found )
			tag.dispatchId = 0;
		
		found = false;
		// Test the ServiceAddressId
		if( tag.serviceAddressId > 0 )
			{
			for( Iterator<ServiceAddressInfo> i = serviceAddressIds.iterator(); i.hasNext(); )
				{
				if( i.next().serviceAddressId == tag.serviceAddressId )
					{
					found = true;
					break;
					}
				}
			}
		if( !found )//TODO break dispatch ID
			tag.serviceAddressId = 0;
		
		int index = 1;
		stmt.setInt(index++, tag.serviceTagId);
		stmt.setInt(index++, tag.serviceAddressId);
		stmt.setInt(index++, tag.dispatchId);
		stmt.setString(index++, tag.serviceType);
		stmt.setString(index++, tag.serviceDate);
		stmt.setString(index++, tag.billTo);
		stmt.setString(index++, tag.billAddress1);
		stmt.setString(index++, tag.billAddress2);
		stmt.setString(index++, tag.billAddress3);
		stmt.setString(index++, tag.billAddress4);
		stmt.setString(index++, tag.billAttn);
		stmt.setString(index++, tag.siteName);
		stmt.setString(index++, tag.tenant);
		stmt.setString(index++, tag.address1);
		stmt.setString(index++, tag.address2);
		stmt.setString(index++, tag.city);
		stmt.setString(index++, tag.state);
		stmt.setString(index++, tag.zip);
		stmt.setString(index++, tag.buildingNo);
		stmt.setString(index++, tag.note);
		stmt.setString(index++, tag.batchNo);
		stmt.setString(index++, tag.jobNo);
		stmt.setString(index++, tag.empno);
		if( tag.empno == empno )
			stmt.setString(index++, request.MEID );
		else
			stmt.setString(index++, "0");
		stmt.setString(index++, tag.disposition);
		
		stmt.execute();
		
		int size = tag.units.size();
		for( int i = 0; i < size; i++ )
			{
			insertOpenServiceUnit( tag.serviceTagId, tag.units.get(i) );
			}
		
		if( tag.blue != null )
			insertOpenBlue( tag.serviceTagId, tag.blue );
		if( tag.safetyChecklist != null )
			insertOpenSafetyChecklist( tag.serviceTagId, tag.safetyChecklist );
		}
	
	private void insertOpenServiceUnit(int serviceTagId, ServiceTagUnit unit) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.openServiceTagUnit_insert;
		int index = 1;
		Pair pair;
		
		if( !equipmentIds.contains(unit.equipmentId) )
			unit.equipmentId = validate(unit.equipmentId, new_equipmentIds );
		
		// Validate the data size and prevent truncation errors by SQL Server
		if( (unit.servicePerformed != null) && (unit.servicePerformed.length() > 5000) )
			unit.servicePerformed = unit.servicePerformed.substring(0, 5000);
		if( (unit.comments != null) && (unit.comments.length() > 5000) )
			unit.comments = unit.comments.substring(0, 5000);
		
		stmt.setInt(index++, serviceTagId);
		stmt.setInt(index++, unit.equipmentId);
		stmt.setString(index++, unit.servicePerformed);
		stmt.setString(index++, unit.comments);
		
		stmt.execute();
		
		// Fetch the ServiceTagUnitId
		ResultSet result = stmt.getGeneratedKeys();
		if( result.next() )
			{
			pair = new Pair();
			pair.id = unit.serviceTagUnitId;
			unit.serviceTagUnitId = result.getInt(1);
			pair.newid = unit.serviceTagUnitId;
			serviceTagUnitId_map.add(pair);
			ServiceTagUnitId_Translation.put(pair.id, (pair.newid*-1));
			}
		else
			throw new SQLException("Error inserting Service Tag Unit. No ID Returned.");
		
		insertOpenLabor(unit.serviceTagUnitId, unit.labor );
		insertOpenMaterial(unit.serviceTagUnitId, unit.material );
		insertOpenRefrigerant(unit.serviceTagUnitId, unit.refrigerant );
		insertOpenPMChecklist(unit.serviceTagUnitId, unit.pmChecklist );
		}
	
	private void insertOpenLabor( int serviceTagUnitId, List<ServiceLabor> list ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.openServiceLabor_insert;
		ServiceLabor labor;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			labor = list.get(i);
			
			stmt.setInt(1, serviceTagUnitId);
			stmt.setString(2, labor.serviceDate);
			stmt.setFloat(3, labor.regHours);
			stmt.setFloat(4, labor.thHours);
			stmt.setFloat(5, labor.dtHours);
			stmt.setString(6, labor.mechanic);
			stmt.setString(7,labor.rate);
			stmt.execute();
			}
		}
	
	private void insertOpenMaterial( int serviceTagUnitId, List<ServiceMaterial> list ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.openServiceMaterial_insert;
		ServiceMaterial material;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			material = list.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (material.materialDesc != null) && (material.materialDesc.length() > 100) )
				material.materialDesc = material.materialDesc.substring(0, 100);
			if( (material.refrigerantAdded != null) && (material.refrigerantAdded.length() > 10) )
				material.refrigerantAdded = material.refrigerantAdded.substring(0, 10);
			if( (material.source != null) && (material.source.length() > 200) )
				material.source = material.source.substring(0, 200);
			
			stmt.setInt(1, serviceTagUnitId);
			stmt.setFloat(2, material.quantity);
			stmt.setString(3, material.materialDesc);
			stmt.setFloat(4, material.cost);
			stmt.setString(5, material.refrigerantAdded);
			stmt.setString(6, material.source);
			
			stmt.execute();
			}
		}
		
	private void insertOpenRefrigerant( int serviceTagUnitId, List<ServiceRefrigerant> list ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.openServiceRefrigerant_insert;
		ServiceRefrigerant refrigerant;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			refrigerant = list.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
		//	if( (material.materialDesc != null) && (material.materialDesc.length() > 100) )
		//		material.materialDesc = material.materialDesc.substring(0, 100);
		//	if( (material.refrigerantAdded != null) && (material.refrigerantAdded.length() > 10) )
		//		material.refrigerantAdded = material.refrigerantAdded.substring(0, 10);
		//	if( (material.source != null) && (material.source.length() > 200) )
		//		material.source = material.source.substring(0, 200);
			
			stmt.setInt(1, serviceTagUnitId);
			stmt.setString(2, refrigerant.transferDate);
			stmt.setString(3, refrigerant.techName);
			stmt.setString(4,refrigerant.typeOfRefrigerant);
			stmt.setFloat(5, refrigerant.amount);
			stmt.setString(6,refrigerant.nameOfCylinder);
			stmt.setString(7,refrigerant.cylinderSerialNo);
			stmt.setString(8,refrigerant.transferedTo);
			stmt.setString(9,refrigerant.serialNo);
			stmt.setString(10,refrigerant.modelNo);
			stmt.execute();
			}
		}
	
	private void insertOpenPMChecklist( int serviceTagUnitId, List<PMChecklist> list ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.openPMChecklist_insert;
		PMChecklist pmChecklist;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			pmChecklist = list.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (pmChecklist.itemValue!= null) && (pmChecklist.itemValue.length() > 100) )
				pmChecklist.itemValue = pmChecklist.itemValue.substring(0, 100);
			if( (pmChecklist.itemComment != null) && (pmChecklist.itemComment.length() > 100) )
				pmChecklist.itemComment = pmChecklist.itemComment.substring(0, 100);
			
			stmt.setInt(1, serviceTagUnitId);
			stmt.setString(2, pmChecklist.itemText);
			stmt.setString(3, pmChecklist.itemType);
			stmt.setString(4, pmChecklist.itemValue);
			stmt.setString(5, pmChecklist.itemComment);
			
			stmt.execute();
			}
		}
	
	private void insertOpenBlue( int serviceTagId, OpenBlue blue ) throws SQLException
		{
		PreparedStatement blue_stmt = prep_stmts.openBlue_insert;
		PreparedStatement unit_stmt = prep_stmts.openBlueUnit_insert;
		
		blue_stmt.setInt(1, serviceTagId);
		blue_stmt.setString(2, blue.dateCreated);
		blue_stmt.execute();
		
		// Fetch the ServiceTagUnitId
		ResultSet result = blue_stmt.getGeneratedKeys();
		if( result.next() )
			{
			blue.blueId = result.getInt(1);
			}
		else
			throw new SQLException("Error inserting Open Blue Header. No ID Returned.");
		
		OpenBlueUnit unit;
		int size = blue.units.size();
		for( int i = 0; i < size; i++ )
			{
			unit = blue.units.get(i);
			
			validateBlueUnit(unit);
			if( !equipmentIds.contains(unit.equipmentId) )
				unit.equipmentId = validate(unit.equipmentId, new_equipmentIds );
			
			unit_stmt.setInt(1, blue.blueId);
			unit_stmt.setInt(2, unit.equipmentId);
			unit_stmt.setString(3, unit.description);
			unit_stmt.setString(4, unit.materials);
			unit_stmt.setFloat(5, unit.laborHours);
			unit_stmt.setString(6, unit.notes);
			unit_stmt.setFloat(7, unit.cost);
			if( (unit.completed != null) && (unit.completed.contentEquals("M") || unit.completed.contentEquals("Y")) )
				unit_stmt.setString(8, "Y");
			else
				unit_stmt.setString(8, "N");
			
			unit_stmt.execute();
			}
		}
	
	private void insertOpenSafetyChecklist( int serviceTagId, OpenSafetyTagChecklist checklist ) throws SQLException
		{
		PreparedStatement header_stmt = prep_stmts.openSafetyChecklist_insert;
		PreparedStatement item_stmt = prep_stmts.openSafetyChecklistItem_insert;
		
		// Validate the data size and prevent truncation errors by SQL Server
		if( (checklist.comments != null) && (checklist.comments.length() > 5000) )
			checklist.comments = checklist.comments.substring(0, 5000);
		
		header_stmt.setInt(1, serviceTagId);
		header_stmt.setString(2, checklist.checkListDate);
		header_stmt.setString(3, checklist.comments);
		header_stmt.execute();
		
		ChecklistItem item;
		int size = checklist.items.size();
		for( int i = 0; i < size; i++ )
			{
			item = checklist.items.get(i);
			
			if( !safetyChecklistIds.contains(item.safetyChecklistId) )
				throw new SQLException("ERROR: Invalid SafetyChecklistId");
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (item.itemValue != null) && (item.itemValue.length() > 50) )
				item.itemValue = item.itemValue.substring(0, 50);
			
			item_stmt.setInt(1, serviceTagId);
			item_stmt.setInt(2, item.safetyChecklistId);
			item_stmt.setString(3, item.itemRequired);
			item_stmt.setString(4, item.itemValue);
			
			item_stmt.execute();
			}
		}
	
	// Closed Tag Processing Functions
	private void insertClosedTag( OpenServiceTag tag ) throws SQLException
		{
		newClosedTags.add(tag.serviceTagId);
		
		PreparedStatement stmt = prep_stmts.serviceTag_insert;
		
		int index = 1;
		stmt.setInt(index++, tag.serviceTagId);
		stmt.setInt(index++, tag.serviceAddressId);
		stmt.setInt(index++, tag.dispatchId);
		stmt.setString(index++, tag.serviceType);
		stmt.setString(index++, tag.serviceDate);
		stmt.setString(index++, tag.billTo);
		stmt.setString(index++, tag.billAddress1);
		stmt.setString(index++, tag.billAddress2);
		stmt.setString(index++, tag.billAddress3);
		stmt.setString(index++, tag.billAddress4);
		stmt.setString(index++, tag.billAttn);
		stmt.setString(index++, tag.siteName);
		stmt.setString(index++, tag.tenant);
		stmt.setString(index++, tag.address1);
		stmt.setString(index++, tag.address2);
		stmt.setString(index++, tag.city);
		stmt.setString(index++, tag.state);
		stmt.setString(index++, tag.zip);
		stmt.setString(index++, tag.buildingNo);
		stmt.setString(index++, tag.note);
		stmt.setString(index++, tag.batchNo);
		stmt.setString(index++, tag.jobNo);
		stmt.setString(index++, tag.empno);
		stmt.setString(index++, tag.disposition);
		
		stmt.execute();
		
		int size = tag.units.size();
		for( int i = 0; i < size; i++ )
			{
			insertServiceUnit( tag.units.get(i), tag );
			}
		
		insertBlue( tag.serviceTagId, tag.blue );
		insertSafetyChecklist( tag.serviceTagId, tag.safetyChecklist );
		insertTagReceipt( tag.serviceTagId, tag.receipts );
		
		}
	
	private void insertServiceUnit( ServiceTagUnit unit, OpenServiceTag tag) throws SQLException
		{
		if( !equipmentIds.contains(unit.equipmentId) )
			unit.equipmentId = validate(unit.equipmentId, new_equipmentIds );
		
		PreparedStatement stmt = prep_stmts.serviceTagUnit_insert;
		int index = 1;
		
		// Validate the data size and prevent truncation errors by SQL Server
		if( (unit.servicePerformed != null) && (unit.servicePerformed.length() > 5000) )
			unit.servicePerformed = unit.servicePerformed.substring(0, 5000);
		if( (unit.comments != null) && (unit.comments.length() > 5000) )
			unit.comments = unit.comments.substring(0, 5000);
		
		stmt.setInt(index++, tag.serviceTagId);
		stmt.setInt(index++, unit.equipmentId);
		stmt.setString(index++, unit.servicePerformed);
		stmt.setString(index++, unit.comments);
		
		stmt.execute();
		
		// Fetch the ServiceTagUnitId
		ResultSet result = stmt.getGeneratedKeys();
		int oldId = unit.serviceTagUnitId;
		if( result.next() )
			{
			unit.serviceTagUnitId = result.getInt(1);
			}
		else
			throw new SQLException("Error inserting Service Tag Unit. No ID Returned.");
		
		ServiceTagUnitId_Translation.put(oldId, unit.serviceTagUnitId);
		
		insertLabor(unit.serviceTagUnitId, unit.labor, tag );
		insertMaterial(unit.serviceTagUnitId, unit.material );
		insertRefrigerant(unit.serviceTagUnitId, unit.refrigerant);
		insertPMChecklist(unit.serviceTagUnitId, unit.pmChecklist );
		insertUnitPhoto(unit.serviceTagUnitId, unit.photos );
		}
	
	private void insertLabor( int serviceTagUnitId, List<ServiceLabor> list, OpenServiceTag tag ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.serviceLabor_insert;
		ServiceLabor labor;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			labor = list.get(i);
			
			stmt.setInt(1, serviceTagUnitId);
			stmt.setString(2, labor.serviceDate);
			stmt.setFloat(3, labor.regHours);
			stmt.setFloat(4, labor.thHours);
			stmt.setFloat(5, labor.dtHours);
			stmt.setString(6, labor.mechanic);
			stmt.setString(7,labor.rate);
			
			stmt.execute();
			
			prepTimeCard(labor, tag);
			}
		}
	
	private void insertMaterial( int serviceTagUnitId, List<ServiceMaterial> list ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.serviceMaterial_insert;
		ServiceMaterial material;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			material = list.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (material.materialDesc != null) && (material.materialDesc.length() > 100) )
				material.materialDesc = material.materialDesc.substring(0, 100);
			if( (material.refrigerantAdded != null) && (material.refrigerantAdded.length() > 10) )
				material.refrigerantAdded = material.refrigerantAdded.substring(0, 10);
			if( (material.source != null) && (material.source.length() > 200) )
				material.source = material.source.substring(0, 200);
			
			stmt.setInt(1, serviceTagUnitId);
			stmt.setFloat(2, material.quantity);
			stmt.setString(3, material.materialDesc);
			stmt.setFloat(4, material.cost);
			stmt.setString(5, material.refrigerantAdded);
			stmt.setString(6, material.source);
			
			stmt.execute();
			}
		}
		
		private void insertRefrigerant( int serviceTagUnitId, List<ServiceRefrigerant> list ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.serviceRefrigerant_insert;
		ServiceRefrigerant refrigerant;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			refrigerant = list.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			//if( (material.materialDesc != null) && (material.materialDesc.length() > 100) )
				//material.materialDesc = material.materialDesc.substring(0, 100);
		//	if( (material.refrigerantAdded != null) && (material.refrigerantAdded.length() > 10) )
		//		material.refrigerantAdded = material.refrigerantAdded.substring(0, 10);
		//	if( (material.source != null) && (material.source.length() > 200) )
		//		material.source = material.source.substring(0, 200);
			
			stmt.setInt(1, serviceTagUnitId);
			stmt.setString(2, refrigerant.transferDate);
			stmt.setString(3, refrigerant.techName);
			stmt.setString(4,refrigerant.typeOfRefrigerant);
			stmt.setFloat(5, refrigerant.amount);
			stmt.setString(6,refrigerant.nameOfCylinder);
			stmt.setString(7,refrigerant.cylinderSerialNo);
			stmt.setString(8,refrigerant.transferedTo);
			stmt.setString(9,refrigerant.serialNo);
			stmt.setString(10,refrigerant.modelNo);
			stmt.execute();
			
			
			}
		}
	
	private void insertPMChecklist( int serviceTagUnitId, List<PMChecklist> list ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.PMChecklist_insert;
		PMChecklist pmChecklist;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			pmChecklist = list.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (pmChecklist.itemValue!= null) && (pmChecklist.itemValue.length() > 100) )
				pmChecklist.itemValue = pmChecklist.itemValue.substring(0, 100);
			if( (pmChecklist.itemComment != null) && (pmChecklist.itemComment.length() > 100) )
				pmChecklist.itemComment = pmChecklist.itemComment.substring(0, 100);
			
			stmt.setInt(1, serviceTagUnitId);
			stmt.setString(2, pmChecklist.itemText);
			stmt.setString(3, pmChecklist.itemType);
			stmt.setString(4, pmChecklist.itemValue);
			stmt.setString(5, pmChecklist.itemComment);
			
			stmt.execute();
			}
		}
	
	private void insertUnitPhoto( int serviceTagUnitId, List<Photo> list ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.servicePhoto_insert;
		Photo photo;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			photo = list.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (photo.comments != null) && (photo.comments.length() > 5000) )
				photo.comments = photo.comments.substring(0, 5000);
			
			stmt.setInt(1, serviceTagUnitId);
			stmt.setString(2, photo.photoDate);
			stmt.setBytes(3, photo.photo);
			stmt.setString(4, photo.comments);
			
			stmt.execute();
			}
		}
	
	private void insertBlue( int serviceTagId, OpenBlue blue) throws SQLException
		{
		if( blue == null )
			return;
		
		PreparedStatement blue_stmt = prep_stmts.blue_insert;
		PreparedStatement unit_stmt = prep_stmts.blueUnit_insert;
		
		blue_stmt.setInt(1, serviceTagId);
		blue_stmt.setString(2, blue.dateCreated);
		
		blue_stmt.execute();
		
		// Fetch the ServiceTagUnitId
		ResultSet result = blue_stmt.getGeneratedKeys();
		if( result.next() )
			{
			blue.blueId = result.getInt(1);
			}
		else
			throw new SQLException("Error inserting Blue Header. No ID Returned.");
		
		OpenBlueUnit unit;
		int size = blue.units.size();
		for( int i = 0; i < size; i++ )
			{
			unit = blue.units.get(i);
			
			validateBlueUnit(unit);
			if( !equipmentIds.contains(unit.equipmentId) )
				unit.equipmentId = validate(unit.equipmentId, new_equipmentIds );
			
			unit_stmt.setInt(1, blue.blueId);
			unit_stmt.setInt(2, unit.equipmentId);
			unit_stmt.setString(3, unit.description);
			unit_stmt.setString(4, unit.materials);
			unit_stmt.setFloat(5, unit.laborHours);
			unit_stmt.setString(6, unit.notes);
			unit_stmt.setFloat(7, unit.cost);
			
			unit_stmt.execute();
			}
		}
	
	private void validateBlueUnit(OpenBlueUnit unit)
		{
		// Validate the data size and prevent truncation errors by SQL Server
		if( (unit.description != null) && (unit.description.length() > 5000) )
			unit.description = unit.description.substring(0, 5000);
		
		if( (unit.materials != null) && (unit.materials.length() > 2000) )
			unit.materials = unit.materials.substring(0, 2000);
		
		if( (unit.notes != null) && (unit.notes.length() > 5000) )
			unit.notes = unit.notes.substring(0, 5000);
		}
	
	private void insertSafetyChecklist( int serviceTagId, OpenSafetyTagChecklist checklist ) throws SQLException
		{
		PreparedStatement header_stmt = prep_stmts.safetyChecklist_insert;
		PreparedStatement item_stmt = prep_stmts.safetyChecklistItem_insert;
		
		// Validate the data size and prevent truncation errors by SQL Server
		if( (checklist.comments != null) && (checklist.comments.length() > 5000) )
			checklist.comments = checklist.comments.substring(0, 5000);
		
		header_stmt.setInt(1, serviceTagId);
		header_stmt.setString(2, checklist.checkListDate);
		header_stmt.setString(3, checklist.comments);
		header_stmt.execute();
		
		ChecklistItem item;
		int size = checklist.items.size();
		for( int i = 0; i < size; i++ )
			{
			item = checklist.items.get(i);
			
			if( !safetyChecklistIds.contains(item.safetyChecklistId) )
				throw new SQLException("ERROR: Invalid SafetyChecklistId");
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (item.itemValue != null) && (item.itemValue.length() > 50) )
				item.itemValue = item.itemValue.substring(0, 50);
			
			item_stmt.setInt(1, serviceTagId);
			item_stmt.setInt(2, item.safetyChecklistId);
			item_stmt.setString(3, item.itemRequired);
			item_stmt.setString(4, item.itemValue);
			
			item_stmt.execute();
			}
		}
	
	private void insertTagReceipt( int serviceTagId, List<Receipt> list ) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.serviceReceipt_insert;
		Receipt photo;
		int size = list.size();
		for( int i = 0; i < size; i++ )
			{
			photo = list.get(i);
			
			// Validate the data size and prevent truncation errors by SQL Server
			if( (photo.comments != null) && (photo.comments.length() > 5000) )
				photo.comments = photo.comments.substring(0, 5000);
			
			stmt.setInt(1, serviceTagId);
			stmt.setString(2, photo.photoDate);
			stmt.setBytes(3, photo.photo);
			stmt.setString(4, photo.comments);
			
			stmt.execute();
			}
		}

	
	// Time Card Functions
	private void prepTimeCard(ServiceLabor labor, OpenServiceTag tag) throws SQLException
		{
		boolean terminated = false;
		boolean department = true;
		boolean pay_period_invalid = false;
		boolean approved_complete = false;
		boolean multi_headers = false;
		boolean date_invalid = false;
		boolean dispatchFound = false;
		
		TimeCardRec rec = new TimeCardRec();
		rec.regHrs = labor.regHours;
		rec.thHrs = labor.thHours;
		rec.dtHrs = labor.dtHours;
		rec.date_worked = labor.serviceDate;
		rec.empno = labor.mechanic;
		rec.rate = labor.rate;
		// Test if the employee is terminated
		prep_stmts.timecard_check_terminated.setString(1, rec.empno);
		ResultSet result = prep_stmts.timecard_check_terminated.executeQuery();
		String termed;
		String dept;
		if( result.next() )
			{
			termed = result.getString(1);
			dept = result.getString(2);
			if( dept == null )
				department = false;
			else
				{
				if( !dept.contentEquals("REF") )
					department = false;
				}
			
			
			if( termed == null )
				terminated = true;
			else
				{
				if( !termed.contentEquals("N") )
					terminated = true;
				}
			}
		
		if( !department )
			{
			// TODO: Do we need any logging here?
			return;
			}
		
		// Test and Get the Pay Period
		prep_stmts.timecard_payperiod.setString(1, rec.date_worked);
		prep_stmts.timecard_payperiod.setString(2, rec.date_worked);
		result = prep_stmts.timecard_payperiod.executeQuery();
		if( result.next() )
			{
			rec.pay_per = result.getString(1);
			
			if( result.next() )
				pay_period_invalid = true;
			}
		else
			pay_period_invalid = true;
		
		// Test the timecard header
		prep_stmts.timecard_header.setString(1, rec.empno);
		prep_stmts.timecard_header.setString(2, rec.date_worked);
		prep_stmts.timecard_header.setString(3, rec.date_worked);
		result = prep_stmts.timecard_header.executeQuery();
		if( result.next() )
			{
			String completed = result.getString(1);
			String approved = result.getString(2);
			if( (completed.length() > 0) || (approved.length() > 0) )
				{
				approved_complete = true;
				}
			if( result.next() )
				multi_headers = true;
			}
		else
			{
			// Do nothing, the stored procedure handles inserting a timecard header
			}
		
		try
			{
			DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
			Date d = dateformat.parse(rec.date_worked);
			Calendar cal = new GregorianCalendar();			cal.setTime(d);
			Calendar date_start = new GregorianCalendar();	date_start.setTime(d);
			Calendar date_end = new GregorianCalendar();	date_end.setTime(d);
			
			int day_of_week_adjusted = cal.get(Calendar.DAY_OF_WEEK);
			
			// Adjust the day of the week to Therma's schedule
			if( day_of_week_adjusted >= 3 )
				day_of_week_adjusted -= 2;
			else
				day_of_week_adjusted += 5;
			
			rec.day_of_week = day_of_week_adjusted;
			
			date_start.add(Calendar.DATE, -(rec.day_of_week-1) );
			date_end.add(Calendar.DATE, 7-rec.day_of_week);
			
			rec.date_start = dateformat.format( date_start.getTime() );
			rec.date_end = dateformat.format( date_end.getTime() );
			}
		catch (Exception e)
			{
			date_invalid = true;
			e.printStackTrace();
			}
		
		
		// Fill the Site Name and JobNo from Dispatch, Service Address, or the Tag
		if( tag.dispatchId > 0 )
			{
			DispatchInfo info = null;
			for( Iterator<DispatchInfo> i = dispatchIds.iterator(); i.hasNext(); )
				{
				info = i.next();
				if( info.dispatchId == tag.dispatchId )
					{
					if( info.tenant != null && (info.tenant.length() > 0) )
						{
						rec.job_name = info.tenant;
						System.out.println("Tenant is available for Timecard. Tenant = '" + info.tenant + "'");
						}
					else
						{
						rec.job_name = info.siteName;
						System.out.println("Tenant is not available for Timecard. Site Name = '" + info.siteName + "'");
						}
					rec.job_no = info.jobNo.replaceAll("(TTCA)", "");
					dispatchFound = true;
					break;
					}
				}
			if( !dispatchFound )
				{
				rec.job_name = "?";
				rec.job_no = "?";
				}
			}
		
		if( !dispatchFound && (tag.serviceAddressId > 0) )
			{
			System.out.println("No Dispatch Linked; attempting to find the Service Address");
			// Attemtp to find the service address site name.
			ServiceAddressInfo sinfo = null;
			for( Iterator<ServiceAddressInfo> i = serviceAddressIds.iterator(); i.hasNext(); )
				{
				sinfo = i.next();
				if( sinfo.serviceAddressId == tag.serviceAddressId )
					{
					System.out.println("Found Service Address. Site Name = '" + sinfo.siteName + "'");
					if( tag.tenant != null && tag.tenant.length() > 0 )
						rec.job_name = tag.tenant;
					else
						rec.job_name = sinfo.siteName;
					rec.job_no = tag.jobNo;
					break;
					}
				}
			}
		
		if( (tag.dispatchId <= 0 ) && (tag.serviceAddressId <= 0) )
			{
			System.out.println("No Dispatch or ServiceAddress Linked. Adding manual entry data");
			if( tag.tenant != null && tag.tenant.length() > 0 )
				rec.job_name = tag.tenant;
			else
				rec.job_name = tag.siteName;
			rec.job_no = tag.jobNo;
			}
		
		
		if( terminated || pay_period_invalid || multi_headers || date_invalid )
			{
			String reason = "";
			
			int index = 1;
			PreparedStatement stmt = prep_stmts.timecard_exception;
			stmt.setString(index++, rec.empno);
			stmt.setString(index++, rec.pay_per);
			stmt.setInt(index++, tag.serviceTagId);
			stmt.setString(index++, rec.job_no);
			stmt.setString(index++, rec.job_name);
			stmt.setString(index++, rec.date_worked);
			stmt.setFloat(index++, rec.regHrs);
			stmt.setFloat(index++, rec.thHrs);
			stmt.setFloat(index++, rec.dtHrs);
			//stmt.setString(index++, rec.rate);
			if( terminated )
				reason += TIMECARD_TERMINATED;
			if( pay_period_invalid )
				reason += TIMECARD_PAY_PERIOD;
			if( approved_complete )
				reason += TIMECARD_APPROVED_COMPLETE;
			if( multi_headers )
				reason += TIMECARD_MULTI_HEADERS;
			if( date_invalid )
				reason += TIMECARD_INVALID_DATE + "Date: '" + rec.date_worked + "'";
			
			stmt.setString(index++, reason );
			
			stmt.execute();
			}
		else
			addToTimeCard(rec);
		}
	
	private void addToTimeCard(TimeCardRec rec)
		{
		boolean found = false;
		TimeCard tc;
		int size = tc_list.size();
		for( int i = 0; i < size; i++)
			{
			tc = tc_list.get(i);
			if( tc.job_no.contentEquals(rec.job_no) &&
					tc.pay_per.contentEquals(rec.pay_per) &&
					tc.empno.contentEquals(rec.empno) )
				{
				// Add to this timecard
				tc.worked[rec.day_of_week-1].date_worked = rec.date_worked;
				tc.worked[rec.day_of_week-1].regHrs += rec.regHrs;
				tc.worked[rec.day_of_week-1].thHrs += rec.thHrs;
				tc.worked[rec.day_of_week-1].dtHrs += rec.dtHrs;
				
				found = true;
				break;
				}
			}
		
		// If not found, append to the timecard list
		if( !found )
			{
			tc = new TimeCard();
			tc.job_name = rec.job_name;
			tc.job_no = rec.job_no;
			tc.pay_per = rec.pay_per;
			tc.empno = rec.empno;
			
			tc.date_start = rec.date_start;
			tc.date_end = rec.date_end;
			
			tc.worked[rec.day_of_week-1].date_worked = rec.date_worked;
			tc.worked[rec.day_of_week-1].regHrs += rec.regHrs;
			tc.worked[rec.day_of_week-1].thHrs += rec.thHrs;
			tc.worked[rec.day_of_week-1].dtHrs += rec.dtHrs;
			
			tc_list.add(tc);
			}
		
		}
	
	private void insertTimeCard() throws SQLException
		{
		PreparedStatement stmt = prep_stmts.timecard_stored_proc;
		
		TimeCard timecard;
		int size = tc_list.size();
		for( int i = 0; i < size; i++ )
			{
			timecard = tc_list.get(i);
			
			if( !timecard.isEmpty() )
				{
				int index = 1;
				stmt.setString(index++, timecard.empno);
				stmt.setString(index++, timecard.pay_per);
				stmt.setString(index++, timecard.date_start);
				stmt.setString(index++, timecard.date_end);
				stmt.setString(index++, timecard.job_no);
				stmt.setString(index++, timecard.job_name);
				stmt.setString(index++, timecard.acct);
				stmt.setString(index++, timecard.bill_code);
				stmt.setString(index++, timecard.shop_flag);
				stmt.setString(index++, timecard.ocip_flag);
				stmt.setString(index++, timecard.nonwork_reason);
				stmt.setString(index++, timecard.note);
				stmt.setString(index++, timecard.travel_flag);
				
				TimeCard.Time time;
				for( int j = 0; j < timecard.worked.length; j++ )
					{
					time = timecard.worked[j];
					stmt.setString(index++, time.date_worked);
					stmt.setFloat(index++, time.regHrs);
					stmt.setFloat(index++, time.thHrs);
					stmt.setFloat(index++, time.dtHrs);
					stmt.setInt(index++, time.travel_qty);
					stmt.setFloat(index++, time.travel_rate);
					}
				}
			stmt.execute();
			}
			
		}
	
	// Service Tag Group processing functions
	private void processGroups() throws SQLException
		{
		PreparedStatement group_stmt = prep_stmts.group_insert;
		PreparedStatement xref_stmt = prep_stmts.groupXref_insert;
		PreparedStatement email_stmt = prep_stmts.emailQueue_insert;
		
		ServiceTagGroup group;
		int size = request.pkg.groups.size();
		int groupid, size2, map_size = serviceTagId_map.size();
		for( int i = 0; i < size; i++ )
			{
			// Fetch the Service Tag Group object
			group = request.pkg.groups.get(i);
			
			// Insert the group into the database
			group_stmt.setBytes(1, group.signature);
			group_stmt.setString(2, group.noSignatureReason);
			//group_stmt.setString(3, group.dateCreated); Use getDate instead, so we get a timestamp as well
			group_stmt.execute();
			
			// Fetch the ServiceTagUnitId
			ResultSet result = group_stmt.getGeneratedKeys();
			if( result.next() )
				{
				groupid = result.getInt(1);
				}
			else
				throw new SQLException("Error inserting Service Tag Group. No ID Returned.");
			
			if( group.emailList == null )
				group.emailList = "";
			
			// Insert the group into the email queue
			email_stmt.setInt(1, groupid);
			email_stmt.setString(2, group.emailList);
			email_stmt.setString(3, "I");
			email_stmt.execute();
			
			// Insert the Xref records
			size2 = group.serviceTagXref.size();
			int index;
			int tagId;
			Pair p;
			for( int j = 0; j < size2; j++ )
				{
				tagId = group.serviceTagXref.get(j);
				
				if( tagId < 0 )
					{
					index = -1;
					
					for( int k = 0; k < map_size; k++ )
						{
						if( serviceTagId_map.get(k).equals(tagId) )
							{
							index = k;
							break;
							}
						}
					if( index > -1 )
						{
						p = serviceTagId_map.get(index);
						tagId = p.newid;
						}
					}
				
				xref_stmt.setInt(1, groupid);
				xref_stmt.setInt(2, tagId );
				xref_stmt.execute();
				}
			}
		
		
		}
	
	// Other Insert Statements
	private int insertUnknown( String tableName, String descriptionCol ) throws SQLException
		{
		int ret;
		
		String sql = "INSERT INTO " + tableName + "( " + descriptionCol + " ) VALUES ( 'UNKNOWN' )";
		PreparedStatement stmt = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		stmt.execute();
		ResultSet result = stmt.getGeneratedKeys();
		if( result.next() )
			{
			ret = result.getInt(1);
			}
		else
			{
			throw new SQLException("Failed to insert UNKNOWN record.");
			}
		if( result != null && !result.isClosed() )
			{
			result.close();
			}
		
		stmt.close();
		
		return ret;
		}
	
	private int insertNextTag() throws SQLException
		{
		int ret;
		prep_stmts.nextTag_insert.setString(1, empno);
		prep_stmts.nextTag_insert.execute();
		
		ResultSet result = prep_stmts.nextTag_insert.getGeneratedKeys();
		if( result.next() )
			{
			ret = result.getInt(1);
			}
		else
			{
			throw new SQLException();
			}
		if( result != null && !result.isClosed())
			{
			result.close();
			}
		
		return ret;
		}
	
	private void insertSyncTransactionFail(Exception e) throws SQLException
		{
		PreparedStatement stmt = prep_stmts.sync_transaction_fail;
		
		stmt.setString(1, e.getMessage());
		stmt.setString(2, e.getLocalizedMessage());
		
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		stmt.setString(3, sw.toString());
		
		stmt.execute();
		}
	
	// Validation Functions
	private int validate(int ID, List<Pair> valid_list)
		{
		int ret = 0;
		
		// Resolve the new equipmentId
		if( ID <= 0 )
			{
			int size = valid_list.size();
			for( int i = 0; i < size; i++ )
				{
				if( valid_list.get(i).id == ID )
					{
					ret = valid_list.get(i).newid;
					}
				}
			}
		else
			{
			if( !valid_list.contains( ID ) )
				ret = 0;
			}
		
		return ret;
		}
	
	// TimeCard Classes
	private class TimeCard
		{
		String empno;
		String pay_per;
		String date_start;
		String date_end;
		String job_no;
		String job_name;
		String acct;
		String bill_code;
		String shop_flag;
		String ocip_flag;
		String travel_flag;
		String nonwork_reason;
		String note;
		
		Time[] worked;
		
		public TimeCard()
			{
			empno = "";
			pay_per = "";
			date_start = "";
			date_end = "";
			job_no = "";
			job_name = "";
			//acct = "L320";
			bill_code = "";
			shop_flag = "";
			ocip_flag = "";
			travel_flag = "";
			nonwork_reason = "";
			note = "";
			
			worked = new Time[7];
			for( int i = 0; i < worked.length; i++ )
				worked[i] = new Time();
			}
		
		public boolean isEmpty()
			{
			boolean empty = true;
			
			for( int i = 0; i < worked.length; i++ )
				if( !worked[i].isEmpty() )
					{
					empty = false;
					break;
					}
			
			return empty;
			}
		
		private class Time
			{
			float regHrs;
			float thHrs;
			float dtHrs;
			int travel_qty;
			float travel_rate;
			String date_worked;
			
			public Time()
				{
				regHrs = 0f;
				thHrs = 0f;
				dtHrs = 0f;
				travel_qty = 0;
				travel_rate = 0f;
				date_worked = "";
				}
			
			public boolean isEmpty()
				{
				return (regHrs != 0) && (thHrs != 0) && (dtHrs != 0)
						&& (travel_qty != 0) && (travel_rate != 0) && (date_worked.length() > 0);
				}
			}
			public void UpdateAcct()
			{
			try
				{
				String temp;
				if( job_no != null )
					temp = job_no.replaceAll("TTCA", "");
				else
					temp = "";
				
				if( temp.length() <= 0 )
					acct = "";
				else if( temp.charAt(0) == '3' )
					acct = "L330";
				else if( temp.charAt(0) == '4' )
					acct = "L260";
				else if( temp.charAt(0) == '5' )
					acct = "L320";
				else
					acct = "";
				}
			catch(Exception e)
				{
				acct = "";
				}
			}
		
		}
	
	private class TimeCardRec
		{
		String empno;
		String pay_per;
		String date_start;
		String date_end;
		String job_no;
		String job_name;
		
		int day_of_week;
		float regHrs;
		float thHrs;
		float dtHrs;
		String date_worked;
		String rate;
		
		public TimeCardRec()
			{
			empno = "";
			pay_per = "";
			date_start = "";
			date_end = "";
			job_no = "";
			job_name = "";
			
			day_of_week = 0;
			regHrs = 0f;
			thHrs = 0f;
			dtHrs = 0f;
			date_worked = "";
			rate = "";
			}
		}
	
	// Misc Functions
	private String buildParams(int size)
		{
		String ret = "";
		for( int i = 0; i < size; i++ )
			{
			ret += "?";
			if( i < size-1 )
				ret += ", ";
			}
		return ret;
		}
	
	/***********************************
	 * Form Processing Functions
	 ***********************************/
	
	/**
	 * Processes the user's Form Data
	 * 
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 */
	private void ProcessFormData() throws IllegalArgumentException, IllegalAccessException, SQLException
		{
		if( request.formDataPkg == null )
			return;
		
		DropOpenFormDataMEID(request.MEID);
		List<Object> NoIdList = new ArrayList<Object>();
		List<Object> HasIdList = new ArrayList<Object>();
		
		int size = request.formDataPkg.FormData.size();
		FormDataPackage.FormData formData;
		Integer stunitId = null;
		boolean ParentServiceUnit;
		for( int i = 0; i < size; i++ )
			{
			formData = (FormData) request.formDataPkg.FormData.get(i);
			
			if( formData.InputByEmpno != empno )
				formData.tabletMEID = null;
			
			ParentServiceUnit = formData.ParentTable.contentEquals("ServiceTagUnit");
			if( ParentServiceUnit )
				stunitId = ServiceTagUnitId_Translation.get(formData.ParentId);
			
			// Make sure the ID can be resolved before adding it to the processing lists
			if( !ParentServiceUnit || stunitId != null )
				{
				if( stunitId != null )
					formData.ParentId = stunitId;
				
				stunitId = null;
				
				if( formData.Completed == null )
					formData.Completed = "N";
				else if( formData.Completed.length() <= 0)
					formData.Completed = "N";
				
				if( formData.FormDataId > 0 )
					HasIdList.add(formData);
				else
					NoIdList.add(formData);
				}
			}
		
		Resolved_FormDataIds = ProcessObjectList("FormData", "FormDataId", NoIdList, null, null);
		InsertResolvedIdMap("Resolved_FormData", Resolved_FormDataIds);
		ProcessObjectList("FormData", null, HasIdList, null, null);
		ProcessObjectList("FormDataValues", null, request.formDataPkg.FormDataValues, "FormDataId", Resolved_FormDataIds);
		ProcessObjectList("FormDataSignatures", null, request.formDataPkg.FormDataSignatures, "FormDataId", Resolved_FormDataIds);
		ProcessObjectList("FormPhotos", "FormPhotoId", request.formDataPkg.FormPhotos, "FormDataId", Resolved_FormDataIds);
		}
	
	private void DropOpenFormDataMEID(String MEID) throws SQLException
		{
		String SQLStatement = "DELETE FROM FormDataValues WHERE FormDataId IN " +
				"(SELECT FormDataId FROM FormData WHERE Completed != 'Y' AND tabletMEID = ?)";
		PreparedStatement stmt = db.prepareStatement(SQLStatement);
		stmt.setString(1, MEID);
		stmt.execute();
		stmt.close();
		
		SQLStatement = "DELETE FROM FormData WHERE Completed != 'Y' AND tabletMEID = ?";
		stmt = db.prepareStatement(SQLStatement);
		stmt.setString(1, MEID);
		stmt.execute();
		stmt.close();
		}
	
	/**
	 * Allows insertion of records based on a Java Object. The java object's fields/attributes define the columns
	 * 	to use for database inserts.
	 * 
	 * @param tableName - Table Name to insert records into
	 * @param ObjectIdenfitier - The ID field of the object.
	 * 			If provided, prevents the column from being included in the insert statement. This causes the return value
	 * 			to provide a resolved new ID.
	 * @param list - Object List to insert into the database
	 * @param ObjectSubIdentifier - The sub ID that must be replaced with a value from PreResolvedIds
	 * @param PreResolvedIds - Map of already resolved IDs. Only effective if the ObjectSubIdentifier is declared
	 * @return - If an ID field is provided, returns a resolution of the old ID to the new ID
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 */
	private Map<Long, Long> ProcessObjectList(String tableName, String ObjectIdentifier, List<Object> list,
						String ObjectSubIdentifier, Map<Long, Long> PreResolvedIds)
			throws IllegalArgumentException, IllegalAccessException, SQLException
		{
		Map<Long, Long> resolvedIds = null;
		if( ObjectIdentifier != null )
			resolvedIds = new HashMap<Long, Long>();
		//else if( ObjectSubIdentifier == null )
		//	{
		//	PreparedStatement identityOn = db.prepareStatement("SET IDENTITY_INSERT " + tableName + " ON");
		//	identityOn.execute();
		//	identityOn.close();
		//	}
		
		int size = list.size();
		if( size > 0 )
			{
			Object o;
			Object attr;
			o = list.get(0);
			Field[] fields = o.getClass().getFields();
			
			String SQLStatement = "";
			if( ObjectIdentifier == null && ObjectSubIdentifier == null )
				SQLStatement += "SET IDENTITY_INSERT " + tableName + " ON ";
			
			SQLStatement += "INSERT INTO " + tableName + "(";
			String params = "";
			String fieldName;
			Field IDField = null;
			Field SubIDField = null;
			ResultSet results;
			long newId;
			for( int i = 0; i < fields.length; i++ )
				{
				fieldName = fields[i].getName();
				if( !(ObjectIdentifier != null && ObjectIdentifier.contentEquals(fieldName)) )
					{
					params += "?";
					SQLStatement += fields[i].getName();
					if( i < fields.length-1 )
						{
						params += ", ";
						SQLStatement += ", ";
						}
					}
				else
					IDField  = fields[i];
				
				if( ObjectSubIdentifier != null && ObjectSubIdentifier.contentEquals(fieldName) )
					{
					SubIDField = fields[i];
					}
				
				}
			SQLStatement += ") VALUES (" + params + ")";
			PreparedStatement stmt;
			if( ObjectIdentifier != null )
				stmt = db.prepareStatement(SQLStatement, PreparedStatement.RETURN_GENERATED_KEYS);
			else
				stmt = db.prepareStatement(SQLStatement);
			
			int cnt;
			Long PreResolvedId;
			for( int i = 0; i < size; i++ )
				{
				PreResolvedId = null;
				o = list.get(i);
				cnt = 1;
				for( int j = 0; j < fields.length; j++ )
					{
					fieldName = fields[j].getName();
					if( !(ObjectIdentifier != null && fields[j] == IDField) )
						{
						attr = fields[j].get(o);
						if( fields[j] == SubIDField )
							{
							if( ((Long)attr) > 0 )
								{
								PreResolvedId = (Long) attr;
								}
							else
								PreResolvedId = PreResolvedIds.get(attr);
							if( PreResolvedId != null )
								stmt.setLong(cnt++, PreResolvedId);
							}
						else if( attr instanceof Integer )
							stmt.setInt(cnt++, (Integer) attr);
						
						else if( attr instanceof Long )
							stmt.setLong(cnt++, (Long) attr);
						
						else if( attr instanceof String )
							stmt.setString(cnt++, (String) attr);
						
						else if( attr instanceof Float )
							stmt.setFloat(cnt++, (Float) attr);
						
						else if( attr instanceof Double )
							stmt.setDouble(cnt++, (Double) attr);
						else if( attr instanceof byte[] )
							stmt.setBytes(cnt++, (byte[]) attr);
						else if( attr == null )
							stmt.setNull(cnt++, Types.VARCHAR);
						}
					}
				
				if( ObjectIdentifier != null )
					{
					stmt.execute();
					results = stmt.getGeneratedKeys();
					if( !results.next() )
						throw new SQLException("Insert did not generate a new Id for " + tableName);
					
					newId = results.getLong(1);
					resolvedIds.put( (Long)IDField.get(o), newId );
					}
				else
					{
					if( PreResolvedId != null || ObjectSubIdentifier == null)
						stmt.execute();
					}
				}
			
			if( stmt != null )
				stmt.close();
			}
		
		if( ObjectSubIdentifier == null && ObjectIdentifier == null )
			{
			PreparedStatement identityOff = db.prepareStatement("SET IDENTITY_INSERT " + tableName + " OFF");
			identityOff.execute();
			identityOff.close();
			}
		
		return resolvedIds;
		}
	
	// ResolvedIds Insert
	private void InsertResolvedIdMap(String TableName, Map<?,?> map) throws SQLException
		{
		String sql = "DELETE FROM " + TableName + " WHERE tabletMEID = ? OR empno = ?";
		PreparedStatement stmt = db.prepareStatement(sql);
		stmt.setString(1, request.MEID);
		stmt.setString(2, empno);
		stmt.execute();
		
		sql = "INSERT INTO " + TableName + "( OldId, NewId, tabletMEID, empno) VALUES (?,?,?,?)";
		stmt = db.prepareStatement(sql);
		stmt.setString(3, request.MEID);
		stmt.setString(4, empno);
		
		Entry<?,?> entry;
		Object oldid, newid;
		for( Iterator<?> i = map.entrySet().iterator(); i.hasNext(); )
			{
			entry = (Entry<?, ?>) i.next();
			oldid = entry.getKey();
			newid = entry.getValue();
			if( oldid instanceof Integer )
				stmt.setInt(1, (Integer) oldid);
			else if( oldid instanceof Long )
				stmt.setLong(1, (Long) oldid);
			
			if( newid instanceof Integer )
				stmt.setInt(2, (Integer) newid);
			else if( newid instanceof Long )
				stmt.setLong(2, (Long) newid);
			
			stmt.execute();
			}
		
		stmt.close();
		}
	}

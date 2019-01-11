import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.twix.*;
import com.twix.ServiceAddress.pmAddressChecklist;

public class Twix_ServerResults
	{
	private Connection db;
	private Connection Therma_DW;
	private String empno;
	private ClientRequest request;
	private Prep_Statements prep_stmts;

	private Set<Integer> serviceAddressIds;
	private Set<Integer> dispatchIds;
	
	private Set<String> custIds;
	private Set<String> altBillTo;
	
	// Result Sets
	private ResultSet dispatch;
	private ResultSet serviceAddress;
	private ResultSet pmAddressChecklist;
	private ResultSet tenant;
	private ResultSet serviceAddressContact;
	private ResultSet serviceAddressNotes;
	private ResultSet equipment;
	private ResultSet fan;
	private ResultSet sheave;
	private ResultSet filter;
	private ResultSet refCircuit;
	private ResultSet compressor;
	
	private ResultSet openServiceTag;
	private ResultSet openServiceTagUnit;
	private ResultSet openServiceLabor;
	private ResultSet openServiceMaterial;
	private ResultSet openServiceRefrigerant;
	private ResultSet openPMChecklist;
	private ResultSet openBlue;
	private ResultSet openBlueUnit;
	private ResultSet openSafetyChecklist;
	private ResultSet openSafetyChecklistItem;
	
	private ResultSet serviceTag;
	private ResultSet serviceTagUnit;
	private ResultSet serviceLabor;
	private ResultSet serviceMaterial;
	private ResultSet serviceRefrigerant;
	private ResultSet closedBlue;
	private ResultSet closedBlueUnit;
	
	// The download package, available to the caller
	public Package_Download pkg;
	public FormPackage FormPkg;
	public FormDataPackage FormDataPkg;
	public AttributePackage AttrPkg;
	
	public Twix_ServerResults(Connection database, Connection datawarehouse, String employee, Prep_Statements stmts, ClientRequest req)
		{
		db = database;
		Therma_DW = datawarehouse;
		empno = employee;
		prep_stmts = stmts;
		request = req;
		
		serviceAddressIds = new TreeSet<Integer>();
		
		custIds = new TreeSet<String>();
		altBillTo = new TreeSet<String>();
		}
	
	// Sync build Response
	public void buildResonse() throws SQLException
		{
		pkg = new Package_Download();
		
		// Fetch the data from the database into result sets
		getOpenTags();
		getDispatch();
		getServiceAddress(null);
		
		// Build the response from the prefetched resultsets
		buildOpenTags();
		buildDispatch();
		buildClosedTags();
		buildServiceAddress();
		buildEquipment();
		
		Timestamp lastSync = getLastSync();
		buildStaticTables(lastSync);
		getAndBuildDispatchPriority(lastSync);
		setLastSync();
		
		getAndBuildBilling();
		
		// Build the Resolved Id Maps
		pkg.ResolvedServiceTagIds = GetIntResolvedIds("Resolved_OpenServiceTag");
		pkg.ResolvedServiceTagUnitIds = GetIntResolvedIds("Resolved_OpenServiceTagUnit");
		
		try
			{
			if( request.latestDates != null && request.latestDates.All_LatestDate != null )
				lastSync = Timestamp.valueOf(request.latestDates.All_LatestDate);
			else
				lastSync = null;
			}
		catch (Exception e)
			{
			lastSync = null;
			}
		
		getAndBuildPickLists(lastSync);
		getAndBuildFormPackage(lastSync);
		UpdateFormMEID(request.MEID, empno);
		getAndBuildAttributeStructure(lastSync);
		}
	
	public void buildResponse( ArrayList<Integer> fetchSAIds ) throws SQLException
		{
		pkg = new Package_Download();
		
		// Build Download Data out of the requested Service Address Ids
		serviceAddressIds.addAll(fetchSAIds);
		
		getServiceAddress(fetchSAIds);
		
		buildServiceAddress();
		buildEquipment();
		buildClosedTags();
		}
	
	public void buildDispatchResponse( int dispatchId ) throws SQLException
		{
		pkg = new Package_Download();
		getAndBuildDispatch(dispatchId);
		}
	
	/**
	 * ********** Data Fetch Functions ***********
	 */
	/**
	 * Fetches all the open service tags
	 * @throws SQLException
	 */
	private void getOpenTags() throws SQLException
		{
		// First, set this tablet as the new owner of this employees tags
		prep_stmts.update_meid.setString(1, request.MEID);
		prep_stmts.update_meid.setString(2, empno);
		prep_stmts.update_meid.execute();
		
		Set<Integer> openServiceTagIds = new TreeSet<Integer>();
		Set<Integer> serviceTagUnitIds = new TreeSet<Integer>();
		dispatchIds = new TreeSet<Integer>();
		Set<Integer> blueIds = new TreeSet<Integer>();
		
		//*****************************
		// Service Tag
		//*****************************
		PreparedStatement stmt = prep_stmts.openServiceTag_select;
		stmt.setString(1, empno);
		openServiceTag = stmt.executeQuery();
		while( openServiceTag.next() )
			{
			openServiceTagIds.add( openServiceTag.getInt(1) );
			serviceAddressIds.add( openServiceTag.getInt(2) );
			dispatchIds.add( openServiceTag.getInt(3) );
			}
		// Reset the ResultSet
		openServiceTag.beforeFirst();
		if( openServiceTagIds.size() > 0 )
			{
			//***** Build the ServiceTagUnit list once!
			String tagParams = buildParams(openServiceTagIds.size());
			
			
			//*****************************
			// Service Tag Unit
			//*****************************
			String sql = "SELECT serviceTagUnitId, serviceTagId, equipmentId, " +
					"servicePerformed, comments " +
				"FROM openServiceTagUnit WHERE serviceTagId IN ( " + tagParams + " ) " +
				"ORDER BY serviceTagId, serviceTagUnitId";
			stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			// Bind the parameters
			int index = 1;
			for( Iterator<Integer> i = openServiceTagIds.iterator(); i.hasNext(); )
				stmt.setInt(index++, i.next() );
			openServiceTagUnit = stmt.executeQuery();
			while( openServiceTagUnit.next() )
				{
				serviceTagUnitIds.add( openServiceTagUnit.getInt(1) );
				}
			// Restart the ResultSet
			openServiceTagUnit.beforeFirst();
			
			if( serviceTagUnitIds.size() > 0 )
				{
				//***** Build the ServiceTagUnit list once!
				String unitParams = buildParams(serviceTagUnitIds.size());
				
				//*****************************
				// Service Labor
				//*****************************
				sql = "SELECT serviceLaborId, serviceTagUnitId, serviceDate, " +
						"regHours, thHours, dtHours, mechanic, rate " +
					"FROM openServiceLabor WHERE serviceTagUnitId IN ( " + unitParams + " ) " +
					"ORDER BY serviceTagUnitId, serviceLaborId";
				stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				// Bind the parameters
				index = 1;
				for( Iterator<Integer> i = serviceTagUnitIds.iterator(); i.hasNext(); )
					stmt.setInt(index++, i.next() );
				openServiceLabor = stmt.executeQuery();
				
				//*****************************
				// Service Material
				//*****************************
				sql = "SELECT serviceMaterialId, serviceTagUnitId, quantity, " +
						"materialDesc, cost, refrigerantAdded, source " +
					"FROM openServiceMaterial WHERE serviceTagUnitId IN ( " + unitParams + " ) " +
					"ORDER BY serviceTagUnitId, serviceMaterialId";
				stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				// Bind the parameters
				index = 1;
				for( Iterator<Integer> i = serviceTagUnitIds.iterator(); i.hasNext(); )
					stmt.setInt(index++, i.next() );
				openServiceMaterial = stmt.executeQuery();
				
				//*****************************
				// Service Refrigerant
				//*****************************
				sql = "SELECT serviceRefrigerantId, serviceTagUnitId, transferDate, " +
						"techName,typeOfRefrigerant, amount, nameOfCylinder, cylinderSerialNo, transferedTo, serialNo, modelNo " +
					"FROM openServiceRefrigerant WHERE serviceTagUnitId IN ( " + unitParams + " ) " +
					"ORDER BY serviceTagUnitId, serviceRefrigerantId";
				stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				// Bind the parameters
				index = 1;
				for( Iterator<Integer> i = serviceTagUnitIds.iterator(); i.hasNext(); )
					stmt.setInt(index++, i.next() );
				openServiceRefrigerant = stmt.executeQuery();
				
				//*****************************
				// PM Checklist
				//*****************************
				sql = "SELECT pmChecklistId, serviceTagUnitId, itemText, " +
						"itemType, itemValue, itemComment, identifier " +
					"FROM openPMChecklist WHERE serviceTagUnitId IN ( " + unitParams + " ) " +
					"ORDER BY serviceTagUnitId, pmChecklistId";
				stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				// Bind the parameters
				index = 1;
				for( Iterator<Integer> i = serviceTagUnitIds.iterator(); i.hasNext(); )
					stmt.setInt(index++, i.next() );
				openPMChecklist = stmt.executeQuery();
				}
			
			//*****************************
			// Blue
			//*****************************
			sql = "SELECT blueId, serviceTagId, dateCreated " +
					"FROM openBlue WHERE serviceTagId IN ( " + tagParams + " ) " +
					"ORDER BY blueId";
			stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			// Bind the parameters
			index = 1;
			for( Iterator<Integer> i = openServiceTagIds.iterator(); i.hasNext(); )
				stmt.setInt(index++, i.next() );
			openBlue = stmt.executeQuery();
			while( openBlue.next() )
				{
				blueIds.add( openBlue.getInt(1) );
				}
			// Reset the ResultSet
			openBlue.beforeFirst();
			
			if( blueIds.size() > 0 )
				{
				//*****************************
				// Blue Units
				//*****************************
				sql = "SELECT blueUnitId, blueId, equipmentId, description, " +
						"materials, laborHours, tradesmenhrs, otherhrs, notes, completed, cost " +
					"FROM openBlueUnit WHERE blueId IN ( " + buildParams(blueIds.size()) + " ) " +
					"ORDER BY blueId, blueUnitId";
				stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				// Bind the parameters
				index = 1;
				for( Iterator<Integer> i = blueIds.iterator(); i.hasNext(); )
					stmt.setInt(index++, i.next() );
				openBlueUnit = stmt.executeQuery();
				}
			//*****************************
			// SafetyChecklist
			//*****************************
			sql = "SELECT serviceTagId, checklistDate, comments " +
					"FROM openSafetyTagChecklist WHERE serviceTagid IN ( " + tagParams + " ) " +
					"ORDER BY serviceTagId";
			stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			// Bind the parameters
			index = 1;
			for( Iterator<Integer> i = openServiceTagIds.iterator(); i.hasNext(); )
				stmt.setInt(index++, i.next() );
			openSafetyChecklist = stmt.executeQuery();
			
			//*****************************
			// SafetyChecklistItem
			//*****************************
			sql = "SELECT serviceTagId, safetyChecklistId, itemRequired, itemValue " +
					"FROM openSafetyTagChecklistItem WHERE serviceTagid IN ( " + tagParams + " ) " +
					"ORDER BY serviceTagId, safetyChecklistId";
			stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			// Bind the parameters
			index = 1;
			for( Iterator<Integer> i = openServiceTagIds.iterator(); i.hasNext(); )
				stmt.setInt(index++, i.next() );
			openSafetyChecklistItem = stmt.executeQuery();
			}
		}
	
	private void getDispatch() throws SQLException
		{
		String sql = "SELECT dispatchId, serviceAddressId," +
				"batchNo, jobNo, LTRIM(RTRIM(cusNo)), LTRIM(RTRIM(altBillTo)), contractType, " +
				"dateStarted, dateEnded, dateOrdered, customerPO, requestedBy, " +
				"requestedByPhone, requestedByEmail, siteContact, siteContactPhone, " +
				"description, mechanic1, mechanic2, siteName, mechanic3, " +
				"mechanic4, mechanic5, mechanic6, mechanic7, status, tenant, PMComments, PMEstTime " +
			"FROM dispatch " +
			"WHERE (";
		//	Boolean Logic
		//
		//	A - Mechanic Assigned
		//	B - Mechanic Not Assigned
		//	C - Status 'A' or 'O'
		//	D - ServiceAddressId not ''
		//	E - ServiceAddressId not null
		//	F - DispatchId in OpenTag Query
		//
		//	( ((A or B) and C) or F ) and (C and D)
		//
		sql +=	"( ( (dispatch.mechanic1 = ? " +
				"OR dispatch.mechanic2 = ? " +
				"OR dispatch.mechanic3 = ? " +
				"OR dispatch.mechanic4 = ? " +
				"OR dispatch.mechanic5 = ? " +
				"OR dispatch.mechanic6 = ? " +
				"OR dispatch.mechanic7 = ?) " +
			"OR ( ISNULL(SUBSTRING(mechanic1,5,6),'')='' " +
				"AND ISNULL(SUBSTRING(mechanic2,5,6),'')='' " +
				"AND ISNULL(SUBSTRING(mechanic3,5,6),'')='' " +
				"AND ISNULL(SUBSTRING(mechanic4,5,6),'')='' " +
				"AND ISNULL(SUBSTRING(mechanic5,5,6),'')='' " +
				"AND ISNULL(SUBSTRING(mechanic6,5,6),'')='' " +
				"AND ISNULL(SUBSTRING(mechanic7,5,6),'')='') ) " +
			"AND ( status = 'O' OR status = 'A') )"; // End ((A or B) and C)
		// Make sure we include all dispatchIds that are linked to tags
		sql += " OR dispatchId IN ( SELECT dispatchId FROM openServiceTag WHERE empno = ? ) ";// Add " or F )"
				
		sql += // Parenthesis ends the starting where, which or may not include the dispatchId IN clause
			") AND (serviceAddressId <> '' " +
			"AND serviceAddressId IS NOT NULL)"; // Add " and (C and D)" 
		
			
		PreparedStatement stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		// Bind the Parameters
		int index = 1;
		
		// Mechanic 1-7
		stmt.setString(index++, empno);	//1
		stmt.setString(index++, empno);	//2
		stmt.setString(index++, empno);	//3
		stmt.setString(index++, empno);	//4
		stmt.setString(index++, empno);	//5
		stmt.setString(index++, empno);	//6
		stmt.setString(index++, empno);	//7
		stmt.setString(index++, empno);	//Open Service Tag
		
		dispatch = stmt.executeQuery();
		
		String cusNo, altBilling;
		while( dispatch.next() )
			{
			serviceAddressIds.add( dispatch.getInt(2) );
			
			cusNo = dispatch.getString(5);
			if( cusNo != null )
				custIds.add( cusNo );
			
			altBilling = dispatch.getString(6);
			if( altBilling != null )
				altBillTo.add( altBilling );
			}
		// Result the ResultSet
		dispatch.beforeFirst();
		}
	
	/**
	 * Builds a Select Statement for fetching the service address Ids from the
	 * 	dispatch table, applicable to the user's needs
	 * 
	 * @return the Select Statement string
	 */
	private String getDispatchInnerSelect()
		{
		String ret = "SELECT serviceAddressId FROM dispatch WHERE " +
				"( " +
					"( " +
						"( " +
							"( " +
							"dispatch.mechanic1 = ? " +
							"OR dispatch.mechanic2 = ? " +
							"OR dispatch.mechanic3 = ? " +
							"OR dispatch.mechanic4 = ? " +
							"OR dispatch.mechanic5 = ? " +
							"OR dispatch.mechanic6 = ? " +
							"OR dispatch.mechanic7 = ? " +
							") " +
						" OR " +
							"( " +
							"ISNULL(SUBSTRING(mechanic1,5,6),'')='' " +
							"AND ISNULL(SUBSTRING(mechanic2,5,6),'')='' " +
							"AND ISNULL(SUBSTRING(mechanic3,5,6),'')='' " +
							"AND ISNULL(SUBSTRING(mechanic4,5,6),'')='' " +
							"AND ISNULL(SUBSTRING(mechanic5,5,6),'')='' " +
							"AND ISNULL(SUBSTRING(mechanic6,5,6),'')='' " +
							"AND ISNULL(SUBSTRING(mechanic7,5,6),'')='' " +
							") " +
						") " +
					"AND ( status = 'O' OR status = 'A') " +
					") " +
					
				"OR dispatchId IN ( SELECT dispatchId FROM openServiceTag WHERE empno = ? ) " +
				") " +
					
				"AND (serviceAddressId <> '' " +
					"AND serviceAddressId IS NOT NULL " +
				") " +
				
				"UNION ALL " +
				"SELECT serviceAddressId FROM openServiceTag WHERE empno = ? " +
					"AND (serviceAddressId <> '' AND serviceAddressId IS NOT NULL)";
		
		return ret;
		}
	
	private void getClosedTags(String SAInnerSelect, ArrayList<Integer> fetchSAIds) throws SQLException
		{
		if( serviceAddressIds.size() > 0 )
			{
			Set<Integer> serviceTagIds = new TreeSet<Integer>();
			Set<Integer> serviceTagUnitIds = new TreeSet<Integer>();
			
			String sql = "SELECT st.serviceTagId, st.serviceAddressId, st.dispatchId, " +
					"d.contractType, st.serviceDate, " +
					"st.billTo, st.billAddress1, st.billAddress2, st.billAddress3, st.billAddress4, st.billAttn, " +
					"COALESCE(d.tenant, st.tenant), " +
					"COALESCE(d.batchNo, st.batchNo), " +
					"COALESCE(d.jobNo, st.jobNo), " +
					"st.empno, st.disposition, d.description , st.xoi_flag " +
				"FROM serviceTag as st " +
					"LEFT OUTER JOIN dispatch as d ON st.dispatchId = d.dispatchId " +
					"LEFT OUTER JOIN serviceAddress as sa ON d.serviceAddressId = sa.serviceAddressId " +
				"WHERE st.serviceAddressId IN ( " + SAInnerSelect + " ) " +
					"ORDER BY st.serviceTagId";
			PreparedStatement stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			int index = 1;
			//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
			//	stmt.setInt(index++, i.next() );
			
			if( fetchSAIds == null )
				{
				// Mechanic 1-7
				stmt.setString(index++, empno);	//1
				stmt.setString(index++, empno);	//2
				stmt.setString(index++, empno);	//3
				stmt.setString(index++, empno);	//4
				stmt.setString(index++, empno);	//5
				stmt.setString(index++, empno);	//6
				stmt.setString(index++, empno);	//7
				stmt.setString(index++, empno);	//Open Service Tag
				stmt.setString(index++, empno);	//Open Service Tag 2
				}
			else
				{
				// Site Download Request
				int size = fetchSAIds.size();
				for( int i = 0; i < size; i++ )
					stmt.setInt(index++, fetchSAIds.get(i) );
				}
			
			serviceTag = stmt.executeQuery();
			while( serviceTag.next() )
				{
				serviceTagIds.add( serviceTag.getInt(1) );
				}
			// Reset the ResultSet
			serviceTag.beforeFirst();
			
			if( serviceTagIds.size() > 0 )
				{
				String STInnerSelect = "SELECT servicetagId FROM servicetag WHERE serviceAddressId IN ( " +
							SAInnerSelect + " )";
				
				// Service Tag Unit
				sql = "SELECT serviceTagUnitId, serviceTagId, equipmentId, " +
						"servicePerformed, comments " +
					"FROM serviceTagUnit WHERE serviceTagId IN ( " + STInnerSelect + " ) " +
					"ORDER BY serviceTagId, serviceTagUnitId";
				stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				index = 1;
				//for( Iterator<Integer> i = serviceTagIds.iterator(); i.hasNext(); )
				//	stmt.setInt(index++, i.next() );
				
				if( fetchSAIds == null )
					{
					// Mechanic 1-7
					stmt.setString(index++, empno);	//1
					stmt.setString(index++, empno);	//2
					stmt.setString(index++, empno);	//3
					stmt.setString(index++, empno);	//4
					stmt.setString(index++, empno);	//5
					stmt.setString(index++, empno);	//6
					stmt.setString(index++, empno);	//7
					stmt.setString(index++, empno);	//Open Service Tag
					stmt.setString(index++, empno);	//Open Service Tag 2
					}
				else
					{
					// Site Download Request
					int size = fetchSAIds.size();
					for( int i = 0; i < size; i++ )
						stmt.setInt(index++, fetchSAIds.get(i) );
					}
				serviceTagUnit = stmt.executeQuery();
				while( serviceTagUnit.next() )
					{
					serviceTagUnitIds.add( serviceTagUnit.getInt(1) );
					}
				// Result the ResultSet
				serviceTagUnit.beforeFirst();
				
				if( serviceTagUnitIds.size() > 0 )
					{
					String SUInnerSelect = "SELECT serviceTagUnitId FROM serviceTagUnit WHERE serviceTagId IN (" + 
							STInnerSelect + " )";
					
					// Prebuild the serviceTagUnitId param count
					//String unitParams = buildParams( serviceTagUnitIds.size() );
					
					// Service Labor
					sql = "SELECT serviceLaborId, serviceTagUnitId, serviceDate, " +
							"regHours, thHours, dtHours, mechanic, rate " +
						"FROM serviceLabor WHERE serviceTagUnitId IN ( " + SUInnerSelect + " ) " +
						"ORDER BY serviceTagUnitId, serviceLaborId";
					stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					index = 1;
					//for( Iterator<Integer> i = serviceTagUnitIds.iterator(); i.hasNext(); )
					//	stmt.setInt(index++, i.next() );
					
					if( fetchSAIds == null )
						{
						// Mechanic 1-7
						stmt.setString(index++, empno);	//1
						stmt.setString(index++, empno);	//2
						stmt.setString(index++, empno);	//3
						stmt.setString(index++, empno);	//4
						stmt.setString(index++, empno);	//5
						stmt.setString(index++, empno);	//6
						stmt.setString(index++, empno);	//7
						stmt.setString(index++, empno);	//Open Service Tag
						stmt.setString(index++, empno);	//Open Service Tag 2
						}
					else
						{
						// Site Download Request
						int size = fetchSAIds.size();
						for( int i = 0; i < size; i++ )
							stmt.setInt(index++, fetchSAIds.get(i) );
						}
					serviceLabor = stmt.executeQuery();
					
					// Service Material
					sql = "SELECT serviceMaterialId, serviceTagUnitId, quantity, " +
							"materialDesc, cost, refrigerantAdded, source " +
						"FROM serviceMaterial WHERE serviceTagUnitId IN ( " + SUInnerSelect + " ) " +
						"ORDER BY serviceTagUnitId, serviceMaterialId";
					stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					index = 1;
					//for( Iterator<Integer> i = serviceTagUnitIds.iterator(); i.hasNext(); )
					//	stmt.setInt(index++, i.next() );
					if( fetchSAIds == null )
						{
						// Mechanic 1-7
						stmt.setString(index++, empno);	//1
						stmt.setString(index++, empno);	//2
						stmt.setString(index++, empno);	//3
						stmt.setString(index++, empno);	//4
						stmt.setString(index++, empno);	//5
						stmt.setString(index++, empno);	//6
						stmt.setString(index++, empno);	//7
						stmt.setString(index++, empno);	//Open Service Tag
						stmt.setString(index++, empno);	//Open Service Tag 2
						}
					else
						{
						// Site Download Request
						int size = fetchSAIds.size();
						for( int i = 0; i < size; i++ )
							stmt.setInt(index++, fetchSAIds.get(i) );
						}
					serviceMaterial = stmt.executeQuery();
					
					// Service Refrigerant
					sql = "SELECT serviceRefrigerantId, serviceTagUnitId, transferDate, " +
							"techName, typeOfRefrigerant, amount, nameOfCylinder, cylinderSerialNo, transferedTo, serialNo, modelNo " +
						"FROM serviceRefrigerant WHERE serviceTagUnitId IN ( " + SUInnerSelect + " ) " +
						"ORDER BY serviceTagUnitId, serviceRefrigerantId";
					stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					index = 1;
					//for( Iterator<Integer> i = serviceTagUnitIds.iterator(); i.hasNext(); )
					//	stmt.setInt(index++, i.next() );
					if( fetchSAIds == null )
						{
						// Mechanic 1-7
						stmt.setString(index++, empno);	//1
						stmt.setString(index++, empno);	//2
						stmt.setString(index++, empno);	//3
						stmt.setString(index++, empno);	//4
						stmt.setString(index++, empno);	//5
						stmt.setString(index++, empno);	//6
						stmt.setString(index++, empno);	//7
						stmt.setString(index++, empno);	//Open Service Tag
						stmt.setString(index++, empno);	//Open Service Tag 2
						}
					else
						{
						// Site Download Request
						int size = fetchSAIds.size();
						for( int i = 0; i < size; i++ )
							stmt.setInt(index++, fetchSAIds.get(i) );
						}
					serviceRefrigerant = stmt.executeQuery();
					}
				}
			
			}
		}
	
	private void getServiceAddress(ArrayList<Integer> fetchSAIds) throws SQLException
		{
		if( serviceAddressIds.size() > 0 )
			{
			String SAInnerSelect;
			if( fetchSAIds == null )
				SAInnerSelect = getDispatchInnerSelect();
			else
				SAInnerSelect = buildParams(fetchSAIds.size());
			
			// Service Address
			//String saParams = buildParams(serviceAddressIds.size());
			String sql = "SELECT serviceAddressId, siteName, " +
					"address1, address2, city, state, zip, " +
					"buildingNo, note " +
				"FROM serviceAddress " +
				"WHERE serviceAddressId IN ( " + SAInnerSelect + " ) " +
				"ORDER BY serviceAddressId";
			PreparedStatement stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			int index = 1;
			//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
			//	stmt.setInt(index++, i.next() );
			if( fetchSAIds == null )
				{
				// Mechanic 1-7
				stmt.setString(index++, empno);	//1
				stmt.setString(index++, empno);	//2
				stmt.setString(index++, empno);	//3
				stmt.setString(index++, empno);	//4
				stmt.setString(index++, empno);	//5
				stmt.setString(index++, empno);	//6
				stmt.setString(index++, empno);	//7
				stmt.setString(index++, empno);	//Open Service Tag
				stmt.setString(index++, empno);	//Open Service Tag 2
				}
			else
				{
				// Site Download Request
				int size = fetchSAIds.size();
				for( int i = 0; i < size; i++ )
					stmt.setInt(index++, fetchSAIds.get(i) );
				}
			serviceAddress = stmt.executeQuery();
			
			getClosedTags(SAInnerSelect, fetchSAIds);
			getEquipment(SAInnerSelect, fetchSAIds);
			getServiceAddressContact(SAInnerSelect, fetchSAIds);
			getServiceAddressNotes(SAInnerSelect, fetchSAIds);
			getServiceAddressTenant(SAInnerSelect, fetchSAIds);
			getClosedBlues(SAInnerSelect, fetchSAIds);
			
			// PM Address Checklist
			sql = "SELECT pmChecklistId, serviceAddressId, " +
					"equipmentCategoryId, itemType, itemText, identifier " +
				"FROM pmAddressChecklist " +
				"WHERE serviceAddressId IN ( " + SAInnerSelect + " ) " +
				"ORDER BY serviceAddressId, pmChecklistId";
			stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			index = 1;
			//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
			//	stmt.setInt(index++, i.next() );
			
			if( fetchSAIds == null )
				{
				// Mechanic 1-7
				stmt.setString(index++, empno);	//1
				stmt.setString(index++, empno);	//2
				stmt.setString(index++, empno);	//3
				stmt.setString(index++, empno);	//4
				stmt.setString(index++, empno);	//5
				stmt.setString(index++, empno);	//6
				stmt.setString(index++, empno);	//7
				stmt.setString(index++, empno);	//Open Service Tag
				stmt.setString(index++, empno);	//Open Service Tag 2
				}
			else
				{
				// Site Download Request
				int size = fetchSAIds.size();
				for( int i = 0; i < size; i++ )
					stmt.setInt(index++, fetchSAIds.get(i) );
				}
			
			pmAddressChecklist = stmt.executeQuery();
			}
		}
	
	private void getEquipment(String SAInnerSelect, ArrayList<Integer> fetchSAIds) throws SQLException
		{
		// Equipment Inner Select
		//String equipInner = "SELECT equipmentId FROM equipment WHERE serviceAddressId IN ( " + saParams + " )";
		
		// Equipment
		String sql =
				"SELECT equipmentId, equipmentCategoryId," +
					"serviceAddressId, unitNo, barCodeNo, manufacturer, model, " +
					"productIdentifier, serialNo, voltage, economizer, capacity, " +
					"capacityUnits, refrigerantType, areaServed, mfgYear, " +
					"dateInService, dateOutService, notes, verifiedByEmpno " +
				"FROM equipment WHERE serviceAddressId IN ( " + SAInnerSelect + " )";
		PreparedStatement stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		int index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		equipment = stmt.executeQuery();
		
		String EQInnerSelect = "SELECT equipmentId FROM equipment WHERE serviceAddressId IN ( " + 
				SAInnerSelect + " )";
		
		// Fan & Belt
		sql = "SELECT fan.fanId, fan.equipmentId, fan.partType, fan.number, belt.beltSize, belt.quantity " +
				"FROM fan " +
				"LEFT OUTER JOIN belt " +
					"ON belt.fanId = fan.fanId " +
				"WHERE fan.equipmentId IN ( " + EQInnerSelect + " )";
		
		stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		fan = stmt.executeQuery();
		
			// Sheave
		sql = "SELECT fanId, type, number, manufacturer " +
				"FROM sheave WHERE fanId IN ( SELECT fan.fanId FROM fan WHERE fan.equipmentId IN ( " + EQInnerSelect + " ) ) " +
				"ORDER BY fanId";
		stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		sheave = stmt.executeQuery();
		
		// Filter
		sql = "SELECT equipmentId, type, quantity, filterSize " +
				"FROM filter WHERE equipmentId IN ( " + EQInnerSelect + " ) " +
				"ORDER BY equipmentId";
		stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		filter = stmt.executeQuery();
		
		// RefCircuit
		sql = "SELECT circuitId, equipmentId, circuitNo, lbsRefrigerant " +
				"FROM refCircuit WHERE equipmentId IN ( " + EQInnerSelect + " ) " +
				"ORDER BY equipmentId, circuitId";
		stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		refCircuit = stmt.executeQuery();
		
			// Compressor
		sql = "SELECT circuitId, compressorNo, manufacturer, model, serialNo, dateInService, dateOutService " +
				"FROM compressor WHERE circuitId IN ( SELECT circuitId FROM refCircuit WHERE equipmentId IN (" + EQInnerSelect + ") ) " +
				"ORDER BY circuitId";
		stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		compressor = stmt.executeQuery();
		}
	
	private void getClosedBlues(String SAInnerSelect, ArrayList<Integer> fetchSAIds) throws SQLException
		{
		String blueSelect = "SELECT b.blueId FROM blue as b " +
								"LEFT OUTER JOIN serviceTag as st " + 
									"ON st.serviceTagId = b.serviceTagId " +
								"LEFT OUTER JOIN serviceAddress as sa " + 
									"ON sa.serviceAddressId = st.serviceAddressId " +
							"WHERE sa.serviceAddressId IN ( " + SAInnerSelect + " )";
		
		String sql =
				"SELECT b.blueId, b.serviceTagId, b.dateCreated " +
				"FROM blue as b " +
					"LEFT OUTER JOIN serviceTag as st " + 
						"ON st.serviceTagId = b.serviceTagId " +
					"LEFT OUTER JOIN serviceAddress as sa " + 
						"ON sa.serviceAddressId = st.serviceAddressId " +
				"WHERE sa.serviceAddressId IN ( " + SAInnerSelect + " )";
		PreparedStatement stmt = db.prepareStatement(sql);
		int index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		closedBlue = stmt.executeQuery();
		
		sql = "SELECT bu.blueUnitId, bu.blueId, bu.equipmentId, bu.description, " +
					"bu.materials, bu.laborHours, bu.tradesmenhrs, bu.otherhrs, bu.notes, bu.cost " +
				"FROM blueUnit as bu where blueId IN ( " + blueSelect + " )";
		stmt = db.prepareStatement(sql);
		index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		closedBlueUnit = stmt.executeQuery();
		}
	
	private void getServiceAddressContact(String SAInnerSelect, ArrayList<Integer> fetchSAIds) throws SQLException
		{
		String sql = "SELECT contactId, serviceAddressId," +
				"contactName, phone1, phone1Type, phone2, phone2Type, " +
				"email, contactType, ext1, ext2 " +
				"FROM serviceAddressContact WHERE serviceAddressId IN ( " + SAInnerSelect + " ) ";
		PreparedStatement stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		int index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		serviceAddressContact = stmt.executeQuery();
		}
	private void getServiceAddressNotes(String SAInnerSelect, ArrayList<Integer> fetchSAIds) throws SQLException
		{
		String sql = "SELECT noteid, serviceaddressid," +
				"notes " +
				"FROM notes WHERE serviceaddressid IN ( " + SAInnerSelect + " ) ";
		PreparedStatement stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		int index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		serviceAddressNotes = stmt.executeQuery();
		}
	
	private void getServiceAddressTenant( String SAInnerSelect, ArrayList<Integer> fetchSAIds) throws SQLException
		{
		String sql = "SELECT tenantId, serviceAddressId, tenant " +
				"FROM serviceAddressTenant " +
				"WHERE serviceAddressId IN ( " + SAInnerSelect + " ) " +
				"ORDER BY serviceAddressId";
		PreparedStatement stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		int index = 1;
		//for( Iterator<Integer> i = serviceAddressIds.iterator(); i.hasNext(); )
		//	stmt.setInt(index++, i.next() );
		
		if( fetchSAIds == null )
			{
			// Mechanic 1-7
			stmt.setString(index++, empno);	//1
			stmt.setString(index++, empno);	//2
			stmt.setString(index++, empno);	//3
			stmt.setString(index++, empno);	//4
			stmt.setString(index++, empno);	//5
			stmt.setString(index++, empno);	//6
			stmt.setString(index++, empno);	//7
			stmt.setString(index++, empno);	//Open Service Tag
			stmt.setString(index++, empno);	//Open Service Tag 2
			}
		else
			{
			// Site Download Request
			int size = fetchSAIds.size();
			for( int i = 0; i < size; i++ )
				stmt.setInt(index++, fetchSAIds.get(i) );
			}
		tenant = stmt.executeQuery();
		}
	
	/**
	 * ********** Data Build Functions ***********
	 */
	private void buildOpenTags() throws SQLException
		{
		int serviceTagId;
		int index;
		
		OpenServiceTag tag;
		
		while( openServiceTag.next() )
			{
			index = 1;
			serviceTagId = openServiceTag.getInt(index++);
			tag = new OpenServiceTag();
			tag.serviceTagId = serviceTagId;
			tag.serviceAddressId	= openServiceTag.getInt(index++);
			tag.dispatchId			= openServiceTag.getInt(index++);
			tag.serviceType			= openServiceTag.getString(index++);
			tag.serviceDate			= openServiceTag.getString(index++);
			tag.billTo				= openServiceTag.getString(index++);
			tag.billAddress1		= openServiceTag.getString(index++);
			tag.billAddress2		= openServiceTag.getString(index++);
			tag.billAddress3		= openServiceTag.getString(index++);
			tag.billAddress4		= openServiceTag.getString(index++);
			tag.billAttn			= openServiceTag.getString(index++);
			tag.siteName			= openServiceTag.getString(index++);
			tag.tenant				= openServiceTag.getString(index++);
			tag.address1			= openServiceTag.getString(index++);
			tag.address2			= openServiceTag.getString(index++);
			tag.city				= openServiceTag.getString(index++);
			tag.state				= openServiceTag.getString(index++);
			tag.zip					= openServiceTag.getString(index++);
			tag.buildingNo			= openServiceTag.getString(index++);
			tag.note				= openServiceTag.getString(index++);
			tag.batchNo				= openServiceTag.getString(index++);
			tag.jobNo				= openServiceTag.getString(index++);
			tag.empno				= openServiceTag.getString(index++);
			tag.tabletMEID			= request.MEID; index++;
			tag.disposition			= openServiceTag.getString(index++);
			tag.xoi_flag			= openServiceTag.getString(index++);
			// Add the Open Tag to the download package
			pkg.openServiceTag.add(tag);
			}
		
		buildOpenServiceTagUnits();
		buildOpenBlue();
		buildOpenSafetyChecklist();
		}
	
		// Open Service Tag Unit Details
	private void buildOpenServiceTagUnits() throws SQLException
		{
		if( openServiceTagUnit == null )
			return;
		
		int index;
		ServiceTagUnit unit;
		
		while( openServiceTagUnit.next() )
			{
			index = 1;
			unit = new ServiceTagUnit();
			
			unit.serviceTagUnitId	= openServiceTagUnit.getInt(index++);
			unit.serviceTagId		= openServiceTagUnit.getInt(index++);
			unit.equipmentId		= openServiceTagUnit.getInt(index++);
			unit.servicePerformed	= openServiceTagUnit.getString(index++);
			unit.comments			= openServiceTagUnit.getString(index++);
			
			pkg.openServiceTagUnit.add(unit);
			}
		
		buildOpenLabor();
		buildOpenMaterial();
		buildOpenRefrigerant();
		buildOpenPMChecklist();
		}
	
	private void buildOpenLabor() throws SQLException
		{
		if( openServiceLabor == null )
			return;
		
		ServiceLabor labor;
		
		while( openServiceLabor.next() )
			{
			labor = new ServiceLabor();
			labor.serviceLaborId	= openServiceLabor.getInt(1);
			labor.serviceTagUnitId	= openServiceLabor.getInt(2);
			labor.serviceDate		= openServiceLabor.getString(3);
			labor.regHours			= openServiceLabor.getFloat(4);
			labor.thHours			= openServiceLabor.getFloat(5);
			labor.dtHours			= openServiceLabor.getFloat(6);
			labor.mechanic			= openServiceLabor.getString(7);
			labor.rate				= openServiceLabor.getString(8);
			pkg.openServiceLabor.add(labor);
			}
		
		}
	
	private void buildOpenMaterial() throws SQLException
		{
		if( openServiceMaterial == null )
			return;
		
		ServiceMaterial material;
		
		while( openServiceMaterial.next() )
			{
			material = new ServiceMaterial();
			material.serviceMaterialId	= openServiceMaterial.getInt(1);
			material.serviceTagUnitId	= openServiceMaterial.getInt(2);
			material.quantity			= openServiceMaterial.getFloat(3);
			material.materialDesc		= openServiceMaterial.getString(4);
			material.cost				= openServiceMaterial.getFloat(5);
			material.refrigerantAdded	= openServiceMaterial.getString(6);
			material.source				= openServiceMaterial.getString(7);
			
			pkg.openServiceMaterial.add(material);
			}
		
		}
	
	private void buildOpenRefrigerant() throws SQLException
		{
		if( openServiceRefrigerant == null )
			return;
		
		ServiceRefrigerant refrigerant;
		
		while( openServiceRefrigerant.next() )
			{
			refrigerant = new ServiceRefrigerant();
			refrigerant.serviceRefrigerantId	= openServiceRefrigerant.getInt(1);
			refrigerant.serviceTagUnitId	= openServiceRefrigerant.getInt(2);
			refrigerant.transferDate			= openServiceRefrigerant.getString(3);
			refrigerant.techName		= openServiceRefrigerant.getString(4);
			refrigerant.typeOfRefrigerant				= openServiceRefrigerant.getString(5);
			refrigerant.amount	= openServiceRefrigerant.getFloat(6);
			refrigerant.nameOfCylinder				= openServiceRefrigerant.getString(7);
			refrigerant.cylinderSerialNo				= openServiceRefrigerant.getString(8);
			refrigerant.transferedTo				= openServiceRefrigerant.getString(9);
			refrigerant.serialNo				= openServiceRefrigerant.getString(10);
			refrigerant.modelNo				= openServiceRefrigerant.getString(11);
			pkg.openServiceRefrigerant.add(refrigerant);
			}
		
		}
	
	private void buildOpenPMChecklist() throws SQLException
		{
		if( openPMChecklist == null )
			return;
		
		PMChecklist pmChecklist;
		
		while( openPMChecklist.next() )
			{
			pmChecklist = new PMChecklist();
			pmChecklist.pmChecklistId	= openPMChecklist.getInt(1);
			pmChecklist.serviceTagUnitId= openPMChecklist.getInt(2);
			pmChecklist.itemText		= openPMChecklist.getString(3);
			pmChecklist.itemType		= openPMChecklist.getString(4);
			pmChecklist.itemValue		= openPMChecklist.getString(5);
			pmChecklist.itemComment		= openPMChecklist.getString(6);
			pmChecklist.identifier		= openPMChecklist.getString(7);
			
			pkg.openPMChecklist.add(pmChecklist);
			}
		}
	
		// Open Blue Details
	private void buildOpenBlue() throws SQLException
		{
		if( openBlue == null )
			return;
		
		OpenBlue blue;
		OpenBlueUnit unit;
		
		while( openBlue.next() )
			{
			blue = new OpenBlue();
			blue.blueId				= openBlue.getInt(1);
			blue.serviceTagId 		= openBlue.getInt(2);
			blue.dateCreated		= openBlue.getString(3);
			
			pkg.openBlue.add(blue);
			}
		
		if( openBlueUnit == null )
			return;
		
		while( openBlueUnit.next() )
			{
			unit = new OpenBlueUnit();
			unit.blueUnitId		= openBlueUnit.getInt(1);
			unit.blueId			= openBlueUnit.getInt(2);
			unit.equipmentId	= openBlueUnit.getInt(3);
			unit.description	= openBlueUnit.getString(4);
			unit.materials		= openBlueUnit.getString(5);
			unit.laborHours		= openBlueUnit.getFloat(6);
			unit.tradesmenhrs	= openBlueUnit.getFloat(7);
			unit.otherhrs		= openBlueUnit.getFloat(8);
			unit.notes			= openBlueUnit.getString(9);
			unit.completed		= openBlueUnit.getString(10);
			unit.cost			= openBlueUnit.getFloat(11);
			
			pkg.openBlueUnit.add(unit);
			}
			
		}
		// Open SafetyChecklist Details
	private void buildOpenSafetyChecklist() throws SQLException
		{
		if( openSafetyChecklist == null )
			return;
		OpenSafetyTagChecklist checklist;
		while( openSafetyChecklist.next() )
			{
			checklist = new OpenSafetyTagChecklist();
			checklist.serviceTagId 		= openSafetyChecklist.getInt(1);
			checklist.checkListDate		= openSafetyChecklist.getString(2);
			checklist.comments			= openSafetyChecklist.getString(3);
			
			pkg.SafetyTagChecklist.add(checklist);
			}
		
		if( openSafetyChecklistItem == null )
			return;
		ChecklistItem item;
		while( openSafetyChecklistItem.next() )
			{
			item = new ChecklistItem();
			item.serviceTagId		= openSafetyChecklistItem.getInt(1);
			item.safetyChecklistId	= openSafetyChecklistItem.getInt(2);
			item.itemRequired		= openSafetyChecklistItem.getString(3);
			item.itemValue			= openSafetyChecklistItem.getString(4);
			
			pkg.SafetyTagChecklistItem.add(item);
			}
		
		}
	
	
	private void buildDispatch() throws SQLException
		{
		if( dispatch == null )
			return;
		
		int index;
		Dispatch d;
		
		while( dispatch.next() )
			{
			index = 1;
			d = new Dispatch();
			d.dispatchId			= dispatch.getInt(index++);
			d.serviceAddressId		= dispatch.getInt(index++);
			d.batchNo				= dispatch.getString(index++);
			d.jobNo					= dispatch.getString(index++);
			d.cusNo					= dispatch.getString(index++);
			d.altBillTo				= dispatch.getString(index++);
			d.contractType			= dispatch.getString(index++);
			d.dateStarted			= dispatch.getString(index++);
			d.dateEnded				= dispatch.getString(index++);
			d.dateOrdered			= dispatch.getString(index++);
			d.customerPO			= dispatch.getString(index++);
			d.requestedBy			= dispatch.getString(index++);
			d.requestedByPhone		= dispatch.getString(index++);
			d.requestedByEmail		= dispatch.getString(index++);
			d.siteContact			= dispatch.getString(index++);
			d.siteContactPhone		= dispatch.getString(index++);
			d.description			= dispatch.getString(index++);
			d.mechanic1				= dispatch.getString(index++);
			d.mechanic2				= dispatch.getString(index++);
			d.siteName				= dispatch.getString(index++);
			d.mechanic3				= dispatch.getString(index++);
			d.mechanic4				= dispatch.getString(index++);
			d.mechanic5				= dispatch.getString(index++);
			d.mechanic6				= dispatch.getString(index++);
			d.mechanic7				= dispatch.getString(index++);
			d.status				= dispatch.getString(index++);
			d.tenant				= dispatch.getString(index++);
			d.PMComments			= dispatch.getString(index++);
			d.PMEstTime				= dispatch.getString(index++);
			
			// Add the Service Tag to the download package
			pkg.dispatch.add(d);
			}
		}
	
	private void buildClosedTags() throws SQLException
		{
		if( serviceTag == null )
			return;
		
		int serviceTagId;
		int index;
		
		ServiceTag tag;
		
		while( serviceTag.next() )
			{
			index = 1;
			serviceTagId = serviceTag.getInt(index++);
			tag = new ServiceTag();
			tag.serviceTagId = serviceTagId;
			tag.serviceAddressId	= serviceTag.getInt(index++);
			tag.dispatchId			= serviceTag.getInt(index++);
			tag.serviceType			= serviceTag.getString(index++);
			tag.serviceDate			= serviceTag.getString(index++);
			tag.billTo				= serviceTag.getString(index++);
			tag.billAddress1		= serviceTag.getString(index++);
			tag.billAddress2		= serviceTag.getString(index++);
			tag.billAddress3		= serviceTag.getString(index++);
			tag.billAddress4		= serviceTag.getString(index++);
			tag.billAttn			= serviceTag.getString(index++);
			tag.tenant				= serviceTag.getString(index++);
			/*
			 * Not necessary since we must be linked to a site
			tag.siteName			= serviceTag.getString(index++);
			tag.address1			= serviceTag.getString(index++);
			tag.address2			= serviceTag.getString(index++);
			tag.city				= serviceTag.getString(index++);
			tag.state				= serviceTag.getString(index++);
			tag.zip					= serviceTag.getString(index++);
			tag.buildingNo			= serviceTag.getString(index++);
			tag.note				= serviceTag.getString(index++);
			*/
			tag.batchNo				= serviceTag.getString(index++);
			tag.jobNo				= serviceTag.getString(index++);
			tag.empno				= serviceTag.getString(index++);
			tag.disposition			= serviceTag.getString(index++);
			tag.dispatchDescription	= serviceTag.getString(index++);
			tag.xoi_flag			= serviceTag.getString(index++);
			
			// Add the Service Tag to the download package
			pkg.serviceTag.add(tag);
			}
		
		buildServiceTagUnits();
		}
	
	// Service Tag Unit Details
	private void buildServiceTagUnits() throws SQLException
		{
		if( serviceTagUnit == null )
			return;
		
		int index;
		ServiceTagUnit unit;
		while( serviceTagUnit.next() )
			{
			index = 1;
			unit = new ServiceTagUnit();
			unit.serviceTagUnitId	= serviceTagUnit.getInt(index++);
			unit.serviceTagId		= serviceTagUnit.getInt(index++);
			unit.equipmentId		= serviceTagUnit.getInt(index++);
			unit.servicePerformed	= serviceTagUnit.getString(index++);
			unit.comments			= serviceTagUnit.getString(index++);
			
			pkg.serviceTagUnit.add(unit);
			}
		
		buildLabor();
		buildMaterial();
		buildRefrigerant();
		}
	
	private void buildLabor() throws SQLException
		{
		ServiceLabor labor;
		
		if( serviceLabor == null )
			return;
		
		while( serviceLabor.next() )
			{
			labor = new ServiceLabor();
			labor.serviceLaborId	= serviceLabor.getInt(1);
			labor.serviceTagUnitId	= serviceLabor.getInt(2);
			labor.serviceDate		= serviceLabor.getString(3);
			labor.regHours			= serviceLabor.getFloat(4);
			labor.thHours			= serviceLabor.getFloat(5);
			labor.dtHours			= serviceLabor.getFloat(6);
			labor.mechanic			= serviceLabor.getString(7);
			labor.rate				= serviceLabor.getString(8);
			pkg.serviceLabor.add(labor);
			}
		
		}
	
	private void buildMaterial() throws SQLException
		{
		ServiceMaterial material;
		
		if( serviceMaterial == null )
			return;
		
		while( serviceMaterial.next() )
			{
			
			material = new ServiceMaterial();
			material.serviceMaterialId	= serviceMaterial.getInt(1);
			material.serviceTagUnitId	= serviceMaterial.getInt(2);
			material.quantity			= serviceMaterial.getFloat(3);
			material.materialDesc		= serviceMaterial.getString(4);
			material.cost				= serviceMaterial.getFloat(5);
			material.refrigerantAdded	= serviceMaterial.getString(6);
			material.source				= serviceMaterial.getString(7);
			
			pkg.serviceMaterial.add(material);
			}
		
		}
		
	private void buildRefrigerant() throws SQLException
		{
		ServiceRefrigerant refrigerant;
		
		if( serviceRefrigerant == null )
			return;
		
		while( serviceRefrigerant.next() )
			{
			
			refrigerant = new ServiceRefrigerant();
			refrigerant.serviceRefrigerantId	= serviceRefrigerant.getInt(1);
			refrigerant.serviceTagUnitId	= serviceRefrigerant.getInt(2);
			refrigerant.transferDate			= serviceRefrigerant.getString(3);
			refrigerant.techName		= serviceRefrigerant.getString(4);
			refrigerant.typeOfRefrigerant			= serviceRefrigerant.getString(5);
			refrigerant.amount	= serviceRefrigerant.getFloat(6);
			refrigerant.nameOfCylinder			= serviceRefrigerant.getString(7);
			refrigerant.cylinderSerialNo			= serviceRefrigerant.getString(8);
			refrigerant.transferedTo		= serviceRefrigerant.getString(9);
			refrigerant.serialNo	= serviceRefrigerant.getString(10);
			refrigerant.modelNo		= serviceRefrigerant.getString(11);
			
			pkg.serviceRefrigerant.add(refrigerant);
			}
		
		}
	
	// Service Address Details
	private void buildServiceAddress() throws SQLException
		{
		if( serviceAddress == null )
			return;
		
		int index;
		ServiceAddress sa;
		
		while( serviceAddress.next() )
			{
			index = 1;
			sa = new ServiceAddress();
			sa.serviceAddressId		= serviceAddress.getInt(index++);
			sa.siteName				= serviceAddress.getString(index++);
			sa.address1				= serviceAddress.getString(index++);
			sa.address2				= serviceAddress.getString(index++);
			sa.city					= serviceAddress.getString(index++);
			sa.state				= serviceAddress.getString(index++);
			sa.zip					= serviceAddress.getString(index++);
			sa.buildingNo			= serviceAddress.getString(index++);
			sa.note					= serviceAddress.getString(index++);
			
			// Add the Service Tag to the download package
			pkg.serviceAddress.add(sa);
			}
		
		buildServiceAddressTenants();
		buildServiceAddressContact();
		buildServiceAddressNotes();
		buildPMAddressChecklist();
		}
	
	private void buildServiceAddressTenants() throws SQLException
		{
		ServiceAddress.tenant sa_tenant;
		
		while( tenant.next() )
			{
			sa_tenant = new ServiceAddress.tenant();
			sa_tenant.tenantId			= tenant.getInt(1);
			sa_tenant.serviceAddressId	= tenant.getInt(2);
			sa_tenant.tenant			= tenant.getString(3);
			
			pkg.tenants.add(sa_tenant);
			}
		
		}
	
	private void buildPMAddressChecklist() throws SQLException
		{
		pmAddressChecklist pmList;
		
		while(pmAddressChecklist.next())
			{
			pmList = new pmAddressChecklist();
			pmList.pmChecklistId		= pmAddressChecklist.getInt(1);
			pmList.serviceAddressId		= pmAddressChecklist.getInt(2);
			pmList.equipmentCategoryId	= pmAddressChecklist.getInt(3);
			pmList.itemType 			= pmAddressChecklist.getString(4);
			pmList.itemText				= pmAddressChecklist.getString(5);
			pmList.identifier			= pmAddressChecklist.getString(6);
			pkg.pmAddressChecklist.add(pmList);
			}
		
		}
	
	// Equipment
	private void buildEquipment() throws SQLException
		{
		if( equipment == null )
			return;
		
		int index;
		Equipment eq;
		
		while( equipment.next() )
			{
			index = 1;
			eq = new Equipment();
			eq.equipmentId			= equipment.getInt(index++);
			eq.equipmentCategoryId	= equipment.getInt(index++);
			eq.serviceAddressId		= equipment.getInt(index++);
			eq.unitNo				= equipment.getString(index++);
			eq.barCodeNo			= equipment.getString(index++);
			eq.manufacturer			= equipment.getString(index++);
			eq.model				= equipment.getString(index++);
			eq.productIdentifier	= equipment.getString(index++);
			eq.serialNo				= equipment.getString(index++);
			eq.voltage				= equipment.getString(index++);
			eq.economizer			= equipment.getString(index++);
			eq.capacity				= equipment.getFloat(index++);
			eq.capacityUnits		= equipment.getString(index++);
			eq.refrigerantType		= equipment.getString(index++);
			eq.areaServed			= equipment.getString(index++);
			eq.mfgYear				= equipment.getString(index++);
			eq.dateInService		= equipment.getString(index++);
			eq.dateOutService		= equipment.getString(index++);
			eq.notes				= equipment.getString(index++);
			eq.verifiedByEmpno		= equipment.getString(index++);
			
			// Add the Open Tag to the download package
			pkg.equipment.add(eq);
			}
		
		buildFans();
		buildFilters();
		buildRefCircuits();
		buildClosedBlues();
		}
		// Fan
	private void buildFans() throws SQLException
		{
		if( fan == null )
			return;
		Fan f;
		while( fan.next() )
			{
			f = new Fan();
			f.fanId			= fan.getInt(1);
			f.equipmentId	= fan.getInt(2);
			f.partType		= fan.getString(3);
			f.number		= fan.getString(4);
			
			f.beltSize		= fan.getString(5);
			f.beltQty		= fan.getInt(6);
			
			pkg.fan.add(f);
			}
		
		if( sheave == null )
			return;
		Sheave s;
		while( sheave.next() )
			{
			s = new Sheave();
			s.fanId			= sheave.getInt(1);
			s.type			= sheave.getString(2);
			s.number		= sheave.getString(3);
			s.manufacturer	= sheave.getString(4);
			
			pkg.sheave.add(s);
			}
		
		}
		// Filter
	private void buildFilters() throws SQLException
		{
		if( filter == null )
			return;
		
		Filter fil;
		while( filter.next() )
			{
			fil = new Filter();
			fil.equipmentId = filter.getInt(1);
			fil.type		= filter.getString(2);
			fil.quantity	= filter.getInt(3);
			fil.filterSize	= filter.getString(4);
			
			pkg.filter.add(fil);
			}
		
		}
		// RefCircuit
	private void buildRefCircuits() throws SQLException
		{
		if( refCircuit == null )
			return;
		RefCircuit ref;
		while( refCircuit.next() )
			{
			ref = new RefCircuit();
			ref.circuitId		= refCircuit.getInt(1);
			ref.equipmentId		= refCircuit.getInt(2);
			ref.circuitNo		= refCircuit.getString(3);
			ref.lbsRefrigerant	= refCircuit.getFloat(4);
			
			pkg.refcircuit.add(ref);
			}
		
		if( compressor == null )
			return;
		Compressor comp;
		while( compressor.next() )
			{
			comp = new Compressor();
			comp.circuitId		= compressor.getInt(1);
			comp.compressorNo	= compressor.getString(2);
			comp.manufacturer	= compressor.getString(3);
			comp.model			= compressor.getString(4);
			comp.serialNo		= compressor.getString(5);
			comp.dateInService	= compressor.getString(6);
			comp.dateOutService	= compressor.getString(7);
			
			pkg.compressor.add(comp);
			}
		
		}
		// Closed Blues
	private void buildClosedBlues() throws SQLException
		{
		if( closedBlue == null )
			return;
		
		OpenBlue blue;
		OpenBlueUnit unit;
		
		while( closedBlue.next() )
			{
			blue = new OpenBlue();
			blue.blueId				= closedBlue.getInt(1);
			blue.serviceTagId 		= closedBlue.getInt(2);
			blue.dateCreated		= closedBlue.getString(3);
			
			pkg.closedBlue.add(blue);
			}
		
		if( closedBlueUnit == null )
			return;
		
		while( closedBlueUnit.next() )
			{
			unit = new OpenBlueUnit();
			unit.blueUnitId		= closedBlueUnit.getInt(1);
			unit.blueId			= closedBlueUnit.getInt(2);
			unit.equipmentId	= closedBlueUnit.getInt(3);
			unit.description	= closedBlueUnit.getString(4);
			unit.materials		= closedBlueUnit.getString(5);
			unit.laborHours		= closedBlueUnit.getFloat(6);
			unit.tradesmenhrs	= closedBlueUnit.getFloat(7);
			unit.otherhrs		= closedBlueUnit.getFloat(8);
			unit.notes			= closedBlueUnit.getString(9);
			unit.cost			= closedBlueUnit.getFloat(10);
			
			pkg.closedBlueUnit.add(unit);
			}
		}
	
	// Service Address Contacts
	private void buildServiceAddressContact() throws SQLException
		{
		if( serviceAddressContact == null )
			return;
		
		int index;
		ServiceAddressContact contact;
		
		while( serviceAddressContact.next() )
			{
			index = 1;
			contact = new ServiceAddressContact();
			contact.contactId			= serviceAddressContact.getInt(index++);
			contact.serviceAddressId	= serviceAddressContact.getInt(index++);
			contact.contactName			= serviceAddressContact.getString(index++);
			contact.phone1				= serviceAddressContact.getString(index++);
			contact.phone1Type			= serviceAddressContact.getString(index++);
			contact.phone2				= serviceAddressContact.getString(index++);
			contact.phone2Type			= serviceAddressContact.getString(index++);
			contact.email				= serviceAddressContact.getString(index++);
			contact.contactType			= serviceAddressContact.getString(index++);
			contact.ext1				= serviceAddressContact.getString(index++);
			contact.ext2				= serviceAddressContact.getString(index++);
			
			// Add the Open Tag to the download package
			pkg.contacts.add(contact);
			}
		
		}
	
	private void buildServiceAddressNotes() throws SQLException
		{
		if( serviceAddressNotes == null )
			return;
		
		int index;
		ServiceAddressNotes notes;
		
		while( serviceAddressNotes.next() )
			{
			index = 1;
			notes = new ServiceAddressNotes();
			notes.noteId			= serviceAddressNotes.getInt(index++);
			notes.serviceAddressId	=serviceAddressNotes.getInt(index++);
			notes.notes			= serviceAddressNotes.getString(index++);
			
			
			// Add the Open Tag to the download package
			pkg.notes.add(notes);
			}
		
		}
	
	private void buildStaticTables(Timestamp lastSync) throws SQLException
		{
		PreparedStatement stmt;
		ResultSet results;
		Map<String, Timestamp> masterTableLog = new TreeMap<String, Timestamp>();
		String sql;
		
		stmt = prep_stmts.masterTableLog_select;
		results = stmt.executeQuery();
		while( results.next() )
			{
			masterTableLog.put( results.getString(1), results.getTimestamp(2) );
			}
		
		if( lastSync == null || lastSync.compareTo(masterTableLog.get("equipmentCategory")) < 0 || request.init )
			{
			sql = "SELECT equipmentCategoryId, categoryDesc FROM equipmentCategory";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.EquipmentCategory ec;
				pkg.equipmentCategory = new ArrayList<Package_Download.EquipmentCategory>();
				do
					{
					ec = new Package_Download.EquipmentCategory();
					ec.equipmentCategoryId = results.getInt(1);
					ec.categoryDesc = results.getString(2);
					pkg.equipmentCategory.add(ec);
					}
				while( results.next() );
				}
			}
		if( lastSync == null || lastSync.compareTo(masterTableLog.get("equipmentCategory")) < 0 || request.init )
			{
			sql = "SELECT rateId, rate, rateDesc FROM serviceLaborRate";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.serviceLaborRate slr;
				pkg.servicelaborrate = new ArrayList<Package_Download.serviceLaborRate>();
				do
					{
					slr= new Package_Download.serviceLaborRate();
					slr.rateId = results.getInt(1);
					slr.rate = results.getString(2);
					slr.rateDesc = results.getString(3);
					pkg.servicelaborrate.add(slr);
					}
				while( results.next() );
				}
			}
			/*
			if( lastSync == null || lastSync.compareTo(masterTableLog.get("jobdocs")) < 0 || request.init )
			{
			sql = "SELECT jobno,documentContents,documentName,documentTitle,datasubmitted,jobsite from jobdoc";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.JobDoc jd;
				pkg.jobdoc = new ArrayList<Package_Download.JobDoc>();
				do
					{
					jd= new Package_Download.JobDoc();
					jd.jobno = results.getString(1);
					jd.documentContents = results.getBytes(2);
					jd.documentName = results.getString(3);
					jd.documentTitle = results.getString(4);
					jd.datasubmitted = results.getString(5);
					jd.jobsite = results.getString(6);
					pkg.jobdoc.add(jd);
					}
				while( results.next() );
				}
			}*/
		/*
		if( lastSync == null || lastSync.compareTo(masterTableLog.get("filterType")) < 0 || request.init )
			{
			sql = "SELECT filterTypeId, filterType FROM filterType";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.FilterType filType;
				pkg.filterType = new ArrayList<Package_Download.FilterType>();
				do
					{
					filType = new Package_Download.FilterType();
					filType.filterTypeId = results.getInt(1);
					filType.filterType = results.getString(2);
					pkg.filterType.add(filType);
					}
				while( results.next() );
				}
			}
		
		if( lastSync == null || lastSync.compareTo(masterTableLog.get("filterSize")) < 0 || request.init )
			{
			sql = "SELECT filterSizeId, filterSize FROM filterSize";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.FilterSize filSize;
				pkg.filterSize = new ArrayList<Package_Download.FilterSize>();
				do
					{
					filSize = new Package_Download.FilterSize();
					filSize.filterSizeId = results.getInt(1);
					filSize.filterSize = results.getString(2);
					pkg.filterSize.add(filSize);
					}
				while( results.next() );
				}
			}
			*/
		if( lastSync == null || lastSync.compareTo(masterTableLog.get("refrigerantType")) < 0 || request.init )
			{
			sql = "SELECT refrigerantTypeId, refrigerantType FROM refrigerantType";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.RefrigerantType ref;
				pkg.refrigerantType = new ArrayList<Package_Download.RefrigerantType>();
				do
					{
					ref = new Package_Download.RefrigerantType();
					ref.refrigerantTypeId = results.getInt(1);
					ref.refrigerantType = results.getString(2);
					pkg.refrigerantType.add(ref);
					}
				while( results.next() );
				}
			}
	
		
		if( lastSync == null || lastSync.compareTo(masterTableLog.get("safetyChecklist")) < 0 || request.init )
			{
			sql = "SELECT safetyChecklistId, sortOrder, LOTO, itemType, itemText, itemTextBold FROM safetyChecklist";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.SafetyChecklist clist;
				pkg.safetyChecklist = new ArrayList<Package_Download.SafetyChecklist>();
				do
					{
					clist = new Package_Download.SafetyChecklist();
					clist.safetyChecklistId = results.getInt(1);
					clist.sortOrder 		= results.getInt(2);
					clist.LOTO 				= results.getString(3);
					clist.itemType 			= results.getString(4);
					clist.itemText 			= results.getString(5);
					clist.itemTextBold 		= results.getString(6);
					pkg.safetyChecklist.add(clist);
					}
				while( results.next() );
				}
			}
		
		if( lastSync == null || lastSync.compareTo(masterTableLog.get("mechanic")) < 0 || request.init )
			{
			sql = "SELECT empno, Rtrim(Ltrim(last_name)) + ', ' + Rtrim(Ltrim(first_name)) as mechanic_name, terminated, dept " +
					"FROM users " +
						//"WHERE UPPER(dept) = 'REF' " +
							"ORDER BY mechanic_name";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.Mechanic mech;
				pkg.mechanic = new ArrayList<Package_Download.Mechanic>();
				do
					{
					mech = new Package_Download.Mechanic();
					mech.empno 		= results.getString(1);
					mech.name 		= results.getString(2);
					mech.terminated = results.getString(3);
					mech.dept		= results.getString(4);
					pkg.mechanic.add(mech);
					}
				while( results.next() );
				}
			}
		
		if( lastSync == null || lastSync.compareTo(masterTableLog.get("serviceDescription")) < 0 || request.init )
			{
			sql = "SELECT descriptionId, description FROM serviceDescription";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.ServiceDescription desc;
				pkg.serviceDescription = new ArrayList<Package_Download.ServiceDescription>();
				do
					{
					desc = new Package_Download.ServiceDescription();
					desc.descriptionID 		= results.getInt(1);
					desc.description 		= results.getString(2);
					pkg.serviceDescription.add(desc);
					}
				while( results.next() );
				}
			}
		
		if( lastSync == null || lastSync.compareTo(masterTableLog.get("pmStdChecklist")) < 0 || request.init )
			{
			sql = "SELECT pmChecklistId, equipmentCategoryId, itemText, itemType, identifier FROM pmStdChecklist";
			stmt = db.prepareStatement(sql);
			results = stmt.executeQuery();
			if( results.next() )
				{
				Package_Download.pmStdChecklist pmlist;
				pkg.pmStdChecklist = new ArrayList<Package_Download.pmStdChecklist>();
				do
					{
					pmlist = new Package_Download.pmStdChecklist();
					pmlist.pmChecklistId 		= results.getInt(1);
					pmlist.equipmentCategoryId 	= results.getInt(2);
					pmlist.itemText		 		= results.getString(3);
					pmlist.itemType			 	= results.getString(4);
					pmlist.identifier			= results.getString(5);
					pkg.pmStdChecklist.add(pmlist);
					}
				while( results.next() );
				}
			}
		}
	
	private void getAndBuildBilling() throws SQLException
		{
		int custSize = custIds.size();
		if( custSize <= 0 )
			return;
		String custParams = buildParams(custSize);
		
		int altBillSize = altBillTo.size();
		
		String sql = "SELECT * FROM ( " + 
			"SELECT	COALESCE(A.CustomerId, C.CustomerId) customerId, " + 
				"COALESCE(LTRIM(RTRIM(A.Name)), LTRIM(RTRIM(C.Name))) Name, " + 
				"COALESCE(LTRIM(RTRIM(A.Address1)), LTRIM(RTRIM(C.Address1))) address1, " + 
				"COALESCE(LTRIM(RTRIM(A.Address2)), LTRIM(RTRIM(C.Address2))) address2, " + 
				"COALESCE(LTRIM(RTRIM(A.Address3)), LTRIM(RTRIM(C.City)) + ', ' + LTRIM(RTRIM(C.State)) + ' ' + LTRIM(RTRIM(C.Zip)) ) address3, " + 
				"LTRIM(RTRIM(A.Address4)) address4, " + 
				"LTRIM(RTRIM(A.altBillTo)) altBillTo " + 
				

			"FROM Customers as C " + 
				"LEFT OUTER JOIN CustomersAltBill as A " + 
				"ON C.CustomerID = A.CustomerId "; 
			
		sql += "WHERE C.CustomerID IN ( " + custParams + " ) " + 
					"AND C.CustomerID IN ( " + custParams + " ) " + 
			") cust ";
		if( altBillSize > 0 )
			sql += "WHERE (altBillTo IS NULL) OR (altBillTo IN ( " + buildParams(altBillSize) + " ))";
		PreparedStatement stmt = Therma_DW.prepareStatement(sql);
		
		int index = 1;
		String cusNo;
		for( Iterator<String> i = custIds.iterator(); i.hasNext(); )
			{
			cusNo = i.next();
			stmt.setString(index, cusNo);
			stmt.setString(index + custSize, cusNo);
			index++;
			}
		index += custSize;
		for( Iterator<String> i = altBillTo.iterator(); i.hasNext(); )
			{
			stmt.setString(index++, i.next());
			}
		
		Billing bill;
		ResultSet billing = stmt.executeQuery();
		while( billing.next() )
			{
			bill = new Billing();
			bill.CustomerId	= billing.getString(1);
			bill.Name		= billing.getString(2);
			bill.Address1	= billing.getString(3);
			bill.Address2	= billing.getString(4);
			bill.Address3	= billing.getString(5);
			bill.Address4	= billing.getString(6);
			bill.altBillTo	= billing.getString(7);
			
			pkg.billing.add(bill);
			}
		}
	
	private void getAndBuildDispatchPriority(Timestamp lastSync) throws SQLException
		{
		String sql = "SELECT PriorityId, RGBColor, DaysLate, DateChanged " + 
					"FROM DispatchPriority";
		if( request.latestDates != null && !request.init && request.latestDates.DispatchPriority != null )
			sql += " WHERE DateChanged > ?";
		
		PreparedStatement stmt = db.prepareStatement(sql);
		
		if( request.latestDates != null && request.latestDates.DispatchPriority != null && !request.init )
			{
			Timestamp ts;
			try
				{
				ts = Timestamp.valueOf(request.latestDates.DispatchPriority);
				}
			catch( Exception e )
				{
				e.printStackTrace();
				ts = new Timestamp(0);
				}
			stmt.setTimestamp(1, ts);
			}
		
		DispatchPriority dp;
		ResultSet results = stmt.executeQuery();
		pkg.dispatchPriority = new ArrayList<DispatchPriority>();
		while( results.next() )
			{
			dp = new DispatchPriority();
			dp.PriorityId	= results.getInt(1);
			dp.RGBColor		= results.getString(2);
			dp.DaysLate		= results.getInt(3);
			dp.DateChanged	= results.getString(4);
			
			pkg.dispatchPriority.add(dp);
			}
		}
	
	private void getAndBuildPickLists(Timestamp lastSync) throws SQLException
		{
		/************************************
		 * Form Pick List
		 ***********************************/
		//if( lastSync != null )
		String sql = "SELECT PickId, Description, DateChanged FROM PickList";
		
		if( lastSync != null )
			sql += " WHERE DateChanged > ?";
		PreparedStatement stmt = db.prepareStatement(sql);
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		ResultSet results = stmt.executeQuery();
		Package_Download.PickList PickList;
		int index;
		while( results.next() )
			{
			PickList = new Package_Download.PickList();
			index = 1;
			PickList.PickId		= results.getLong(index++);
			PickList.Description= results.getString(index++);
			PickList.DateChanged= results.getString(index++);
			
			pkg.PickList.add(PickList);
			}
		
		/************************************
		 * Form Pick List Item
		 ***********************************/
		String PickSql = "SELECT PickId FROM PickList";
		if( lastSync != null )
			PickSql += " WHERE DateChanged > ?";
		
		sql = "SELECT PickItemId, PickId, itemValue " + 
				"FROM PickListItem WHERE PickId IN ( " + PickSql + " )";
		stmt = db.prepareStatement(sql);
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		results = stmt.executeQuery();
		Package_Download.PickListItem PickItem;
		while( results.next() )
			{
			PickItem = new Package_Download.PickListItem();
			index = 1;
			PickItem.PickItemId	= results.getLong(index++);
			PickItem.PickId		= results.getLong(index++);
			PickItem.itemValue	= results.getString(index++);
			
			pkg.PickListItem.add(PickItem);
			}
		}
	
	/******************************************************************
	 * Fetches the Form Data that needs to be sent back to the tablet.
	 ******************************************************************/
	private void getAndBuildFormPackage(Timestamp lastSync) throws SQLException
		{
		FormPkg = new FormPackage();
		FormDataPkg = new FormDataPackage();
		/************************************
		 * Form
		 ***********************************/
		String sql = "SELECT f.FormId, f.Type, f.EquipmentCategoryId, f.AttrId, f.Description, f.DateChanged, " +
				"fv.VersionId, fv.VersionNum " + 
				"FROM Form as f " +
					"INNER JOIN FormVersionsXRef as fv " +
						"ON fv.FormId = f.FormId " +
				"WHERE Live = 'Y'";
		if( lastSync != null )
			sql += " AND DateChanged > ?";
		
		PreparedStatement stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		FormPackage.Form formItem;
		ResultSet results = stmt.executeQuery();
		int index;
		while( results.next() )
			{
			formItem = new FormPackage.Form();
			index = 1;
			formItem.FormId					= results.getInt(index++);
			formItem.Type					= results.getString(index++);
			formItem.EquipmentCategoryId	= results.getInt(index++);
			formItem.AttrId					= results.getLong(index++);
			formItem.Description 			= results.getString(index++);
			formItem.DateChanged			= results.getString(index++);
			formItem.VersionId				= results.getLong(index++);
			formItem.VersionNum				= results.getInt(index++);
			
			FormPkg.Form.add(formItem);
			}
		
		/************************************
		 * Form Section
		 ***********************************/
		sql = "SELECT fs.FormSecId, fs.Title, fs.RowCnt, fs.ColCnt, fs.DateChanged, " +
				"fv.VersionId, fv.VersionNum " + 
				"FROM FormSection as fs " +
					"INNER JOIN FormSectionVersionsXRef as fv " +
						"ON fv.FormSecId = fs.FormSecId " +
				"WHERE Live = 'Y'";
		if( lastSync != null )
			sql += " AND DateChanged > ?";
		
		stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		results = stmt.executeQuery();
		FormPackage.FormSection formSection;
		while( results.next() )
			{
			formSection = new FormPackage.FormSection();
			index = 1;
			formSection.FormSecId	= results.getLong(index++);
			formSection.Title		= results.getString(index++);
			formSection.RowCnt		= results.getInt(index++);
			formSection.ColCnt		= results.getInt(index++);
			formSection.DateChanged	= results.getString(index++);
			formSection.VersionId	= results.getLong(index++);
			formSection.VersionNum	= results.getInt(index++);
			
			FormPkg.FormSection.add(formSection);
			}
		
		/************************************
		 * Form Section XRef
		 ***********************************/
		String FormSql = "SELECT FormId FROM Form WHERE Live = 'Y'";
		if( lastSync != null )
			FormSql += " AND DateChanged > ?";
		
		sql = "SELECT XRefId, FormId, FormSecId, SortOrder, SectionType " + 
				"FROM FormSecXRef " +
				"WHERE FormId IN ( " + FormSql + ")";
		
		stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		results = stmt.executeQuery();
		FormPackage.FormSecXRef formSecXRef;
		while( results.next() )
			{
			formSecXRef = new FormPackage.FormSecXRef();
			index = 1;
			formSecXRef.XRefId		= results.getLong(index++);
			formSecXRef.FormId		= results.getInt(index++);
			formSecXRef.FormSecId	= results.getLong(index++);
			
			formSecXRef.SortOrder	= results.getInt(index++);
			formSecXRef.SectionType	= results.getString(index++);
			
			FormPkg.FormSecXRef.add(formSecXRef);
			}
		
		/************************************
		 * Form Matrix XRef
		 ***********************************/
		String FormSecSql = "SELECT FormSecId FROM FormSection WHERE Live = 'Y'";
		if( lastSync != null )
			FormSecSql += " AND DateChanged > ?";
		
		sql = "SELECT MatrixId, AttrId, PickId, Text, Modifiable, ChildFormId, " +
					"Row, Col, FormSecId, RowSpan, ColSpan, " +
					"FontSize, FontColor, Bold, Italic, Underline, Align, VAlign, BGColor, " +
					"BorderLeft, BorderBottom, BorderRight, BorderTop, Required " + 
				"FROM FormMatrixXRef " +
				"WHERE FormSecId IN ( " + FormSecSql + " )";
		
		stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		results = stmt.executeQuery();
		FormPackage.FormMatrixXRef formMatrixXRef;
		while( results.next() )
			{
			formMatrixXRef = new FormPackage.FormMatrixXRef();
			index = 1;
			formMatrixXRef.MatrixId		= results.getLong(index++);
			
			formMatrixXRef.AttrId		= results.getLong(index++);
			formMatrixXRef.PickId		= results.getLong(index++);
			formMatrixXRef.Text			= results.getString(index++);
			formMatrixXRef.Modifiable	= results.getString(index++);
			formMatrixXRef.ChildFormId	= results.getLong(index++);
			
			formMatrixXRef.Row			= results.getInt(index++);
			formMatrixXRef.Col			= results.getInt(index++);
			formMatrixXRef.FormSecId	= results.getLong(index++);
			formMatrixXRef.RowSpan		= results.getInt(index++);
			formMatrixXRef.ColSpan		= results.getInt(index++);
			
			formMatrixXRef.FontSize		= results.getInt(index++);
			formMatrixXRef.FontColor	= results.getString(index++);
			formMatrixXRef.Bold			= results.getString(index++);
			formMatrixXRef.Italic		= results.getString(index++);
			formMatrixXRef.Underline	= results.getString(index++);
			formMatrixXRef.Align		= results.getString(index++);
			formMatrixXRef.VAlign		= results.getString(index++);
			formMatrixXRef.BGColor		= results.getString(index++);
			
			formMatrixXRef.BorderLeft	= results.getString(index++);
			formMatrixXRef.BorderBottom	= results.getString(index++);
			formMatrixXRef.BorderRight	= results.getString(index++);
			formMatrixXRef.BorderTop	= results.getString(index++);
			formMatrixXRef.Required		= results.getString(index++);
			
			FormPkg.FormMatrixXRef.add(formMatrixXRef);
			}
		
		/************************************
		 * Form Matrix
		 ***********************************/
		String FormMatrixXRefSql = "SELECT MatrixId FROM FormMatrixXRef " + 
				"WHERE FormSecId IN ( " + FormSecSql + ")";
		
		sql = "SELECT MatrixId, InputType, Image, ImageType " + 
				"FROM FormMatrix " +
				"WHERE MatrixId IN ( " + FormMatrixXRefSql + ")";
		
		stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		results = stmt.executeQuery();
		FormPackage.FormMatrix formMatrix;
		while( results.next() )
			{
			formMatrix = new FormPackage.FormMatrix();
			index = 1;
			formMatrix.MatrixId		= results.getLong(index++);
			formMatrix.InputType	= results.getString(index++);
			formMatrix.Image		= results.getBytes(index++);
			formMatrix.ImageType	= results.getString(index++);
			
			FormPkg.FormMatrix.add(formMatrix);
			}
		
		/************************************
		 * Form Options
		 ***********************************/
		sql = "SELECT FormSecId, MatrixId, Value " + 
				"FROM FormOptions " +
				"WHERE MatrixId IN ( " + FormMatrixXRefSql + " )" +
						"AND FormSecId IN ( " + FormSecSql + " )";
		
		stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			{
			stmt.setTimestamp(1, lastSync);
			stmt.setTimestamp(2, lastSync);
			}
		
		results = stmt.executeQuery();
		FormPackage.FormOptions formOptions;
		while( results.next() )
			{
			formOptions = new FormPackage.FormOptions();
			index = 1;
			formOptions.FormSecId	= results.getLong(index++);
			formOptions.MatrixId	= results.getLong(index++);
			formOptions.Value		= results.getString(index++);
			
			FormPkg.FormOptions.add(formOptions);
			}
		
		/************************************
		 * Form Section Height
		 ***********************************/
		sql = "SELECT FormSecId, Row, Height " + 
				"FROM FormSecHeights " +
				"WHERE FormSecId IN ( " + FormSecSql + ")";
		
		stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		results = stmt.executeQuery();
		FormPackage.FormSecHeights formSecHeights;
		while( results.next() )
			{
			formSecHeights = new FormPackage.FormSecHeights();
			index = 1;
			formSecHeights.FormSecId	= results.getLong(index++);
			formSecHeights.Row			= results.getInt(index++);
			formSecHeights.Height		= results.getString(index++);
			
			FormPkg.FormSecHeights.add(formSecHeights);
			}
		
		/************************************
		 * Form Section Width
		 ***********************************/
		sql = "SELECT FormSecId, Col, Width " + 
				"FROM FormSecWidths " +
				"WHERE FormSecId IN ( " + FormSecSql + ")";
		
		stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		results = stmt.executeQuery();
		FormPackage.FormSecWidths formSecWidths;
		while( results.next() )
			{
			formSecWidths = new FormPackage.FormSecWidths();
			index = 1;
			formSecWidths.FormSecId	= results.getLong(index++);
			formSecWidths.Col		= results.getInt(index++);
			formSecWidths.Width		= results.getString(index++);
			
			FormPkg.FormSecWidths.add(formSecWidths);
			}
		
		/************************************
		 * Form Type
		 ***********************************/
		sql = "SELECT FormType, Description " + 
				"FROM FormType";
		stmt = db.prepareStatement(sql);
		
		results = stmt.executeQuery();
		FormPackage.FormType formType;
		while( results.next() )
			{
			formType = new FormPackage.FormType();
			index = 1;
			formType.FormType	= results.getString(index++);
			formType.Description= results.getString(index++);
			
			FormPkg.FormType.add(formType);
			}
		
		/************************************
		 * Form Data
		 ***********************************/
		sql = "SELECT FormDataId, FormId, ParentTable, ParentId, LinkTable, LinkId, " +
					"InputByEmpno, DateEntered, Completed, tabletMEID " + 
				"FROM FormData " +
				"WHERE " +
				"( ParentTable = 'Dispatch' AND InputByEmpno = '" + empno + "' AND Completed != 'Y' ) " +
				
				"OR " +
				
				"( ParentTable = 'ServiceTagUnit' AND ParentId IN " +
					"(SELECT (serviceTagUnitId*-1) FROM openServiceTagUnit WHERE serviceTagId IN " +
						"(SELECT serviceTagId FROM openServiceTag WHERE empno = ?)" +
					")" +
				")";
				//"WHERE InputByEmpno = ? AND Completed != 'Y'";
		stmt = db.prepareStatement(sql);
		stmt.setString(1, empno);
		
		results = stmt.executeQuery();
		FormDataPackage.FormData formData;
		while( results.next() )
			{
			formData = new FormDataPackage.FormData();
			index = 1;
			formData.FormDataId			= results.getLong(index++);
			formData.FormId				= results.getInt(index++);
			formData.ParentTable		= results.getString(index++);
			formData.ParentId			= results.getInt(index++);
			formData.LinkTable			= results.getString(index++);
			formData.LinkId				= results.getInt(index++);
			formData.InputByEmpno		= results.getString(index++);
			formData.DateEntered		= results.getString(index++);
			formData.Completed			= results.getString(index++);
			formData.tabletMEID			= results.getString(index++);
			
			FormDataPkg.FormData.add(formData);
			}
		
		/************************************
		 * Form Data Values
		 ***********************************/
		//String FormDataSql = "SELECT FormDataId FROM FormData " +
		//		"WHERE InputByEmpno = ? AND Completed != 'Y'";
		
		String FormDataSql = "SELECT FormDataId " + 
			"FROM FormData " +
			"WHERE " +
			"( ParentTable = 'Dispatch' AND InputByEmpno = '" + empno + "' AND Completed != 'Y' ) " +
			
			"OR " +
			
			"( ParentTable = 'ServiceTagUnit' AND ParentId IN " +
				"(SELECT (serviceTagUnitId*-1) FROM openServiceTagUnit WHERE serviceTagId IN " +
					"(SELECT serviceTagId FROM openServiceTag WHERE empno = ?)" +
				")" +
			")";
		
		sql = "SELECT FormDataId, XRefId, MatrixTrail, Value " + 
				"FROM FormDataValues " +
				"WHERE FormDataId IN (" + FormDataSql + ")";
		stmt = db.prepareStatement(sql);
		stmt.setString(1, empno);
		
		results = stmt.executeQuery();
		FormDataPackage.FormDataValues formDataValues;
		while( results.next() )
			{
			formDataValues = new FormDataPackage.FormDataValues();
			index = 1;
			formDataValues.FormDataId		= results.getLong(index++);
			formDataValues.XRefId			= results.getLong(index++);
			formDataValues.MatrixTrail		= results.getString(index++);
			formDataValues.Value			= results.getString(index++);
			
			FormDataPkg.FormDataValues.add(formDataValues);
			}
		
		FormDataPkg.ResolvedFormDataIds = GetBigIntResolvedIds("Resolved_FormData");
		}
	
	private void UpdateFormMEID(String MEID, String EmpNo) throws SQLException
		{
		String SQLStatement = "UPDATE FormData SET tabletMEID = ? WHERE Completed != 'Y' AND InputByEmpno = ?";
		PreparedStatement stmt = db.prepareStatement(SQLStatement);
		stmt.setString(1, MEID);
		stmt.setString(2, EmpNo);
		stmt.execute();
		stmt.close();
		}
	
	private void getAndBuildAttributeStructure(Timestamp lastSync) throws SQLException
		{
		AttrPkg = new AttributePackage();
		
		String sql = "SELECT AttrId, Type, DisplayName, ShortName, SortOrder, InputMask, " +
				"HostTable, HostColumn, DateChanged, Deprecated " + 
				"FROM AttrDef ";
		if( lastSync != null )
			sql += "WHERE DateChanged > ?";
		else
			sql += "WHERE Deprecated = 'N'";
		
		PreparedStatement stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		AttributePackage.AttrDef defItem;
		ResultSet results = stmt.executeQuery();
		int index;
		while( results.next() )
			{
			defItem = new AttributePackage.AttrDef();
			index = 1;
			defItem.AttrId			= results.getLong(index++);
			defItem.Type			= results.getString(index++);
			defItem.DisplayName		= results.getString(index++);
			defItem.ShortName		= results.getString(index++);
			defItem.SortOrder		= results.getInt(index++);
			defItem.InputMask		= results.getString(index++);
			defItem.HostTable		= results.getString(index++);
			defItem.HostColumn		= results.getString(index++);
			defItem.DateChanged		= results.getString(index++);
			defItem.Deprecated		= results.getString(index++);
			
			AttrPkg.AttrDef.add(defItem);
			}
		
		
		sql = "SELECT Parent, Child, DateChanged " + 
				"FROM AttrXRef ";
		if( lastSync != null )
			sql += "WHERE DateChanged > ?";
		//else
		//	sql += "WHERE Deprecated = 'N'";
		
		stmt = db.prepareStatement(sql);
		
		if( lastSync != null )
			stmt.setTimestamp(1, lastSync);
		
		AttributePackage.AttrXRef xRefItem;
		results = stmt.executeQuery();
		while( results.next() )
			{
			xRefItem = new AttributePackage.AttrXRef();
			index = 1;
			xRefItem.Parent			= results.getLong(index++);
			xRefItem.Child			= results.getLong(index++);
			xRefItem.DateChanged	= results.getString(index++);
			
			AttrPkg.AttrXRef.add(xRefItem);
			}
		
		if( lastSync == null )
			{
			sql = "SELECT equipmentCategoryId, AttrId FROM CategoryAttrLink";
			stmt = db.prepareStatement(sql);
			
			AttributePackage.CategoryAttrLink cLink;
			results = stmt.executeQuery();
			while( results.next() )
				{
				cLink = new AttributePackage.CategoryAttrLink();
				index = 1;
				cLink.equipmentCategoryId	= results.getInt(index++);
				cLink.AttrId				= results.getLong(index++);
				
				AttrPkg.CategoryAttrLink.add(cLink);
				}
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
	
	// Set and Get Last Sync
	private Timestamp getLastSync() throws SQLException
		{
		Timestamp d = null;
		
		String sql = "SELECT lastSync FROM tabletLog " +
				"WHERE tabletMEID = ? ";
		PreparedStatement tablet_fetch_lastsync = db.prepareStatement(sql);
		tablet_fetch_lastsync.setString(1, request.MEID);
		ResultSet results = tablet_fetch_lastsync.executeQuery();
		
		if( results.next() )
			{
			d = results.getTimestamp(1);
			}
		
		return d;
		}
	
	private void setLastSync() throws SQLException
		{
		String sql = "UPDATE tabletLog SET lastSync = getDate(), empno = ? " +
				"WHERE tabletMEID = ? " +
		
		"IF @@ROWCOUNT=0 " +
			"INSERT INTO tabletLog ( tabletMEID, lastSync, empno ) " +
			" VALUES( ?, getDate(), ?)";
		PreparedStatement tablet_set_sync = db.prepareStatement(sql);
		tablet_set_sync.setString(1, empno);
		tablet_set_sync.setString(2, request.MEID);
		tablet_set_sync.setString(3, request.MEID);
		tablet_set_sync.setString(4, empno);
		
		tablet_set_sync.execute();
		
		}
	
	//******** Dispatch Request
	private void getAndBuildDispatch(int dispatchId) throws SQLException
		{
		String sql = "SELECT dispatchId, serviceAddressId," +
				"batchNo, jobNo, LTRIM(RTRIM(cusNo)), LTRIM(RTRIM(altBillTo)), contractType, " +
				"dateStarted, dateEnded, dateOrdered, customerPO, requestedBy, " +
				"requestedByPhone, requestedByEmail, siteContact, siteContactPhone, " +
				"description, mechanic1, mechanic2, siteName, mechanic3, " +
				"mechanic4, mechanic5, mechanic6, mechanic7, status, tenant, PMComments, PMEstTime " +
			"FROM dispatch " +
			"WHERE dispatchId = ? ";
		
		sql += // Parenthesis ends the starting where, which or may not include the dispatchId IN clause
			" AND (serviceAddressId <> '' " +
			"AND serviceAddressId IS NOT NULL)"; // Add " and (C and D)" 
		
			
		PreparedStatement stmt = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		// Bind the dispatchId
		stmt.setInt(1, dispatchId);
		
		// Execute the Query
		dispatch = stmt.executeQuery();
		buildDispatch();
		}
	
	private Map<Long, Long> GetBigIntResolvedIds(String TableName) throws SQLException
		{
		Map<Long, Long> ret = new HashMap<Long, Long>();
		
		String sql = "SELECT oldid, newid FROM " + TableName + " WHERE tabletMEID = ? AND empno = ?";
		PreparedStatement stmt = db.prepareStatement(sql);
		stmt.setString(1, request.MEID);
		stmt.setString(2, empno);
		ResultSet results = stmt.executeQuery();
		while( results.next() )
			ret.put(results.getLong(1), results.getLong(2));
		
		stmt.close();
		return ret;
		}
	
	private Map<Integer, Integer> GetIntResolvedIds(String TableName) throws SQLException
		{
		Map<Integer, Integer> ret = new HashMap<Integer, Integer>();
		
		String sql = "SELECT oldid, newid FROM " + TableName + " WHERE tabletMEID = ? AND empno = ?";
		PreparedStatement stmt = db.prepareStatement(sql);
		stmt.setString(1, request.MEID);
		stmt.setString(2, empno);
		ResultSet results = stmt.executeQuery();
		while( results.next() )
			ret.put(results.getInt(1), results.getInt(2));
		
		stmt.close();
		return ret;
		}
	}

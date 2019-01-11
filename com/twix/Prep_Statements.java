import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Prep_Statements
	{
	private Connection db;
	
	// Sync Statements
		// Equipment Insert Statements
	PreparedStatement equipment_insert;
	PreparedStatement equipment_update;
	PreparedStatement fan_insert;
	PreparedStatement belt_insert;
	PreparedStatement sheave_insert;
	PreparedStatement filter_insert;
	PreparedStatement refcircuit_insert;
	PreparedStatement compressor_insert;
	
		// Equipment Detail drop Statements
	PreparedStatement fan_drop;
	PreparedStatement belt_drop;
	PreparedStatement sheave_drop;
	PreparedStatement filter_drop;
	PreparedStatement refcircuit_drop;
	PreparedStatement compressor_drop;
	
		// Service Address Contact Insert/Update Statements
	PreparedStatement contact_update;
	PreparedStatement contact_insert;
	
		// Open Tag Insert Statements
	PreparedStatement nextTag_insert;
	PreparedStatement openServiceTag_insert;
	PreparedStatement openServiceTagUnit_insert;
	PreparedStatement openServiceLabor_insert;
	PreparedStatement openServiceMaterial_insert;
	PreparedStatement openPMChecklist_insert;
	PreparedStatement openBlue_insert;
	PreparedStatement openBlueUnit_insert;
	PreparedStatement openSafetyChecklist_insert;
	PreparedStatement openSafetyChecklistItem_insert;
	
		// Drop Open Tags Statements
	PreparedStatement openServiceTag_drop;
	PreparedStatement openServiceTagUnit_drop;
	PreparedStatement openServiceLabor_drop;
	PreparedStatement openServiceMaterial_drop;
	PreparedStatement openServicePMChecklist_drop;
	PreparedStatement openBlue_drop;
	PreparedStatement openBlueUnit_drop;
	PreparedStatement openSafetyChecklist_drop;
	PreparedStatement openSafetyChecklistItem_drop;
	
		// Closed Tag Insert Statements
	PreparedStatement serviceTag_insert;
	PreparedStatement serviceTagUnit_insert;
	PreparedStatement serviceLabor_insert;
	PreparedStatement serviceMaterial_insert;
	PreparedStatement servicePhoto_insert;
	PreparedStatement PMChecklist_insert;
	PreparedStatement serviceReceipt_insert;
	PreparedStatement blue_insert;
	PreparedStatement blueUnit_insert;
	PreparedStatement safetyChecklist_insert;
	PreparedStatement safetyChecklistItem_insert;
	
		// Submitted Tag Groups
	PreparedStatement group_insert;
	PreparedStatement groupXref_insert;
	PreparedStatement emailQueue_insert;
	
	// Timecard Statements
	PreparedStatement timecard_stored_proc;
	PreparedStatement timecard_exception;
	PreparedStatement timecard_payperiod;
	PreparedStatement timecard_header;
	PreparedStatement timecard_check_terminated;
	
	// Fetching Data Statements
		// Dispatch
	PreparedStatement dispatch_select;
		// Service Address
	PreparedStatement serviceAddress_select;
	PreparedStatement tenant_select;
	
		// Equipment
	PreparedStatement equipment_select;
	PreparedStatement fan_select;
	PreparedStatement belt_select;
	PreparedStatement sheave_select;
	PreparedStatement filter_select;
	PreparedStatement refcircuit_select;
	PreparedStatement compressor_select;
		// Contacts
	PreparedStatement contact_select;
	
		// Open Tags
	PreparedStatement openServiceTag_select;
	PreparedStatement openServiceTagUnit_select;
	PreparedStatement openServiceLabor_select;
	PreparedStatement openServiceMaterial_select;
	PreparedStatement openPMChecklist_select;
	PreparedStatement openBlue_select;
	PreparedStatement openBlueUnit_select;
	PreparedStatement openSafetyChecklist_select;
	PreparedStatement openSafetyChecklistItem_select;
	
		// Closed Tags
	PreparedStatement serviceTag_select;
	PreparedStatement serviceTagUnit_select;
	PreparedStatement serviceLabor_select;
	PreparedStatement serviceMaterial_select;
	
	// Static Table Statements
	PreparedStatement masterTableLog_select;
	
		// Misc Statements
	PreparedStatement fetch_dispatchIds;
	PreparedStatement sync_transaction_fail;
	PreparedStatement update_meid;
	
	public Prep_Statements(Connection con) throws SQLException
		{
		db = con;
		buildStatements();
		}
	
	private void buildStatements() throws SQLException
		{
		buildServiceAddressDispatchStatements();
		buildEquipmentStatements();
		buildServiceAddressContactStatements();
		buildOpenTagStatements();
		buildClosedTagStatements();
		buildGroupStatements();
		buildTimecardStatements();
		buildStaticTableStatements();
		buildMiscStatements();
		}
	
	private void buildServiceAddressDispatchStatements() throws SQLException
		{
		// Dispatch Selection Statement
		String sql = "SELECT dispatchId, serviceAddressId," +
				"batchNo, jobNo, cusNo, altBillTo, contractType, " +
				"dateStarted, dateEnded, dateOrdered, customerPO, requestedBy, " +
				"requestedByPhone, requestedByEmail, siteContact, siteContactPhone, " +
				"description, mechanic1, mechanic2, siteName, mechanic3, " +
				"mechanic4, mechanic5, mechanic6, mechanic7, status, tenant " +
			"FROM dispatch " +
			"WHERE ( dispatchId IN = ? " +
			"OR (dispatch.mechanic1 = ? " +
				"OR dispatch.mechanic2 = ? " +
				"OR dispatch.mechanic3 = ? " +
				"OR dispatch.mechanic4 = ? " +
				"OR dispatch.mechanic5 = ? " +
				"OR dispatch.mechanic6 = ? " +
				"OR dispatch.mechanic7 = ?) " +
			"OR ( ISNULL(SUBSTRING(mechanic1,5,6),'')='' " +
				"OR ISNULL(SUBSTRING(mechanic2,5,6),'')='' " +
				"OR ISNULL(SUBSTRING(mechanic3,5,6),'')='' " +
				"OR ISNULL(SUBSTRING(mechanic4,5,6),'')='' " +
				"OR ISNULL(SUBSTRING(mechanic5,5,6),'')='' " +
				"OR ISNULL(SUBSTRING(mechanic6,5,6),'')='' " +
				"OR ISNULL(SUBSTRING(mechanic7,5,6),'')='') ) " +
			"AND serviceAddressId <> '' " +
			"AND serviceAddressId IS NOT NULL";
		dispatch_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// Service Address Selection Statement
		sql = "SELECT serviceAddressId, siteName, " +
				"address1, address2, city, state, zip, " +
				"buildingNo, note " +
			"FROM serviceAddress " +
			"WHERE serviceAddressId IN ? " +
			"ORDER BY serviceAddressId";
		serviceAddress_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// Service Address Tenant Selection Statement
		sql = "SELECT tenantId, serviceAddressId, tenant " +
			"FROM serviceAddressTenant " +
			"WHERE serviceAddressId IN ? " +
			"ORDER BY serviceAddressId";
		tenant_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		}
	
	private void buildEquipmentStatements() throws SQLException
		{
		// Equipment Update Statement
		String sql = "UPDATE equipment SET " +
				"equipmentCategoryId = ?, " + 
				"serviceAddressId = ?, " + 
				"unitNo = ?, " + 
				"barCodeNo = ?, " + 
				"manufacturer = ?, " + 
				"model = ?, " + 
				"productIdentifier = ?, " + 
				"serialNo = ?, " + 
				"voltage = ?, " + 
				"economizer = ?, " + 
				"capacity = ?, " + 
				"capacityUnits = ?, " + 
				"refrigerantType = ?, " + 
				"areaServed = ?, " + 
				"mfgYear = ?, " + 
				"dateInService = ?, " + 
				"dateOutService = ?, " + 
				"notes = ?, " + 
				"verifiedByEmpno = ? " +
				
				"WHERE equipmentId = ?";
		equipment_update = db.prepareStatement(sql);
		
		// Equipment Insert Statement
		sql = "INSERT INTO equipment ( equipmentCategoryId," +
				"serviceAddressId, unitNo, barCodeNo, manufacturer, model, " +
				"productIdentifier, serialNo, voltage, economizer, capacity, " +
				"capacityUnits, refrigerantType, areaServed, mfgYear, " +
				"dateInService, dateOutService, notes, verifiedByEmpno " +
				") VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";
		equipment_insert = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		// Delete Belts
		sql = "DELETE FROM belt WHERE fanId IN ( SELECT fanId FROM fan WHERE equipmentId = ? )";
		belt_drop = db.prepareStatement(sql);
		
		// Delete Sheave
		sql = "DELETE FROM sheave WHERE fanId IN ( SELECT fanId FROM fan WHERE equipmentId = ? )";
		sheave_drop = db.prepareStatement(sql);
		
		// Delete Fan
		sql = "DELETE FROM fan WHERE equipmentId = ?";
		fan_drop = db.prepareStatement(sql);
		
		// Delete Filter
		sql = "DELETE FROM filter WHERE equipmentId = ?";
		filter_drop = db.prepareStatement(sql);
		
		// Delete Compressor
		sql = "DELETE FROM compressor WHERE circuitId IN ( SELECT circuitId FROM refCircuit WHERE equipmentId = ? )";
		compressor_drop = db.prepareStatement(sql);
		
		// Delete RefCircuit
		sql = "DELETE FROM refCircuit WHERE equipmentId = ?";
		refcircuit_drop = db.prepareStatement(sql);
		
		// Fan Insert
		sql = "INSERT INTO fan ( equipmentId, number, partType ) VALUES ( ?, ?, ? )";
		fan_insert = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		// Belt Insert
		sql = "INSERT INTO belt ( fanId, beltSize, quantity ) VALUES ( ?, ?, ? )";
		belt_insert = db.prepareStatement(sql);
		
		// Sheave Insert
		sql = "INSERT INTO sheave ( fanId, type, number, manufacturer ) VALUES ( ?, ?, ?, ? )";
		sheave_insert = db.prepareStatement(sql);
		
		// Filter Insert
		sql = "INSERT INTO filter ( equipmentId, type, quantity, filterSize ) VALUES ( ?, ?, ?, ? )";
		filter_insert = db.prepareStatement(sql);
		
		// RefCircuit Insert
		sql = "INSERT INTO refCircuit ( equipmentId, circuitNo, lbsRefrigerant ) VALUES ( ?, ?, ? )";
		refcircuit_insert = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		// Compressor Insert
		sql = "INSERT INTO compressor " +
				"( circuitId, compressorNo, manufacturer, model, serialNo, dateInService, dateOutService ) " +
				"VALUES ( ?, ?, ?, ?, ?, ?, ? )";
		compressor_insert = db.prepareStatement(sql);
		
		/**********************************************
		 * SELECTION Statements
		 *********************************************/
		
		// Equipment Selection Statement
		sql = "SELECT equipmentId, equipmentCategoryId," +
				"serviceAddressId, unitNo, barCodeNo, manufacturer, model, " +
				"productIdentifier, serialNo, voltage, economizer, capacity, " +
				"capacityUnits, refrigerantType, areaServed, mfgYear, " +
				"dateInService, dateOutService, notes, verifiedByEmpno " +
				"FROM equipment WHERE serviceAddressId IN = ? " +
				"ORDER BY equipmentId";
		equipment_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// Fan Selection Statement
		sql = "SELECT fanId, equipmentId, partType, number " +
				"FROM fan WHERE equipmentId IN = ? " +
				"ORDER BY equipmentId, fanId";
		fan_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// Belt Selection Statement
		sql = "SELECT fanId, beltSize, quantity " +
				"FROM belt WHERE fanId IN = ? " +
				"ORDER BY fanId";
		belt_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// Sheave Selection Statement
		sql = "SELECT fanId, type, number, manufacturer " +
				"FROM sheave WHERE fanId IN = ? " +
				"ORDER BY fanId";
		sheave_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// Filter Selection Statement
		sql = "SELECT equipmentId, type, quantity, filterSize " +
				"FROM filter WHERE equipmentId IN = ? " +
				"ORDER BY equipmentId";
		filter_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// RefCircuit Selection Statement
		sql = "SELECT circuitId, equipmentId, circuitNo, lbsRefrigerant " +
				"FROM refCircuit WHERE equipmentId IN = ? " +
				"ORDER BY equipmentId, circuitId";
		refcircuit_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// Compressor Selection Statement
		sql = "SELECT circuitId, compressorNo, manufacturer, model, serialNo, dateInService, dateOutService " +
				"FROM compressor WHERE circuitId IN = ? " +
				"ORDER BY circuitId";
		compressor_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		}
	
	private void buildServiceAddressContactStatements() throws SQLException
		{
		// Update Service Address Contact Statement
		String sql = "UPDATE serviceAddressContact SET " +
				"serviceAddressId = ?, " + 
				"contactName = ?, " + 
				"phone1 = ?, " + 
				"phone1Type = ?, " + 
				"phone2 = ?, " + 
				"phone2Type = ?, " + 
				"email = ?, " + 
				"contactType = ?, " + 
				"ext1 = ?, " + 
				"ext2 = ?, " + 
				"updatedBy = ?, " + 
				"updatedDate = ? " +
				
				"WHERE contactId = ?";
		contact_update = db.prepareStatement(sql);
		
		// Insert Service Address Contact Statement
		sql = "INSERT INTO serviceAddressContact ( serviceAddressId," +
				"contactName, phone1, phone1Type, phone2, phone2Type, " +
				"email, contactType, ext1, ext2, updatedBy, " +
				"updatedDate " +
				") VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";
		contact_insert = db.prepareStatement(sql);
		
		/**********************************************
		 * SELECTION Statements
		 *********************************************/
		
		// Select Service Address Contact Statement
		sql = "SELECT contactId, serviceAddressId," +
				"contactName, phone1, phone1Type, phone2, phone2Type, " +
				"email, contractType, ext1, ext2, updatedBy, " +
				"updatedDate " +
				"FROM serviceAddressContact WHERE serviceAddressId IN ?";
		contact_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		}
	
	private void buildOpenTagStatements() throws SQLException
		{
		// Select and Delete Open Tags
		String sql = "DELETE FROM openServiceTag WHERE tabletMEID = ?";
		openServiceTag_drop = db.prepareStatement(sql);
		
		// Select and delete Service Tag Units
		sql = "DELETE FROM openServiceTagUnit WHERE serviceTagId IN " +
				"( SELECT serviceTagId FROM openServiceTag WHERE tabletMEID = ? )";
		openServiceTagUnit_drop = db.prepareStatement(sql);
		
		// Drop Service Labor
		sql = "DELETE FROM openServiceLabor WHERE serviceTagUnitId IN " +
				"(SELECT serviceTagUnitId FROM openServiceTagUnit WHERE serviceTagId IN " +
					"(SELECT serviceTagId FROM openServiceTag WHERE tabletMEID = ?) )";
		openServiceLabor_drop = db.prepareStatement(sql);
		
		// Drop Service Material
		sql = "DELETE FROM openServiceMaterial WHERE serviceTagUnitId IN " +
				"(SELECT serviceTagUnitId FROM openServiceTagUnit WHERE serviceTagId IN " +
					"(SELECT serviceTagId FROM openServiceTag WHERE tabletMEID = ?) )";
		openServiceMaterial_drop = db.prepareStatement(sql);
		
		// Drop PM Checklist
		sql = "DELETE FROM openPMChecklist WHERE serviceTagUnitId IN " +
				"(SELECT serviceTagUnitId FROM openServiceTagUnit WHERE serviceTagId IN " +
					"(SELECT serviceTagId FROM openServiceTag WHERE tabletMEID = ?) )";
		openServicePMChecklist_drop = db.prepareStatement(sql);
		
		// Select and delete open Blue
		sql = "DELETE FROM openBlue WHERE serviceTagId IN " +
				"( SELECT serviceTagId FROM openServiceTag WHERE tabletMEID = ? )";
		openBlue_drop = db.prepareStatement(sql);
		
		// Drop Open Blue Units
		sql = "DELETE FROM openBlueUnit WHERE blueId IN " +
				"(SELECT blueId FROM openBlue WHERE serviceTagId IN " +
					"(SELECT serviceTagId FROM openServiceTag WHERE tabletMEID = ?) )";
		openBlueUnit_drop = db.prepareStatement(sql);
		
		// Drop SafetyChecklist
		sql = "DELETE FROM openSafetyTagChecklist WHERE serviceTagId IN " +
				"( SELECT serviceTagId FROM openServiceTag WHERE tabletMEID = ? )";
		openSafetyChecklist_drop = db.prepareStatement(sql);
		
		// Drop SafetyChecklistItem
		sql = "DELETE FROM openSafetyTagChecklistItem WHERE serviceTagId IN " +
				"( SELECT serviceTagId FROM openServiceTag WHERE tabletMEID = ? )";
		openSafetyChecklistItem_drop = db.prepareStatement(sql);
		
		// Next Tag Insert Statement
		sql = "INSERT INTO nextTag ( empno, dateAssigned ) VALUES ( ?, getDate() )";
		nextTag_insert = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		// OpenServiceTag Insert Statement
		sql = "INSERT INTO openServiceTag ( serviceTagId, serviceAddressId, dispatchId, " +
				"serviceType, serviceDate, billTo, billAddress1, billAddress2, billAddress3, billAddress4, " +
				"billAttn, siteName, tenant, address1, address2, city, state, zip, buildingNo, note, " +
				"batchNo, jobNo, empno, tabletMEID, disposition ) " +
				
				"VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?  )";
		openServiceTag_insert = db.prepareStatement(sql);
		
		// OpenServiceTagUnit Insert Statement
		sql = "INSERT INTO openServiceTagUnit ( serviceTagId, equipmentId, servicePerformed, comments ) " +
				"VALUES ( ?, ?, ?, ? )";
		openServiceTagUnit_insert = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		// OpenServiceLabor Insert Statement
		sql = "INSERT INTO openServiceLabor ( serviceTagUnitId, serviceDate, regHours, thHours, dtHours, mechanic, rate ) " +
				"VALUES ( ?, ?, ?, ?, ?, ?, ? )";
		openServiceLabor_insert = db.prepareStatement(sql);
		
		// OpenServiceMaterial Insert Statement
		sql = "INSERT INTO openServiceMaterial ( serviceTagUnitId, quantity, materialDesc, cost, refrigerantAdded, source ) " +
				"VALUES ( ?, ?, ?, ?, ?, ? )";
		openServiceMaterial_insert = db.prepareStatement(sql);
		
		// OpenPMChecklist Insert Statement
		sql = "INSERT INTO openPMChecklist ( serviceTagUnitId, itemText, itemType, itemValue, itemComment ) " +
				"VALUES ( ?, ?, ?, ?, ? )";
		openPMChecklist_insert = db.prepareStatement(sql);
		
		// OpenBlue Insert Statement
		sql = "INSERT INTO openBlue ( serviceTagId, dateCreated ) " +
				"VALUES ( ?, ? )";
		openBlue_insert = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		// OpenBlueUnit Insert Statement
		sql = "INSERT INTO openBlueUnit ( blueId, equipmentId, description, materials, laborHours, notes, cost, completed ) " +
				"VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )";
		openBlueUnit_insert = db.prepareStatement(sql);
		
		// OpenSafetyChecklist Insert Statement
		sql = "INSERT INTO openSafetyTagChecklist ( serviceTagId, checklistDate, comments ) " +
				"VALUES ( ?, ?, ? )";
		openSafetyChecklist_insert = db.prepareStatement(sql);
		
		// OpenSafetyChecklistItem Insert Statement
		sql = "INSERT INTO openSafetyTagChecklistItem ( serviceTagId, safetyChecklistId, itemRequired, itemValue ) " +
				"VALUES ( ?, ?, ?, ? )";
		openSafetyChecklistItem_insert = db.prepareStatement(sql);
		
		/**********************************************
		 * SELECTION Statements
		 *********************************************/
		
		// OpenServiceTag Selection Statement
		sql = "SELECT serviceTagId, serviceAddressId, dispatchId, " +
				"serviceType, serviceDate, billTo, billAddress1, billAddress2, billAddress3, billAddress4, " +
				"billAttn, siteName, tenant, address1, address2, city, state, zip, buildingNo, note, " +
				"batchNo, jobNo, empno, tabletMEID, disposition " +
			"FROM openServiceTag WHERE empno = ? " +
			"ORDER BY serviceTagId";
		openServiceTag_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// OpenServiceTagUnit Selection Statement
		sql = "SELECT serviceTagUnitId, serviceTagId, equipmentId, " +
				"servicePerformed, comments " +
			"FROM openServiceTagUnit WHERE serviceTagId IN ? " +
			"ORDER BY serviceTagId, serviceTagUnitId";
		openServiceTagUnit_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// OpenServiceLabor Selection Statement
		sql = "SELECT serviceLaborId, serviceTagUnitId, serviceDate, " +
				"regHours, thHours, dtHours, mechanic, rate " +
			"FROM openServiceLabor WHERE serviceTagUnitId IN ? " +
			"ORDER BY serviceTagUnitId, serviceLaborId";
		openServiceLabor_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// OpenServiceMaterial Selection Statement
		sql = "SELECT serviceMaterialId, serviceTagUnitId, quantity, " +
				"materialDesc, cost, refrigerantAdded, source " +
			"FROM openServiceMaterial WHERE serviceTagUnitId IN ? " +
			"ORDER BY serviceTagUnitId, serviceMaterialId";
		openServiceMaterial_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// OpenPMChecklist Selection Statement
		sql = "SELECT pmChecklistId, serviceTagUnitId, itemText, " +
				"itemType, itemValue, itemComment " +
			"FROM openPMChecklist WHERE serviceTagUnitId IN ? " +
			"ORDER BY serviceTagUnitId, pmChecklistId";
		openPMChecklist_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// OpenBlue Selection Statement
		sql = "SELECT blueId, serviceTagId, dateCreated " +
			"FROM openBlue WHERE serviceTagId IN ? " +
			"ORDER BY blueId";
		openBlue_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// OpenBlueUnit Selection Statement
		sql = "SELECT blueUnitId, blueId, equipmentId, description, " +
					"materials, laborHours, notes, completed, cost " +
				"FROM openBlueUnit WHERE blueId IN ? " +
				"ORDER BY blueId, blueUnitId";
		openBlueUnit_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// OpenSafetyTagChecklist Selection Statement
		sql = "SELECT serviceTagId, checklistDate, comments " +
				"FROM openSafetyTagChecklist WHERE serviceTag IN ? " +
				"ORDER BY serviceTagId";
		openSafetyChecklist_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// OpenSafetyTagChecklistItem Selection Statement
		sql = "SELECT serviceTagId, safetyChecklistId, itemRequired, itemValue " +
				"FROM openSafetyTagChecklistItem WHERE serviceTag IN ? " +
				"ORDER BY serviceTagId, safetyChecklistId";
		openSafetyChecklistItem_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		}
	
	private void buildClosedTagStatements() throws SQLException
		{
		// Closed ServiceTag Insert Statement
		String sql = "INSERT INTO serviceTag ( serviceTagId, serviceAddressId, dispatchId, " +
				"serviceType, serviceDate, billTo, billAddress1, billAddress2, billAddress3, billAddress4, " +
				"billAttn, siteName, tenant, address1, address2, city, state, zip, buildingNo, note, " +
				"batchNo, jobNo, empno, disposition, dateSubmitted ) " +
				
				"VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getDate()  )";
		serviceTag_insert = db.prepareStatement(sql);
		
		// ServiceTagUnit Insert Statement
		sql = "INSERT INTO serviceTagUnit ( serviceTagId, equipmentId, servicePerformed, comments ) " +
				"VALUES ( ?, ?, ?, ? )";
		serviceTagUnit_insert = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		// serviceLabor Insert Statement
		sql = "INSERT INTO serviceLabor ( serviceTagUnitId, serviceDate, regHours, thHours, dtHours, mechanic, rate ) " +
				"VALUES ( ?, ?, ?, ?, ?, ?, ? )";
		serviceLabor_insert = db.prepareStatement(sql);
		
		// serviceMaterial Insert Statement
		sql = "INSERT INTO serviceMaterial ( serviceTagUnitId, quantity, materialDesc, cost, refrigerantAdded, source ) " +
				"VALUES ( ?, ?, ?, ?, ?, ? )";
		serviceMaterial_insert = db.prepareStatement(sql);
		
		// servicePhoto Insert Statement
		sql = "INSERT INTO servicePhoto ( serviceTagUnitId, photoDate, photo, comments ) " +
				"VALUES ( ?, ?, ?, ? )";
		servicePhoto_insert = db.prepareStatement(sql);
		
		// PMChecklist Insert Statement
		sql = "INSERT INTO PMChecklist ( serviceTagUnitId, itemText, itemType, itemValue, itemComment ) " +
				"VALUES ( ?, ?, ?, ?, ? )";
		PMChecklist_insert = db.prepareStatement(sql);
		
		// serviceReceipt Insert Statement
		sql = "INSERT INTO serviceReceipt ( serviceTagId, photoDate, photo, comments ) " +
				"VALUES ( ?, ?, ?, ? )";
		serviceReceipt_insert = db.prepareStatement(sql);
		
		// Blue Insert Statement
		sql = "INSERT INTO Blue ( serviceTagId, dateCreated, processed ) " +
				"VALUES ( ?, ?, 'N' )";
		blue_insert = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		// BlueUnit Insert Statement
		sql = "INSERT INTO BlueUnit ( blueId, equipmentId, description, materials, laborHours, notes, cost ) " +
				"VALUES ( ?, ?, ?, ?, ?, ?, ? )";
		blueUnit_insert = db.prepareStatement(sql);
		
		// SafetyChecklist Insert Statement
		sql = "INSERT INTO safetyTagChecklist ( serviceTagId, checklistDate, comments ) " +
				"VALUES ( ?, ?, ? )";
		safetyChecklist_insert = db.prepareStatement(sql);
		
		// SafetyChecklistItem Insert Statement
		sql = "INSERT INTO safetyTagChecklistItem ( serviceTagId, safetyChecklistId, itemRequired, itemValue ) " +
				"VALUES ( ?, ?, ?, ? )";
		safetyChecklistItem_insert = db.prepareStatement(sql);
		
		/**********************************************
		 * SELECTION Statements
		 *********************************************/
		
		// ServiceTag Selection Statement
		sql = "SELECT serviceTagId, serviceAddressId, dispatchId, " +
				"serviceType, serviceDate, billTo, billAddress1, billAddress2, billAddress3, billAddress4, " +
				"billAttn, siteName, address1, address2, city, state, zip, buildingNo, note, " +
				"batchNo, jobNo, empno, disposition " +
			"FROM serviceTag WHERE empno = ? " +
			"ORDER BY serviceTagId";
		serviceTag_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// ServiceTagUnit Selection Statement
		sql = "SELECT serviceTagUnitId, serviceTagId, equipmentId, " +
				"servicePerformed, comments " +
			"FROM serviceTagUnit WHERE serviceTagId IN ? " +
			"ORDER BY serviceTagId, serviceTagUnitId";
		serviceTagUnit_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// ServiceLabor Selection Statement
		sql = "SELECT serviceLaborId, serviceTagUnitId, serviceDate, " +
				"regHours, thHours, dtHours, mechanic, rate " +
			"FROM serviceLabor WHERE serviceTagUnitId IN ? " +
			"ORDER BY serviceTagUnitId, serviceLaborId";
		serviceLabor_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		// ServiceMaterial Selection Statement
		sql = "SELECT serviceMaterialId, serviceTagUnitId, quantity, " +
				"materialDesc, cost, refrigerantAdded, source " +
			"FROM serviceMaterial WHERE serviceTagUnitId IN ? " +
			"ORDER BY serviceTagUnitId, serviceMaterialId";
		serviceMaterial_select = db.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		}
	
	private void buildGroupStatements() throws SQLException
		{
		String sql = "INSERT INTO serviceTagGroup ( signature, noSignatureReason, dateCreated ) " +
				"VALUES ( ?, ?, getDate() )";
		group_insert = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
		sql = "INSERT INTO serviceTagGroupXref ( groupId, serviceTagId ) " +
				"VALUES ( ?, ? )";
		groupXref_insert = db.prepareStatement(sql);
		
		sql = "INSERT INTO serviceTagEmail ( groupId, emailList, status ) " +
				"VALUES ( ?, ?, ? )";
		emailQueue_insert = db.prepareStatement(sql);
		}
	
	private void buildTimecardStatements() throws SQLException
		{
		// Timecard Stored Procedure
		String sql = "exec usp_timecard_detail_add " +
				"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
				"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
				"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
				"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
				"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
				"?, ?, ?, ?, ?";
		timecard_stored_proc = db.prepareStatement(sql);
		
		// Timecard Exception
		sql = "INSERT INTO timecard_exception ( empno, pay_per, serviceTagId, job_no, job_name, serviceDate, " +
				"dateSubmitted, regHours, thHours, dtHours, exception ) " +
				"VALUES ( ?, ?, ?, ?, ?, ?, getDate(), ?, ?, ?, ? )";
		timecard_exception = db.prepareStatement(sql);
		
		// Timecard pay period Fetch
		sql = "SELECT pay_per FROM pay_pers " +
				"WHERE date_start <= ? " +
					"AND date_end >= ?";
		timecard_payperiod = db.prepareStatement(sql);
		
		// Timecard Header Fetch
		sql = "SELECT completed_by = ISNULL(completed_by,''), approved_by = ISNULL(approved_by,'') " +
				"FROM timecard_header " +
					"WHERE (empno = ?) " +
						"AND ( (date_start <= ?) AND (date_end >= ?) )";
		timecard_header = db.prepareStatement(sql);
		
		// Timecard terminated employee validation
		sql = "SELECT terminated, dept " +
				"FROM users " +
					"WHERE empno = ?";
		timecard_check_terminated = db.prepareStatement(sql);
		}
	
	private void buildStaticTableStatements() throws SQLException
		{
		String sql = "SELECT tableName, updateDate FROM masterTableLog";
		masterTableLog_select = db.prepareStatement(sql);
		}
	
	private void buildMiscStatements() throws SQLException
		{
		String sql = "INSERT INTO syncException (detail, sql, queryError, dateStamp) VALUES ( ?, ?, ?, getDate() )";
		sync_transaction_fail = db.prepareStatement(sql);
		
		sql = "UPDATE openServiceTag SET tabletMEID = ? " +
				"WHERE empno = ?";
		update_meid = db.prepareStatement(sql);
		}
	}

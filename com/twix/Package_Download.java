package com.twix;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Package_Download implements Serializable
	{
	private static final long serialVersionUID = -7649152199596167712L;
	/**
	 * Volatile Tables *************************************************************
	 */

	public List<Dispatch> dispatch;
	public List<ServiceAddress> serviceAddress;
	
	// Service Address Details
	public List<ServiceAddressContact> contacts;
	public List<ServiceAddressNotes> notes;
	public List<ServiceAddress.pmAddressChecklist> pmAddressChecklist;
	public List<ServiceAddress.tenant> tenants;
	
	// Open Service Tags
	public List<OpenServiceTag> openServiceTag;
	public List<ServiceTagUnit> openServiceTagUnit;
	public List<ServiceLabor> openServiceLabor;
	public List<ServiceMaterial> openServiceMaterial;
	public List<ServiceRefrigerant> openServiceRefrigerant;
	public List<PMChecklist> openPMChecklist;
	public List<OpenBlue> openBlue;
	public List<OpenBlueUnit> openBlueUnit;
	public List<OpenSafetyTagChecklist> SafetyTagChecklist;
	public List<ChecklistItem> SafetyTagChecklistItem;
	
	// Closed Service Tags
	public List<ServiceTag> serviceTag;
	public List<ServiceTagUnit> serviceTagUnit;
	public List<ServiceLabor> serviceLabor;
	public List<ServiceMaterial> serviceMaterial;
	public List<ServiceRefrigerant> serviceRefrigerant;
	
	// Closed Blues - Viewable in future release
	public List<OpenBlue> closedBlue;
	public List<OpenBlueUnit> closedBlueUnit;
	
	// Equipment
	public List<Equipment> equipment;
	public List<Fan> fan;
	public List<Sheave> sheave;
	public List<Filter> filter;
	public List<RefCircuit> refcircuit;
	public List<Compressor> compressor;
	
	// Maps to resolve photo and receipt Ids
	public List<Pair> serviceTagId_map;
	public List<Pair> serviceTagUnitId_map;
	
	// Static Tables. Null if the table isn't necessary on sync
	public List<EquipmentCategory> equipmentCategory;
	public List<serviceLaborRate> servicelaborrate;
	//public List<JobDoc> jobdoc;
	public List<FilterType> filterType;
	public List<FilterSize> filterSize;
	public List<RefrigerantType> refrigerantType;
	public List<Mechanic> mechanic;
	public List<SafetyChecklist> safetyChecklist;
	public List<ServiceDescription> serviceDescription;
	public List<pmStdChecklist> pmStdChecklist;
	
	// Billing Table
	public List<Billing> billing;
	
	// Dispatch Priority Color Coding Table
	public List<DispatchPriority> dispatchPriority;
	
	// PickList Tables
	public List<Object> PickList;
	public List<Object> PickListItem;
	
	// Resolved Id Maps
	public Map<Integer,Integer> ResolvedServiceTagIds;
	public Map<Integer,Integer> ResolvedServiceTagUnitIds;
	
	public PackageData data;
	
	
	/**
	 * Static Tables *************************************************************
	 */
	static public class EquipmentCategory implements Serializable
		{
		private static final long serialVersionUID = -6024807841994395833L;
		
		public int equipmentCategoryId;
		public String categoryDesc;
		}
	
	static public class serviceLaborRate implements Serializable
	{
	private static final long serialVersionUID = -6024807841994395833L;
	
	public int rateId;
	public String rate;
	public String rateDesc;
	}
	
	/*static public class JobDoc implements Serializable
	{
	private static final long serialVersionUID =  -6024807841994395833L;
	
	public String jobno;
	public byte[] documentContents;
	public String documentName;
	public String documentTitle;
	public String dataSubmitted;
	public String jobsite;
	}*/
	
	static public class FilterType implements Serializable
		{
		private static final long serialVersionUID = 1369913109101759037L;
		
		public int filterTypeId;
		public String filterType;
		}
	
	static public class FilterSize implements Serializable
		{
		
		private static final long serialVersionUID = -4492601654557384006L;
		
		public int filterSizeId;
		public String filterSize;
		}
	
	static public class RefrigerantType implements Serializable
		{
		private static final long serialVersionUID = -1954150837020753891L;
		
		public int refrigerantTypeId;
		public String refrigerantType;
		}
	
	static public class SafetyChecklist implements Serializable
		{
		private static final long serialVersionUID = -2714936808417779752L;
		
		public int safetyChecklistId;
		public int sortOrder;
		public String LOTO;
		public String itemType;
		public String itemText;
		public String itemTextBold;
		}
	
	static public class ServiceDescription implements Serializable
		{
		private static final long serialVersionUID = 783230990098441437L;
		
		public int descriptionID;
		public String description;
		}
	
	static public class pmStdChecklist implements Serializable
		{
		private static final long serialVersionUID = -8482722450052989038L;
		
		public int pmChecklistId;
		public int equipmentCategoryId;
		public String itemText;
		public String itemType;
		public String identifier;
		}
	
	static public class Mechanic implements Serializable
		{
		private static final long serialVersionUID = 5595777423624947814L;
		
		public String empno;
		public String name;
		public String terminated;
		public String dept;
		}
	
	/**
	 * Constructor. Initializes each array list.
	 * 
	 * 	Note: Static tables are null if there are no changes, otherwise they are initialized
	 * 
	 */
	
	public Package_Download()
		{
		dispatch = new ArrayList<Dispatch>();
		serviceAddress = new ArrayList<ServiceAddress>();

		contacts = new ArrayList<ServiceAddressContact>();
		notes = new ArrayList<ServiceAddressNotes>();
		pmAddressChecklist = new ArrayList<ServiceAddress.pmAddressChecklist>();
		tenants = new ArrayList<ServiceAddress.tenant>();

		openServiceTag = new ArrayList<OpenServiceTag>();
		openServiceTagUnit = new ArrayList<ServiceTagUnit>();
		openServiceLabor = new ArrayList<ServiceLabor>();
		openServiceMaterial = new ArrayList<ServiceMaterial>();
		openServiceRefrigerant = new ArrayList<ServiceRefrigerant>();
		openPMChecklist = new ArrayList<PMChecklist>();
		openBlue = new ArrayList<OpenBlue>();
		openBlueUnit = new ArrayList<OpenBlueUnit>();
		SafetyTagChecklist = new ArrayList<OpenSafetyTagChecklist>();
		SafetyTagChecklistItem = new ArrayList<ChecklistItem>();

		serviceTag = new ArrayList<ServiceTag>();
		serviceTagUnit = new ArrayList<ServiceTagUnit>();
		serviceLabor = new ArrayList<ServiceLabor>();
		serviceMaterial = new ArrayList<ServiceMaterial>();
		serviceRefrigerant = new ArrayList<ServiceRefrigerant>();
		equipment = new ArrayList<Equipment>();
		fan = new ArrayList<Fan>();
		sheave = new ArrayList<Sheave>();
		filter = new ArrayList<Filter>();
		refcircuit = new ArrayList<RefCircuit>();
		compressor = new ArrayList<Compressor>();
		closedBlue = new ArrayList<OpenBlue>();
		closedBlueUnit = new ArrayList<OpenBlueUnit>();
		
		dispatch		= new ArrayList<Dispatch>();
		serviceAddress	= new ArrayList<ServiceAddress>();
		openServiceTag	= new ArrayList<OpenServiceTag>();
		serviceTag		= new ArrayList<ServiceTag>();
		equipment		= new ArrayList<Equipment>();
		
		billing	= new ArrayList<Billing>();
		
		PickList = new ArrayList<Object>();
		PickListItem = new ArrayList<Object>();
		}
	
	public static class PickList implements Serializable
		{
		private static final long serialVersionUID = -4975376188910566956L;
		
		public long PickId;
		public String Description;
		public String DateChanged;
		}
	
	public static class PickListItem implements Serializable
		{
		private static final long serialVersionUID = 8996406860855233955L;
		
		public long PickItemId;
		public long PickId;
		public String itemValue;
		}
	}






package com.twix;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Package_Upload implements Serializable
	{
	private static final long serialVersionUID = 2399804193185639897L;
	
	public List<OpenServiceTag> openTags;
	public List<Equipment> equipment;
	public List<ServiceAddressContact> contact;
	public List<ServiceAddressNotes> notes;
	public List<ServiceTagGroup> groups;
	
	public List<Object> Dispatch;
	public PackageData data;
	
	public Package_Upload()
		{
		openTags = new ArrayList<OpenServiceTag>();
		equipment = new ArrayList<Equipment>();
		contact = new ArrayList<ServiceAddressContact>();
		notes = new ArrayList<ServiceAddressNotes>();
		groups = new ArrayList<ServiceTagGroup>();
		}
	
	public static class Dispatch implements Serializable
		{
		private static final long	serialVersionUID	= -8296880771856196905L;
		public int DispatchId;
		public String CallComplete;
		}
	}

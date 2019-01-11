package com.twix;
import java.io.Serializable;
import java.util.List;

public class OpenServiceTag implements Serializable
	{
	private static final long serialVersionUID = -8010605442108985882L;
	
	public int serviceTagId;
	public int serviceAddressId;
	public int dispatchId;
	public String serviceType;
	public String serviceDate;
	public String billTo;
	public String billAddress1;
	public String billAddress2;
	public String billAddress3;
	public String billAddress4;
	public String billAttn;
	
	public String siteName;
	public String tenant;
	public String address1;
	public String address2;
	public String city;
	public String state;
	public String zip;
	public String buildingNo;
	public String note;
	
	public String batchNo;
	public String jobNo;
	public String customerPO;
	public String requestedBy;
	public String requestedByPhone;
	public String requestedByEmail;
	public String description;
	public String empno;
	
	public String tabletMEID;
	public String disposition;
	public String xoi_flag;
	
	public List<ServiceTagUnit> units;
	public OpenBlue blue;
	public OpenSafetyTagChecklist safetyChecklist;
	public List<Receipt> receipts;
	
	public boolean submit = false;
	
	public OpenServiceTag()
		{
		}
	}
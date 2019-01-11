package com.twix;

import java.io.Serializable;

public class ServiceTag implements Serializable
	{
	private static final long serialVersionUID = 275342361214303731L;
	
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
	public String dispatchDescription; // TODO: Use this column as a dispatch copy column
	public String customerPO;
	public String requestedBy;
	public String requestedByPhone;
	public String requestedByEmail;
	public String description;
	public String empno;
	
	public String tabletMEID;
	public String disposition;
	public String xoi_flag;
	
	public ServiceTag()
		{
		}
	}
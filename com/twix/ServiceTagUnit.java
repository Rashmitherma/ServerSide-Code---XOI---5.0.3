package com.twix;
import java.io.Serializable;
import java.util.ArrayList;

public class ServiceTagUnit implements Serializable
		{
		private static final long serialVersionUID = 1514117131253324570L;
		
		public int serviceTagId;
		public int serviceTagUnitId;
		public int equipmentId;
		public String servicePerformed;
		public String comments;
		
		
		// UPLOAD ONLY DATA
		public ArrayList<ServiceLabor> labor;
		public ArrayList<ServiceMaterial> material;
		public ArrayList<ServiceRefrigerant> refrigerant;
		public ArrayList<PMChecklist> pmChecklist;
		public ArrayList<Photo> photos;
		
		public ServiceTagUnit()
			{
			}
		
		}